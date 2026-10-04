"""Large spreadsheets and CSV files read straight from disk (no upload, no copy into My Files).

Runs inside pa-parser (no network, no keys, Job Object limits). The file is streamed ONCE into a local SQLite cache
next to the app's private temp folder; every later question (inspect / rows / query) is answered from that cache in
well under a second, even for a 100 MB workbook. The model never writes SQL: it sends a declarative query that is
validated here (known column names, whitelisted operators, bound parameters).

Formats: .xlsx / .xlsm (streamed with the standard library, no Excel needed), .csv / .tsv / .txt tables.
Not supported: legacy .xls (save as .xlsx or .csv first), password-protected workbooks.
"""
from __future__ import annotations

import csv
import datetime as dt
import json
import os
import re
import sqlite3
import sys
import time
import zipfile
from pathlib import Path
from typing import Any, Iterator
from xml.etree import ElementTree as ET


def _no_dtd(head: bytes) -> None:
    """Refuse XML with a DTD / entity declarations (billion laughs, XXE) before it reaches the parser."""
    low = head[:65536].lower()
    if b"<!doctype" in low or b"<!entity" in low:
        raise ValueError("XML with DTD/entities is not accepted")


def _open_xml(z: "zipfile.ZipFile", name: str):
    with z.open(name) as probe:
        _no_dtd(probe.read(65536))
    return z.open(name)


def _read_xml(z: "zipfile.ZipFile", name: str) -> bytes:
    data = z.read(name)
    _no_dtd(data)
    return data

MAX_ROWS_TOTAL = 6_000_000
MAX_COLS = 400
BATCH = 5000
OUT_CHARS = 60_000
NS = "{http://schemas.openxmlformats.org/spreadsheetml/2006/main}"
REL = "{http://schemas.openxmlformats.org/officeDocument/2006/relationships}"
PKG_REL = "{http://schemas.openxmlformats.org/package/2006/relationships}"
EPOCH_1900 = dt.datetime(1899, 12, 30)
EPOCH_1904 = dt.datetime(1904, 1, 1)
TABLE_EXT = {".xlsx", ".xlsm", ".csv", ".tsv"}
TEXT_EXT = {".txt", ".md", ".json", ".log", ".xml", ".html", ".htm", ".yaml", ".yml", ".ini", ".docx", ".pdf"}


class TableError(Exception):
    pass


# --------------------------------------------------------------------------------------------- xlsx streaming
def _col_index(ref: str) -> int:
    n = 0
    for ch in ref:
        if ch.isalpha():
            n = n * 26 + (ord(ch.upper()) - 64)
        else:
            break
    return n - 1


_DATE_CHARS = re.compile(r"[ymdhs]", re.I)
_BUILTIN_DATE = {14, 15, 16, 17, 18, 19, 20, 21, 22, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 45, 46, 47, 50, 51, 52, 53, 54, 55, 56, 57, 58}


def _is_date_format(code: str) -> bool:
    c = re.sub(r'"[^"]*"|\[[^\]]*\]|\\.', "", code)
    return bool(_DATE_CHARS.search(c)) and "General" not in code


def _date_styles(z: zipfile.ZipFile) -> set[int]:
    if "xl/styles.xml" not in z.namelist():
        return set()
    custom: dict[int, str] = {}
    out: set[int] = set()
    xf_index = 0
    in_cellxfs = False
    with _open_xml(z, "xl/styles.xml") as fh:
        for ev, el in ET.iterparse(fh, events=("start", "end")):
            if ev == "start" and el.tag == NS + "cellXfs":
                in_cellxfs = True
            elif ev == "end" and el.tag == NS + "numFmt":
                custom[int(el.get("numFmtId", "0"))] = el.get("formatCode", "")
            elif ev == "end" and el.tag == NS + "xf" and in_cellxfs:
                nid = int(el.get("numFmtId", "0"))
                if nid in _BUILTIN_DATE or (nid in custom and _is_date_format(custom[nid])):
                    out.add(xf_index)
                xf_index += 1
            elif ev == "end" and el.tag == NS + "cellXfs":
                in_cellxfs = False
    return out


def _shared_strings(z: zipfile.ZipFile) -> list[str]:
    if "xl/sharedStrings.xml" not in z.namelist():
        return []
    out: list[str] = []
    with _open_xml(z, "xl/sharedStrings.xml") as fh:
        for _ev, el in ET.iterparse(fh, events=("end",)):
            if el.tag == NS + "si":
                out.append("".join(t.text or "" for t in el.iter(NS + "t")))
                el.clear()
    return out


def _sheets(z: zipfile.ZipFile) -> list[tuple[str, str]]:
    wb = ET.fromstring(_read_xml(z, "xl/workbook.xml"))
    rels = {r.get("Id"): r.get("Target") for r in ET.fromstring(_read_xml(z, "xl/_rels/workbook.xml.rels")).iter(PKG_REL + "Relationship")}
    out = []
    for s in wb.iter(NS + "sheet"):
        target = rels.get(s.get(REL + "id"), "")
        target = target.lstrip("/")
        path = target if target.startswith("xl/") else "xl/" + target
        out.append((s.get("name") or f"Sheet{len(out) + 1}", path))
    return out


def _date1904(z: zipfile.ZipFile) -> bool:
    root = ET.fromstring(_read_xml(z, "xl/workbook.xml"))
    pr = root.find(NS + "workbookPr")
    return pr is not None and pr.get("date1904") in ("1", "true")


def _xlsx_rows(z: zipfile.ZipFile, sheet_path: str, strings: list[str], dates: set[int], epoch: dt.datetime) -> Iterator[list[Any]]:
    row: list[Any] = []
    cell_ref = cell_t = cell_s = None
    val: str | None = None
    inline: list[str] = []
    with _open_xml(z, sheet_path) as fh:
        for ev, el in ET.iterparse(fh, events=("start", "end")):
            tag = el.tag
            if ev == "start":
                if tag == NS + "row":
                    row = []
                elif tag == NS + "c":
                    cell_ref, cell_t, cell_s, val, inline = el.get("r", ""), el.get("t"), el.get("s"), None, []
                continue
            if tag == NS + "v":
                val = el.text
            elif tag == NS + "t" and cell_t == "inlineStr":
                inline.append(el.text or "")
            elif tag == NS + "c":
                idx = _col_index(cell_ref) if cell_ref else len(row)
                if idx >= MAX_COLS:
                    el.clear()
                    continue
                while len(row) < idx:
                    row.append(None)
                v: Any = None
                if cell_t == "s" and val is not None:
                    try:
                        v = strings[int(val)]
                    except (ValueError, IndexError):
                        v = None
                elif cell_t == "inlineStr":
                    v = "".join(inline)
                elif cell_t in ("str", "e"):
                    v = val
                elif cell_t == "b":
                    v = "TRUE" if val == "1" else "FALSE"
                elif cell_t == "d":
                    v = val
                elif val not in (None, ""):
                    try:
                        f = float(val)
                        if cell_s is not None and cell_s.isdigit() and int(cell_s) in dates and -1 < f < 2958466:
                            d = epoch + dt.timedelta(days=f)
                            v = d.strftime("%Y-%m-%d") if f == int(f) else d.strftime("%Y-%m-%d %H:%M:%S")
                        else:
                            v = int(f) if f == int(f) and abs(f) < 1e15 and "." not in val and "E" not in val.upper() else f
                    except ValueError:
                        v = val
                if len(row) == idx:
                    row.append(v)
                else:
                    row[idx] = v
                el.clear()
            elif tag == NS + "row":
                yield row
                el.clear()


# --------------------------------------------------------------------------------------------- csv streaming
def _csv_rows(path: Path) -> Iterator[list[Any]]:
    raw = path.open("rb").read(65536)
    enc = "utf-8-sig"
    try:
        raw.decode("utf-8")
    except UnicodeDecodeError:
        enc = "cp1252"
    sample = raw.decode(enc, errors="replace")
    try:
        dialect = csv.Sniffer().sniff(sample[:20000], delimiters=",;\t|")
    except csv.Error:
        dialect = csv.excel_tab if path.suffix.lower() == ".tsv" else csv.excel
    csv.field_size_limit(10_000_000)
    with path.open("r", encoding=enc, errors="replace", newline="") as f:
        for r in csv.reader(f, dialect):
            yield [_num(x) for x in r[:MAX_COLS]]


_NUM = re.compile(r"^-?\d+(\.\d+)?$")


def _num(x: str) -> Any:
    s = x.strip()
    if not s:
        return None
    if _NUM.match(s) and len(s) < 16 and not (len(s) > 1 and s[0] == "0" and s[1].isdigit()):
        return float(s) if "." in s else int(s)
    return x


# --------------------------------------------------------------------------------------------- cache build
def _names(first: list[Any], ncols: int) -> list[str]:
    seen: dict[str, int] = {}
    out = []
    for i in range(ncols):
        raw = first[i] if i < len(first) and first[i] not in (None, "") else f"col_{i + 1}"
        name = re.sub(r"\s+", " ", str(raw)).strip()[:80] or f"col_{i + 1}"
        base = name
        n = seen.get(base.lower(), 0)
        seen[base.lower()] = n + 1
        out.append(base if n == 0 else f"{base}_{n + 1}")
    return out


def _looks_like_header(row: list[Any]) -> bool:
    cells = [c for c in row if c not in (None, "")]
    return bool(cells) and sum(isinstance(c, str) for c in cells) / len(cells) >= 0.6


def _build(src: Path, cache: Path, progress: Any = None) -> None:
    tmp = cache.with_suffix(".building")
    if tmp.exists():
        tmp.unlink()
    db = sqlite3.connect(tmp)
    db.execute("PRAGMA journal_mode=OFF")
    db.execute("PRAGMA synchronous=OFF")
    db.execute("CREATE TABLE _meta(k TEXT PRIMARY KEY, v TEXT)")
    db.execute("CREATE TABLE _sheets(idx INTEGER PRIMARY KEY, name TEXT, tbl TEXT, nrows INTEGER, cols TEXT, header INTEGER)")
    ext = src.suffix.lower()
    if ext in (".xlsx", ".xlsm"):
        with zipfile.ZipFile(src) as z:
            if "xl/workbook.xml" not in z.namelist():
                raise TableError("this is not a valid .xlsx workbook (is it password protected or an old .xls?)")
            strings, dates = _shared_strings(z), _date_styles(z)
            epoch = EPOCH_1904 if _date1904(z) else EPOCH_1900
            sources = [(name, _xlsx_rows(z, path, strings, dates, epoch)) for name, path in _sheets(z)]
            total = _load(db, sources, progress)
    else:
        total = _load(db, [(src.stem, _csv_rows(src))], progress)
    st = src.stat()
    db.execute("INSERT INTO _meta VALUES('size',?)", (str(st.st_size),))
    db.execute("INSERT INTO _meta VALUES('mtime',?)", (str(int(st.st_mtime)),))
    db.execute("INSERT INTO _meta VALUES('rows',?)", (str(total),))
    db.commit()
    db.close()
    os.replace(tmp, cache)


def _clean(r: list[Any], ncols: int) -> list[Any] | None:
    """Pad / cut a row to the table width; empty rows are dropped."""
    if not any(c not in (None, "") for c in r):
        return None
    return (r + [None] * ncols)[:ncols]


def _load(db: sqlite3.Connection, sources: list[tuple[str, Iterator[list[Any]]]], progress: Any) -> int:
    total = 0
    for idx, (name, rows) in enumerate(sources):
        it = iter(rows)
        buf: list[list[Any]] = []
        for r in it:                       # skip leading empty rows
            if any(c not in (None, "") for c in r):
                buf.append(r)
                break
        if not buf:
            db.execute("INSERT INTO _sheets VALUES(?,?,?,?,?,?)", (idx, name, f"t{idx}", 0, "[]", 0))
            continue
        head = buf[0]
        has_header = _looks_like_header(head)
        # look ahead a few rows to size the table (rows can be ragged)
        pending = list(buf)
        for _ in range(200):
            try:
                pending.append(next(it))
            except StopIteration:
                break
        ncols = min(MAX_COLS, max(len(r) for r in pending))
        cols = _names(head if has_header else [], ncols)
        tbl = f"t{idx}"
        db.execute(f"CREATE TABLE {tbl}(" + ",".join(f'"c{i}"' for i in range(ncols)) + ")")
        ins = f"INSERT INTO {tbl} VALUES(" + ",".join("?" * ncols) + ")"
        n = 0
        batch: list[list[Any]] = []
        for r in (pending[1:] if has_header else pending):
            row = _clean(r, ncols)
            if row is not None:
                batch.append(row)
                n += 1
        for r in it:
            row = _clean(r, ncols)
            if row is None:
                continue
            batch.append(row)
            n += 1
            if len(batch) >= BATCH:
                db.executemany(ins, batch)
                batch.clear()
                if progress:
                    progress(total + n)
            if total + n > MAX_ROWS_TOTAL:
                raise TableError(f"too many rows (limit {MAX_ROWS_TOTAL:,}); filter the file first")
        if batch:
            db.executemany(ins, batch)
        db.execute("INSERT INTO _sheets VALUES(?,?,?,?,?,?)", (idx, name, tbl, n, json.dumps(cols), int(has_header)))
        total += n
    db.commit()
    return total


def ensure_cache(src: Path, cache: Path) -> sqlite3.Connection:
    """Open the SQLite cache for `src`, (re)building it when missing or when the file changed on disk."""
    st = src.stat()
    if cache.exists():
        try:
            db = sqlite3.connect(f"file:{cache}?mode=ro", uri=True)
            meta = dict(db.execute("SELECT k, v FROM _meta").fetchall())
            if meta.get("size") == str(st.st_size) and meta.get("mtime") == str(int(st.st_mtime)):
                return db
            db.close()
        except sqlite3.Error:
            pass
    cache.parent.mkdir(parents=True, exist_ok=True)
    _build(src, cache)
    return sqlite3.connect(f"file:{cache}?mode=ro", uri=True)


# --------------------------------------------------------------------------------------------- answers
def _sheet_row(db: sqlite3.Connection, sheet: str | None) -> tuple[int, str, str, int, list[str]]:
    rows = db.execute("SELECT idx, name, tbl, nrows, cols FROM _sheets ORDER BY idx").fetchall()
    if not rows:
        raise TableError("the file has no sheets")
    if sheet:
        for r in rows:
            if r[1].lower() == sheet.lower():
                return r[0], r[1], r[2], r[3], json.loads(r[4])
        raise TableError(f"no sheet named '{sheet}'. Sheets: {', '.join(r[1] for r in rows)}")
    r = next((x for x in rows if x[3] > 0), rows[0])
    return r[0], r[1], r[2], r[3], json.loads(r[4])


def _cell(v: Any) -> str:
    if v is None:
        return ""
    if isinstance(v, float):
        return f"{v:.10g}"
    return str(v).replace("\r", " ").replace("\n", " ")[:300]


def _table(cols: list[str], rows: list[tuple], limit_chars: int = OUT_CHARS) -> str:
    out = ["| " + " | ".join(cols) + " |", "|" + "---|" * len(cols)]
    size = len(out[0]) * 2
    for i, r in enumerate(rows):
        line = "| " + " | ".join(_cell(x).replace("|", "/") for x in r) + " |"
        size += len(line)
        if size > limit_chars:
            out.append(f"... (output limited: showing {i} of {len(rows)} rows - ask for fewer columns or aggregate)")
            break
        out.append(line)
    return "\n".join(out)


def op_inspect(src: Path, db: sqlite3.Connection, p: dict[str, Any]) -> dict[str, Any]:
    sample_n = int(p.get("sample_rows", 5))
    parts = [f"File: {src.name} ({src.stat().st_size / 1e6:.1f} MB)"]
    for idx, name, tbl, nrows, cols_json in db.execute("SELECT idx, name, tbl, nrows, cols FROM _sheets ORDER BY idx").fetchall():
        if p.get("sheet") and name.lower() != str(p["sheet"]).lower():
            continue
        cols = json.loads(cols_json)
        parts.append(f"\n## Sheet '{name}': {nrows:,} data rows x {len(cols)} columns")
        if not nrows:
            continue
        info = []
        for i, c in enumerate(cols[:60]):
            n, nn, mn, mx = db.execute(f'SELECT count("c{i}"), count(DISTINCT "c{i}"), min("c{i}"), max("c{i}") FROM {tbl}' if nrows <= 300_000
                                       else f'SELECT count("c{i}"), -1, min("c{i}"), max("c{i}") FROM {tbl}').fetchone()
            kind = db.execute(f'SELECT typeof("c{i}") FROM {tbl} WHERE "c{i}" IS NOT NULL LIMIT 1').fetchone()
            info.append(f"- {c}: {'number' if kind and kind[0] in ('integer', 'real') else 'text'}, {n:,} filled"
                        + (f", {nn:,} distinct" if nn >= 0 else "") + (f", range {_cell(mn)} .. {_cell(mx)}" if mn is not None else ""))
        parts.append("Columns:\n" + "\n".join(info) + ("\n(more columns not listed)" if len(cols) > 60 else ""))
        sample = db.execute(f"SELECT * FROM {tbl} LIMIT ?", (sample_n,)).fetchall()
        parts.append("First rows:\n" + _table(cols, [r[:len(cols)] for r in sample], 12_000))
    return {"text": "\n".join(parts)}


def op_rows(src: Path, db: sqlite3.Connection, p: dict[str, Any]) -> dict[str, Any]:
    _i, name, tbl, nrows, cols = _sheet_row(db, p.get("sheet"))
    want = p.get("columns") or cols
    for c in want:
        if c not in cols:
            raise TableError(f"unknown column '{c}'. Columns: {', '.join(cols)}")
    sel = ",".join(f'"c{cols.index(c)}"' for c in want)
    start, count = max(0, int(p.get("start", 0))), max(1, min(int(p.get("count", 20)), 200))
    rows = db.execute(f"SELECT {sel} FROM {tbl} LIMIT ? OFFSET ?", (count, start)).fetchall()
    return {"text": f"Sheet '{name}' rows {start + 1}-{start + len(rows)} of {nrows:,}\n" + _table(list(want), rows)}


_OPS = {"=": "=", "!=": "!=", ">": ">", ">=": ">=", "<": "<", "<=": "<="}
_AGG = {"count": "count", "sum": "sum", "avg": "avg", "min": "min", "max": "max", "count_distinct": "count"}


def op_query(src: Path, db: sqlite3.Connection, p: dict[str, Any]) -> dict[str, Any]:
    _i, name, tbl, nrows, cols = _sheet_row(db, p.get("sheet"))
    lower = {c.lower(): i for i, c in enumerate(cols)}

    def col(c: str) -> int:
        i = lower.get(str(c).lower())
        if i is None:
            raise TableError(f"unknown column '{c}'. Columns: {', '.join(cols)}")
        return i

    where, args = [], []
    for f in p.get("where") or []:
        i, op = col(f["col"]), f["op"]
        cn = f'"c{i}"'
        v = f.get("value")
        if op in _OPS:
            where.append(f"{cn} {_OPS[op]} ?")
            args.append(v)
        elif op == "contains":
            where.append(f"lower(CAST({cn} AS TEXT)) LIKE ? ESCAPE '\\'")
            args.append("%" + str(v).lower().replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_") + "%")
        elif op == "startswith":
            where.append(f"lower(CAST({cn} AS TEXT)) LIKE ? ESCAPE '\\'")
            args.append(str(v).lower().replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_") + "%")
        elif op == "in":
            vals = v if isinstance(v, list) else [v]
            where.append(f"{cn} IN ({','.join('?' * len(vals))})")
            args.extend(vals)
        elif op == "is_empty":
            where.append(f"({cn} IS NULL OR {cn} = '')")
        elif op == "not_empty":
            where.append(f"({cn} IS NOT NULL AND {cn} != '')")
        else:
            raise TableError(f"unsupported operator '{op}'")
    wsql = (" WHERE " + " AND ".join(where)) if where else ""
    group = [col(c) for c in (p.get("group_by") or [])]
    aggs = p.get("aggregates") or []
    limit = max(1, min(int(p.get("limit", 50)), 200))
    names: list[str] = []
    exprs: list[str] = []
    for g in group:
        exprs.append(f'"c{g}"')
        names.append(cols[g])
    for a in aggs:
        fn = a["fn"]
        if fn not in _AGG:
            raise TableError(f"unsupported aggregate '{fn}'")
        if a.get("col"):
            c = f'"c{col(a["col"])}"'
            inner = f"DISTINCT {c}" if fn == "count_distinct" else (f"CAST({c} AS REAL)" if fn in ("sum", "avg") else c)
        elif fn == "count":
            inner = "*"
        else:
            raise TableError(f"aggregate '{fn}' needs a column")
        exprs.append(f"{_AGG[fn]}({inner})")
        names.append(a.get("name") or (f"{fn}_{a.get('col')}" if a.get("col") else fn))
    if not exprs:
        sel = p.get("select") or cols
        exprs = [f'"c{col(c)}"' for c in sel]
        names = list(sel)
    order = []
    for o in p.get("order_by") or []:
        key = str(o["col"])
        if key in names:
            order.append(f'{names.index(key) + 1} {"DESC" if o.get("dir") == "desc" else "ASC"}')
        else:
            order.append(f'"c{col(key)}" {"DESC" if o.get("dir") == "desc" else "ASC"}')
    sql = f"SELECT {', '.join(exprs)} FROM {tbl}{wsql}"
    if group:
        sql += " GROUP BY " + ", ".join(f'"c{g}"' for g in group)
    if order:
        sql += " ORDER BY " + ", ".join(order)
    total_groups = None
    if group:
        total_groups = db.execute(f"SELECT count(*) FROM (SELECT 1 FROM {tbl}{wsql} GROUP BY " + ", ".join(f'"c{g}"' for g in group) + ")", args).fetchone()[0]
    t0 = time.time()
    rows = db.execute(sql + " LIMIT ?", (*args, limit)).fetchall()
    matched = db.execute(f"SELECT count(*) FROM {tbl}{wsql}", args).fetchone()[0]
    head = f"Sheet '{name}': {matched:,} of {nrows:,} rows match"
    if total_groups is not None:
        head += f"; {total_groups:,} groups" + (f" (showing the first {limit})" if total_groups > limit else "")
    elif matched > len(rows) and not aggs:
        head += f" (showing the first {len(rows)})"
    return {"text": head + f" [{time.time() - t0:.2f}s]\n" + _table(names, rows)}


# --------------------------------------------------------------------------------------------- plain text files
def op_text(src: Path, p: dict[str, Any]) -> dict[str, Any]:
    ext = src.suffix.lower()
    offset, maxc = max(0, int(p.get("offset", 0))), max(500, min(int(p.get("max_chars", 20000)), OUT_CHARS))
    if ext == ".docx":
        with zipfile.ZipFile(src) as z:
            root = ET.fromstring(_read_xml(z, "word/document.xml"))
        W = "{http://schemas.openxmlformats.org/wordprocessingml/2006/main}"
        text = "\n".join("".join(t.text or "" for t in para.iter(W + "t")) for para in root.iter(W + "p"))
    elif ext == ".pdf":
        from pypdf import PdfReader
        rd = PdfReader(str(src))
        text = "\n\n".join(f"[page {i + 1}]\n{(pg.extract_text() or '').strip()}" for i, pg in enumerate(rd.pages[:300]))
    else:
        raw = src.read_bytes()[: 40_000_000]
        try:
            text = raw.decode("utf-8-sig")
        except UnicodeDecodeError:
            text = raw.decode("cp1252", errors="replace")
    chunk = text[offset:offset + maxc]
    more = len(text) - offset - len(chunk)
    return {"text": chunk + (f"\n\n[{more:,} more characters; call again with offset={offset + len(chunk)}]" if more > 0 else "")}


# --------------------------------------------------------------------------------------------- entry
def handle(req: dict[str, Any]) -> dict[str, Any]:
    src = Path(req["path"])
    op = req["op"]
    if not src.is_file():
        raise TableError("the file is no longer there (moved, renamed or deleted)")
    ext = src.suffix.lower()
    params = req.get("params") or {}
    if op == "text" or (ext in TEXT_EXT and ext not in TABLE_EXT):
        if op != "text":
            raise TableError(f"{ext} files are documents, not tables: use localfile.text")
        return op_text(src, params)
    if ext == ".xls":
        raise TableError("old .xls files are not supported - save the workbook as .xlsx or .csv and attach that")
    if ext not in TABLE_EXT:
        raise TableError(f"unsupported file type {ext}")
    cache = Path(req["cache"])
    fn = {"inspect": op_inspect, "rows": op_rows, "query": op_query}.get(op)
    if fn is None:
        raise TableError("unknown operation")
    db = None
    try:
        db = ensure_cache(src, cache)
        return fn(src, db, params)
    except (zipfile.BadZipFile, ET.ParseError) as e:
        raise TableError(f"the workbook could not be read ({type(e).__name__}); is it password protected or damaged?") from e
    except sqlite3.Error as e:
        raise TableError(f"query failed: {e}") from e
    finally:
        if db is not None:
            db.close()           # a rebuild replaces the cache file, which Windows refuses while it is open


def main_table(header: dict[str, Any]) -> dict[str, Any]:
    try:
        return {"ok": True, **handle(header)}
    except TableError as e:
        return {"ok": False, "error": str(e)}
    except Exception as e:  # noqa: BLE001
        return {"ok": False, "error": f"{type(e).__name__}: {str(e)[:300]}"}


if __name__ == "__main__":      # developer aid: python -m pa_workers.parser.tables '{"op":"inspect","path":"x.xlsx","cache":"x.sqlite"}'
    print(json.dumps(main_table(json.loads(sys.argv[1])), ensure_ascii=False, indent=1))
