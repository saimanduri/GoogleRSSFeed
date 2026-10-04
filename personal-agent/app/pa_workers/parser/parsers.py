"""Document parsers used by pa-parser. Pure functions: bytes in, text + hidden content out.
Nothing here executes document content (no macros, no scripts, no external references)."""
from __future__ import annotations

import email
import email.policy
import io
import json
import re
import zipfile
from typing import Any
from xml.etree import ElementTree as ET

from pa_common.html_text import html_to_text

W = "{http://schemas.openxmlformats.org/wordprocessingml/2006/main}"
A = "{http://schemas.openxmlformats.org/drawingml/2006/main}"
S = "{http://schemas.openxmlformats.org/spreadsheetml/2006/main}"
MAX_TEXT = 2_000_000


def _xml(data: bytes) -> ET.Element:
    # defusedxml-equivalent protection: refuse DTDs/entities outright (billion laughs / XXE)
    if b"<!DOCTYPE" in data[:4096] or b"<!ENTITY" in data[:65536]:
        raise ValueError("XML with DTD/entities is not accepted")
    return ET.fromstring(data)


def pdf_page_images(data: bytes, max_pages: int = 12, max_total: int = 40_000_000) -> dict[str, Any]:
    """Scanned PDFs are pages that are one big picture. Without a PDF renderer we hand over the embedded JPEG of each page (the usual
    format of scanners and phone apps). Pages whose picture uses another encoding are counted in `skipped` so the user is told."""
    import base64
    import io

    from pypdf import PdfReader
    rd = PdfReader(io.BytesIO(data))
    pages, skipped, total = [], 0, 0
    n = min(len(rd.pages), max_pages)
    for i in range(n):
        best = None
        try:
            xo = rd.pages[i].get("/Resources", {}).get("/XObject", {})
            for key in list(xo)[:50]:
                obj = xo[key].get_object()
                if obj.get("/Subtype") != "/Image":
                    continue
                flt = obj.get("/Filter")
                flt = flt[0] if isinstance(flt, list) and len(flt) == 1 else flt
                if flt != "/DCTDecode":
                    continue
                raw = obj._data  # the JPEG file exactly as stored in the PDF (no decoding, no Pillow needed)
                if best is None or len(raw) > len(best):
                    best = raw
        except Exception:  # noqa: BLE001 - a damaged page only counts as skipped
            best = None
        if not best or total + len(best) > max_total:
            skipped += 1
            continue
        total += len(best)
        pages.append({"page": i + 1, "mime": "image/jpeg", "b64": base64.b64encode(best).decode()})
    return {"pages": pages, "skipped": skipped, "total_pages": len(rd.pages)}


def parse(family: str, name: str, data: bytes) -> dict[str, Any]:
    ext = name.lower().rsplit(".", 1)[-1] if "." in name else ""
    if family == "pdf":
        return parse_pdf(data)
    if family == "zip" and ext in ("docx", "docm"):
        return parse_docx(data)
    if family == "zip" and ext in ("xlsx", "xlsm"):
        return parse_xlsx(data)
    if family == "zip" and ext == "pptx":
        return parse_pptx(data)
    if family == "zip":
        zf = zipfile.ZipFile(io.BytesIO(data))
        return {"text": "Archive contents:\n" + "\n".join(i.filename for i in zf.infolist()[:500]), "hidden": [], "pages": 0}
    if family in ("html", "svg") or ext in ("html", "htm"):
        r = html_to_text(_decode(data))
        return {"text": (r["title"] + "\n\n" if r["title"] else "") + r["text"], "hidden": r["hidden"], "pages": 0}
    if ext == "eml":
        return parse_eml(data)
    if family == "rtf":
        return {"text": _strip_rtf(_decode(data)), "hidden": [], "pages": 0}
    if family in ("png", "jpeg", "gif"):
        return {"text": f"[image: {family}; text recognition (OCR) is not available in this version]", "hidden": [], "pages": 0}
    if family == "ole":
        return {"text": "[legacy Office binary format: only modern .docx/.xlsx/.pptx are parsed; please convert]", "hidden": [], "pages": 0}
    if family == "text":
        text = _decode(data)
        if ext == "json":
            try:
                text = json.dumps(json.loads(text), indent=1, ensure_ascii=False)
            except ValueError:
                pass
        return {"text": text[:MAX_TEXT], "hidden": [], "pages": 0}
    return {"text": f"[unsupported file type: {family}]", "hidden": [], "pages": 0}


def _decode(data: bytes) -> str:
    for enc in ("utf-8-sig", "utf-16"):
        try:
            return data.decode(enc)
        except UnicodeDecodeError:
            continue
    return data.decode("latin-1", errors="replace")


def parse_pdf(data: bytes) -> dict[str, Any]:
    from pypdf import PdfReader

    reader = PdfReader(io.BytesIO(data), strict=False)
    if reader.is_encrypted:
        try:
            reader.decrypt("")
        except Exception:  # noqa: BLE001
            return {"text": "[encrypted PDF - cannot be read]", "hidden": [], "pages": len(reader.pages)}
    parts, hidden = [], []
    for i, page in enumerate(reader.pages[:2000]):
        try:
            parts.append(f"\n--- page {i + 1} ---\n" + (page.extract_text() or ""))
        except Exception:  # noqa: BLE001
            parts.append(f"\n--- page {i + 1} --- [unreadable]")
        annots = page.get("/Annots")
        if annots:
            try:
                for a in annots:
                    obj = a.get_object()
                    c = obj.get("/Contents")
                    if c:
                        hidden.append({"kind": "pdf_annotation", "text": str(c)[:2000]})
            except Exception:  # noqa: BLE001
                pass
        if sum(len(p) for p in parts) > MAX_TEXT:
            break
    meta = reader.metadata or {}
    for k, v in dict(meta).items():
        if v:
            hidden.append({"kind": "metadata", "text": f"{k}: {str(v)[:500]}"})
    return {"text": "".join(parts)[:MAX_TEXT], "hidden": hidden[:200], "pages": len(reader.pages)}


def parse_docx(data: bytes) -> dict[str, Any]:
    zf = zipfile.ZipFile(io.BytesIO(data))
    root = _xml(zf.read("word/document.xml"))
    paras, hidden = [], []
    for p in root.iter(f"{W}p"):
        vis, hid = [], []
        for r in p.iter(f"{W}r"):
            rpr = r.find(f"{W}rPr")
            is_hidden = rpr is not None and (rpr.find(f"{W}vanish") is not None or _tiny(rpr) or _white(rpr))
            txt = "".join(t.text or "" for t in r.iter(f"{W}t"))
            (hid if is_hidden else vis).append(txt)
        if vis:
            paras.append("".join(vis))
        if "".join(hid).strip():
            hidden.append({"kind": "hidden_text", "text": "".join(hid)[:2000]})
    for extra, kind in (("word/comments.xml", "comment"), ("word/footnotes.xml", "footnote")):
        if extra in zf.namelist():
            txt = " ".join(t.text or "" for t in _xml(zf.read(extra)).iter(f"{W}t")).strip()
            if txt:
                hidden.append({"kind": kind, "text": txt[:4000]})
    hidden += _core_props(zf)
    return {"text": "\n".join(paras)[:MAX_TEXT], "hidden": hidden, "pages": 0}


def _tiny(rpr: ET.Element) -> bool:
    sz = rpr.find(f"{W}sz")
    return sz is not None and sz.get(f"{W}val", "20").isdigit() and int(sz.get(f"{W}val", "20")) <= 4


def _white(rpr: ET.Element) -> bool:
    c = rpr.find(f"{W}color")
    return c is not None and c.get(f"{W}val", "").upper() in ("FFFFFF",)


def _core_props(zf: zipfile.ZipFile) -> list[dict]:
    if "docProps/core.xml" not in zf.namelist():
        return []
    out = []
    for el in _xml(zf.read("docProps/core.xml")):
        if el.text and el.text.strip():
            out.append({"kind": "metadata", "text": f"{el.tag.split('}')[-1]}: {el.text.strip()[:500]}"})
    return out


def parse_xlsx(data: bytes) -> dict[str, Any]:
    zf = zipfile.ZipFile(io.BytesIO(data))
    shared: list[str] = []
    if "xl/sharedStrings.xml" in zf.namelist():
        for si in _xml(zf.read("xl/sharedStrings.xml")).iter(f"{S}si"):
            shared.append("".join(t.text or "" for t in si.iter(f"{S}t")))
    out = []
    sheets = sorted(n for n in zf.namelist() if re.match(r"xl/worksheets/sheet\d+\.xml$", n))
    for sheet in sheets[:50]:
        out.append(f"\n--- {sheet.rsplit('/', 1)[-1]} ---")
        for row in _xml(zf.read(sheet)).iter(f"{S}row"):
            cells = []
            for c in row.iter(f"{S}c"):
                v = c.find(f"{S}v")
                val = v.text if v is not None else ""
                if c.get("t") == "s" and val and val.isdigit() and int(val) < len(shared):
                    val = shared[int(val)]
                elif c.get("t") == "inlineStr":
                    val = "".join(t.text or "" for t in c.iter(f"{S}t"))
                cells.append(val or "")
            if any(cells):
                out.append("\t".join(cells))
            if sum(len(x) for x in out) > MAX_TEXT:
                break
    return {"text": "\n".join(out)[:MAX_TEXT], "hidden": _core_props(zf), "pages": len(sheets)}


def parse_pptx(data: bytes) -> dict[str, Any]:
    zf = zipfile.ZipFile(io.BytesIO(data))
    slides = sorted((n for n in zf.namelist() if re.match(r"ppt/slides/slide\d+\.xml$", n)),
                    key=lambda n: int(re.findall(r"\d+", n)[-1]))
    out, hidden = [], []
    for i, s in enumerate(slides):
        out.append(f"\n--- slide {i + 1} ---\n" + "\n".join(t.text or "" for t in _xml(zf.read(s)).iter(f"{A}t")))
    for n in zf.namelist():
        if re.match(r"ppt/notesSlides/notesSlide\d+\.xml$", n):
            txt = " ".join(t.text or "" for t in _xml(zf.read(n)).iter(f"{A}t")).strip()
            if txt:
                hidden.append({"kind": "speaker_notes", "text": txt[:2000]})
    return {"text": "\n".join(out)[:MAX_TEXT], "hidden": hidden + _core_props(zf), "pages": len(slides)}


def parse_eml(data: bytes) -> dict[str, Any]:
    msg = email.message_from_bytes(data, policy=email.policy.default)
    head = "\n".join(f"{h}: {msg.get(h, '')}" for h in ("From", "To", "Cc", "Date", "Subject"))
    body = msg.get_body(preferencelist=("plain", "html"))
    text = ""
    hidden: list[dict] = []
    if body is not None:
        content = body.get_content()
        if body.get_content_type() == "text/html":
            r = html_to_text(content)
            text, hidden = r["text"], r["hidden"]
        else:
            text = content
    atts = [p.get_filename() for p in msg.iter_attachments() if p.get_filename()]
    if atts:
        text += "\n\nAttachments (not extracted): " + ", ".join(atts)
    return {"text": (head + "\n\n" + text)[:MAX_TEXT], "hidden": hidden, "pages": 0}


def _strip_rtf(s: str) -> str:
    s = re.sub(r"\{\\\*[^{}]*\}", "", s)
    s = re.sub(r"\\'[0-9a-f]{2}", "", s)
    s = re.sub(r"\\[a-zA-Z]+-?\d* ?", "", s)
    return re.sub(r"[{}]", "", s).strip()[:MAX_TEXT]
