"""Large-table engine: streaming .xlsx / .csv into a SQLite cache and declarative queries (no SQL from the model)."""
import os
import time

import pytest

from pa_workers.parser import tables
from tests.helpers_xlsx import write_xlsx

HEAD = ["Region", "Product", "Units", "Price", "Order date", "Approved"]
ROWS = [
    ["North", "Pen", 10, 1.5, 46000, True],
    ["North", "Book", 3, 12.0, 46001, False],
    ["South", "Pen", 25, 1.5, 46002, True],
    ["South", "Lamp", 2, 30.25, 46003, None],
    ["East", "Pen", 7, 1.75, 46004, True],
    [None, "Orphan", 1, 9.0, None, None],
]


def req(path, op, cache, **params):
    return tables.main_table({"mode": "table", "op": op, "path": str(path), "cache": str(cache), "params": params})


@pytest.fixture
def xlsx(tmp_path):
    p = tmp_path / "sales.xlsx"
    write_xlsx(p, {"Sales": [HEAD] + ROWS, "Notes": [["Key", "Value"], ["owner", "Anita"]]}, date_cols=(4,))
    return p


def test_inspect_lists_sheets_columns_types_and_dates(xlsx, tmp_path):
    r = req(xlsx, "inspect", tmp_path / "c.sqlite")
    assert r["ok"], r
    t = r["text"]
    assert "Sheet 'Sales': 6 data rows x 6 columns" in t and "Sheet 'Notes': 1 data rows x 2 columns" in t
    assert "Units: number" in t and "Region: text" in t
    assert "2025-12-" in t or "2026-" in t          # 46000 is an Excel date serial (style 14) -> ISO date, not a number
    assert "| North | Pen | 10 | 1.5 |" in t


def test_rows_pagination_and_column_selection(xlsx, tmp_path):
    r = req(xlsx, "rows", tmp_path / "c.sqlite", sheet="Sales", start=1, count=2, columns=["Product", "Units"])
    assert r["ok"] and "rows 2-3 of 6" in r["text"] and "| Book | 3 |" in r["text"] and "| Pen | 25 |" in r["text"]


def test_group_by_sum_avg_distinct_and_order(xlsx, tmp_path):
    q = dict(sheet="Sales", group_by=["Region"], aggregates=[{"fn": "sum", "col": "Units", "name": "total"}, {"fn": "count"},
                                                             {"fn": "count_distinct", "col": "Product", "name": "products"}],
             order_by=[{"col": "total", "dir": "desc"}])
    r = req(xlsx, "query", tmp_path / "c.sqlite", **q)
    assert r["ok"], r
    lines = [x for x in r["text"].splitlines() if x.startswith("| ") and "---" not in x]
    assert lines[1].startswith("| South | 27 | 2 | 2 |") and lines[2].startswith("| North | 13 | 2 | 2 |")


def test_where_operators(xlsx, tmp_path):
    c = tmp_path / "c.sqlite"
    r = req(xlsx, "query", c, sheet="Sales", where=[{"col": "product", "op": "contains", "value": "pe"}, {"col": "Units", "op": ">=", "value": 10}],
            select=["Region", "Units"])
    assert r["ok"] and "2 of 6 rows match" in r["text"]
    r = req(xlsx, "query", c, sheet="Sales", where=[{"col": "Region", "op": "in", "value": ["North", "East"]}], aggregates=[{"fn": "sum", "col": "Units"}])
    assert "| 20 |" in r["text"]
    r = req(xlsx, "query", c, sheet="Sales", where=[{"col": "Region", "op": "is_empty"}], select=["Product"])
    assert "| Orphan |" in r["text"]
    r = req(xlsx, "query", c, sheet="Sales", where=[{"col": "Product", "op": "startswith", "value": "l"}], select=["Product"])
    assert "| Lamp |" in r["text"]


def test_no_sql_injection_through_names_or_values(xlsx, tmp_path):
    c = tmp_path / "c.sqlite"
    r = req(xlsx, "query", c, sheet="Sales", select=['Units"; DROP TABLE t0; --'])
    assert not r["ok"] and "unknown column" in r["error"]
    r = req(xlsx, "query", c, sheet="Sales", where=[{"col": "Region", "op": "=", "value": "x' OR '1'='1"}], select=["Region"])
    assert r["ok"] and "0 of 6 rows match" in r["text"]
    r = req(xlsx, "query", c, sheet="Sales", where=[{"col": "Region", "op": "LIKE", "value": "%"}], select=["Region"])
    assert not r["ok"] and "unsupported operator" in r["error"]
    assert req(xlsx, "query", c, sheet="Nope", select=["Region"])["error"].startswith("no sheet named")
    assert req(xlsx, "rows", c, sheet="Sales")["ok"]        # the table is still intact


def test_inline_strings_and_sparse_cells(tmp_path):
    p = tmp_path / "inline.xlsx"
    write_xlsx(p, {"S": [["a", "b", "c"], ["x", None, "z"], [None, None, "only c"]]}, shared=False)
    r = req(p, "rows", tmp_path / "c.sqlite", sheet="S")
    assert r["ok"] and "| x |  | z |" in r["text"] and "|  |  | only c |" in r["text"]


def test_cache_is_reused_and_rebuilt_when_file_changes(xlsx, tmp_path):
    c = tmp_path / "c.sqlite"
    assert req(xlsx, "inspect", c)["ok"]
    m1 = c.stat().st_mtime_ns
    assert req(xlsx, "rows", c)["ok"]
    assert c.stat().st_mtime_ns == m1                   # second question did not re-read the workbook
    write_xlsx(xlsx, {"Sales": [HEAD] + ROWS[:2]})
    os.utime(xlsx, (time.time() + 5, time.time() + 5))
    r = req(xlsx, "inspect", c)
    assert "2 data rows" in r["text"]                    # edited file: re-indexed automatically


def test_csv_semicolon_cp1252_and_numbers(tmp_path):
    p = tmp_path / "data.csv"
    p.write_bytes("Name;Amount;City\nZoë;1250,50;Köln\nBob;7;Pune\nAnu;30;Pune\n".encode("cp1252"))
    r = req(p, "query", tmp_path / "c.sqlite", group_by=["City"], aggregates=[{"fn": "sum", "col": "Amount"}], order_by=[{"col": "City"}])
    assert r["ok"], r
    assert "| Köln | 0" in r["text"] or "| Köln |" in r["text"]
    assert "| Pune | 37 |" in r["text"]


def test_old_xls_and_bad_files_give_clear_errors(tmp_path):
    x = tmp_path / "old.xls"
    x.write_bytes(b"\xd0\xcf\x11\xe0")
    assert "save the workbook as .xlsx" in req(x, "inspect", tmp_path / "c.sqlite")["error"]
    bad = tmp_path / "bad.xlsx"
    bad.write_bytes(b"not a zip")
    assert not req(bad, "inspect", tmp_path / "c2.sqlite")["ok"]
    assert "no longer there" in req(tmp_path / "gone.xlsx", "inspect", tmp_path / "c3.sqlite")["error"]


def test_text_files_paged(tmp_path):
    p = tmp_path / "notes.txt"
    p.write_text("hello " * 1000, encoding="utf-8")
    r = req(p, "text", tmp_path / "x", offset=0, max_chars=500)
    assert r["ok"] and "more characters" in r["text"] and "offset=500" in r["text"]
