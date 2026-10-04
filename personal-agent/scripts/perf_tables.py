"""Performance check for the large-table engine: generates a big .xlsx (default ~100 MB) and times indexing + queries.

    python scripts\\perf_tables.py [rows]      (default 1_500_000 rows x 12 columns)
Not part of the test suite (it takes minutes); results are recorded in docs/TESTS.md.
"""
import os
import random
import sys
import tempfile
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "app"))
sys.path.insert(0, str(ROOT))
from pa_workers.parser import tables  # noqa: E402
from tests.helpers_xlsx import write_xlsx  # noqa: E402

N = int(sys.argv[1]) if len(sys.argv) > 1 else 1_500_000
REGIONS = ["North", "South", "East", "West", "Central", "Overseas", "Online", "Partner"]
rnd = random.Random(7)


def rows():
    yield ["OrderID", "Region", "Product", "Customer", "Units", "Price", "Discount", "Order date", "Ship date", "Status", "Notes", "Approver"]
    for i in range(N):
        yield [100000 + i, rnd.choice(REGIONS), f"Product {rnd.randint(1, 400)}", f"Customer {rnd.randint(1, 120000)} Pvt Ltd", rnd.randint(1, 500),
               round(rnd.random() * 900, 2), rnd.choice([0, 0.05, 0.1, 0.15]), 44000 + rnd.randint(0, 1500), 44010 + rnd.randint(0, 1500),
               rnd.choice(["Open", "Shipped", "Cancelled", "Returned"]), f"note {i % 5000} for the finance review team", f"Approver {rnd.randint(1, 60)}"]


d = Path(tempfile.mkdtemp(prefix="pa-perf-"))
f = d / "big.xlsx"
t0 = time.time()
write_xlsx(f, {"Orders": rows()}, date_cols=(7, 8))
print(f"generated {f.stat().st_size / 1e6:.0f} MB, {N:,} rows in {time.time() - t0:.0f}s")
cache = d / "big.sqlite"


def run(op, **params):
    t = time.time()
    r = tables.main_table({"mode": "table", "op": op, "path": str(f), "cache": str(cache), "params": params})
    return r, time.time() - t


r, dt = run("inspect")
print(f"first inspect (indexes the whole file): {dt:.1f}s ok={r['ok']} cache={cache.stat().st_size / 1e6:.0f} MB")
r, dt = run("inspect")
print(f"second inspect (from cache): {dt:.2f}s")
r, dt = run("query", group_by=["Region"], aggregates=[{"fn": "sum", "col": "Units", "name": "units"}, {"fn": "avg", "col": "Price"}, {"fn": "count"}],
            order_by=[{"col": "units", "dir": "desc"}])
print(f"group by region: {dt:.2f}s\n{r['text'][:400]}")
r, dt = run("query", where=[{"col": "Status", "op": "=", "value": "Cancelled"}, {"col": "Customer", "op": "contains", "value": "99999"}], select=["OrderID", "Customer", "Units"], limit=5)
print(f"filter + contains: {dt:.2f}s  {r['text'].splitlines()[0]}")
print("done; temp folder:", d)
for p in d.iterdir():
    os.remove(p)
d.rmdir()
