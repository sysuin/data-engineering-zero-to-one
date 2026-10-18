# nondeterministic: wall-clock timings, which differ by machine and run
# The same week's revenue, timed from a CSV file and two Parquet files.
import time

import duckdb
from _order_lines import lines_csv, lines_parquet, lines_parquet_by_sku

from groundwork.config import THREADS

files = {"csv": lines_csv(), "parquet, by time": lines_parquet(),
         "parquet, by sku": lines_parquet_by_sku()}
con = duckdb.connect(config={"threads": THREADS})
query = """SELECT sum(qty * unit_price_cents) FROM '{}'
           WHERE placed_at >= '2025-03-03'
             AND placed_at <  '2025-03-10'"""

print("file                 bytes on disk   answer (cents)  best of 5")
for name, path in files.items():
    runs = []
    for _ in range(5):
        start = time.perf_counter()
        cents = con.execute(query.format(path)).fetchone()[0]
        runs.append(time.perf_counter() - start)
    print(f"{name:<18} {path.stat().st_size:>15,} {cents:>16,}"
          f" {min(runs) * 1000:>7.1f} ms")
