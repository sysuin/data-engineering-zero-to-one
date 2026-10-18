# What the footer lets a reader skip, for one week's revenue, when the
# rows are in time order and when they are not.
from datetime import datetime

import duckdb
import pyarrow.parquet as pq
from _order_lines import lines_parquet, lines_parquet_by_sku

from groundwork.config import THREADS, TRADELINE_DB

by_time = lines_parquet()           # order_id order, so time order
by_sku = lines_parquet_by_sku()     # the same rows, sorted by SKU

lo, hi = datetime(2025, 3, 3), datetime(2025, 3, 10)   # one week, UTC
wanted = ["placed_at", "qty", "unit_price_cents"]
print(f"revenue, {lo:%d %b} to {hi:%d %b %Y}: {', '.join(wanted)}")
print("file, 13 row groups each read  bytes read  file bytes   share")
for path in (by_time, by_sku):
    meta = pq.ParquetFile(path).metadata
    read = 0
    groups = []
    for g in range(meta.num_row_groups):
        rg = meta.row_group(g)
        s = rg.column(3).statistics                 # placed_at
        if s.max < lo or s.min >= hi:
            continue                                # skipped, unread
        groups.append(g)
        read += sum(rg.column(i).total_compressed_size
                    for i in range(rg.num_columns)
                    if rg.column(i).path_in_schema in wanted)
    size = path.stat().st_size
    print(f"{path.name:<26} {len(groups):>2} {read:>11,} {size:>11,}"
          f" {read / size:>7.1%}")

# Same answer from both files, and from Tradeline itself.
con = duckdb.connect(config={"threads": THREADS})
con.execute(f"ATTACH '{TRADELINE_DB}' AS t (TYPE sqlite, READ_ONLY)")
q = """SELECT sum(qty * unit_price_cents) FROM {}
       WHERE placed_at >= '{}' AND placed_at < '{}'"""
sources = [("by time", f"'{by_time}'"), ("by sku", f"'{by_sku}'"),
           ("tradeline",
            "t.order_lines JOIN t.orders ON id = order_id")]
for name, source in sources:
    cents = con.execute(q.format(source, lo, hi)).fetchone()[0]
    print(f"  {name:<10} {cents:>14,} cents")
