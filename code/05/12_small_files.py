# nondeterministic: wall-clock timings, which differ by machine and run
# The same order lines as one file, a file a month and a file a day.
import shutil
import time

import duckdb
from _order_lines import HERE, lines_parquet

from groundwork.config import THREADS

con = duckdb.connect(config={"threads": THREADS})
src = lines_parquet()
out = HERE / "small"
shutil.rmtree(out, ignore_errors=True)
out.mkdir()
layouts = {"one_file": None, "by_month": "%Y-%m", "by_day": "%Y-%m-%d"}

print("layout       files       bytes   full scan   one month")
for name, fmt in layouts.items():
    if fmt is None:                   # the same writer, for fairness
        con.execute(f"""COPY (FROM '{src}') TO '{out / name}.parquet'
            (FORMAT parquet, COMPRESSION zstd)""")
        glob = f"{out / name}.parquet"
    else:
        con.execute(f"""COPY (SELECT *,
            strftime(placed_at, '{fmt}') AS p FROM '{src}')
            TO '{out / name}'
            (FORMAT parquet, PARTITION_BY (p), COMPRESSION zstd)""")
        glob = str(out / name / "*" / "*.parquet")
    files = con.execute(f"SELECT count(*), sum(size) FROM read_blob("
                        f"'{glob}')").fetchone()
    timings = []
    for where in ("true", "placed_at >= '2025-03-01' "
                          "AND placed_at < '2025-04-01'"):
        start = time.perf_counter()
        con.execute(f"SELECT sum(qty * unit_price_cents) FROM "
                    f"read_parquet('{glob}') WHERE {where}").fetchone()
        timings.append(time.perf_counter() - start)
    print(f"{name:<9} {files[0]:>8,} {files[1]:>11,}"
          f" {timings[0] * 1000:>8.0f} ms {timings[1] * 1000:>8.0f} ms")

shutil.rmtree(out)
