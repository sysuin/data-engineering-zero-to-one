# Exercise 5.3: Parquet against CSV on a small file, the catalogue.
import shutil

import duckdb

from groundwork.config import LANDING, SANDBOX

src = LANDING / "catalogue" / "catalogue.csv"
out = SANDBOX / "ch05" / "catalogue"
out.mkdir(parents=True, exist_ok=True)
con = duckdb.connect()
table = f"read_csv('{src}')"
(rows,) = con.execute(f"SELECT count(*) FROM {table}").fetchone()
cols = len(con.execute(f"DESCRIBE FROM {table}").fetchall())
con.execute(f"COPY (FROM read_csv('{src}', all_varchar = true)) "
            f"TO '{out}/catalogue.parquet' (FORMAT parquet, "
            "COMPRESSION zstd)")
csv_bytes = src.stat().st_size
pq_bytes = (out / "catalogue.parquet").stat().st_size
print(f"catalogue.csv: {rows:,} rows, {cols} columns")
print(f"  CSV      {csv_bytes:>8,} bytes")
share = pq_bytes / csv_bytes
print(f"  Parquet  {pq_bytes:>8,} bytes  {share:.0%} of the CSV")
shutil.rmtree(out)
