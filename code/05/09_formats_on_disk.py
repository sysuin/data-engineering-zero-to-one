# The same 1.2 million order lines, written six ways, measured in bytes.
import gzip
import json
import shutil
from pathlib import Path

import pyarrow.parquet as pq
from _order_lines import HERE, lines_csv, lines_parquet

table = pq.read_table(lines_parquet())
out = HERE / "formats"
out.mkdir(exist_ok=True)
sizes = {}

# Text: JSON Lines, CSV, and CSV through gzip.
with open(out / "lines.jsonl", "w") as f:
    for batch in table.to_batches(max_chunksize=50_000):
        for row in batch.to_pylist():
            row["placed_at"] = row["placed_at"].isoformat(sep=" ")
            f.write(json.dumps(row) + "\n")
sizes["JSON Lines"] = (out / "lines.jsonl").stat().st_size
sizes["CSV"] = lines_csv().stat().st_size
with open(lines_csv(), "rb") as src, \
        gzip.GzipFile(out / "lines.csv.gz", "wb", mtime=0) as dst:
    shutil.copyfileobj(src, dst)
sizes["CSV, gzip"] = (out / "lines.csv.gz").stat().st_size

# Parquet: values plain, then encoded, then encoded and compressed.
ways = {"Parquet, plain": dict(compression="none",
                               use_dictionary=False),
        "Parquet, encoded": dict(compression="none"),
        "Parquet, snappy": dict(compression="snappy"),
        "Parquet, zstd": dict(compression="zstd")}
for name, options in ways.items():
    path = out / "lines.parquet"
    pq.write_table(table, path, row_group_size=100_000, **options)
    sizes[name] = path.stat().st_size

print(f"{table.num_rows:,} rows, {table.num_columns} columns")
csv = sizes["CSV"]
for name, n in sizes.items():
    print(f"  {name:<18} {n:>12,} bytes  {n / csv:>6.1%} of the CSV")

shutil.rmtree(out)                  # keep the numbers, not the files
Path(__file__).with_suffix(".json").write_text(json.dumps(sizes))
