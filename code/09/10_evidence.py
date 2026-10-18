# Every file source loaded as Groundwork's Build loads it, into the
# sandbox; then one raw row traced back to the bytes it came from.
import hashlib
import io
import json
import shutil
from datetime import date, timedelta
from pathlib import Path

import duckdb
import openpyxl

from groundwork.config import LANDING, NOW, SANDBOX
from groundwork.ingest import catalogue, files, ledger, reference
from groundwork.ingest import relay, suppliers

here = SANDBOX / "ch09" / "evidence"
shutil.rmtree(here, ignore_errors=True)
where = dict(root=here / "raw", warehouse=here / "warehouse.duckdb",
             archive=here / "landed")

night = date(2025, 12, 22)               # the fortnight on disk
while night <= date(2026, 1, 4):
    relay.load(night, **where)           # at 06:00 the next day
    night += timedelta(days=1)
day, at = date(2026, 1, 4), NOW.replace(tzinfo=None)
for source in (suppliers, catalogue, ledger, reference):
    source.load(day, run_at=at, **where)

con = duckdb.connect(str(where["warehouse"]))
summary = con.sql("""
    SELECT source, count(*) FILTER (status = 'loaded'),
           count(*) FILTER (status = 'duplicate'),
           count(DISTINCT sha256), sum(rows), sum(size)
    FROM audit.file_ledger
    GROUP BY source ORDER BY source""").fetchall()
kept = {s: sum(p.stat().st_size for p in (here / "landed" / s).rglob(
    "*.gz")) for s, *_ in summary}
print(f"{'source':<10} {'loaded':>6} {'dup':>4} {'distinct':>8} "
      f"{'rows':>8} {'landed':>10} {'kept':>9}")
for s, n, d, distinct, rows, size in summary:
    print(f"{s:<10} {n:>6} {d:>4} {distinct:>8} {rows:>8,} "
          f"{size:>10,} {kept[s]:>9,}")
(views,) = con.sql("SELECT count(*) FROM duckdb_views() "
                   "WHERE schema_name = 'raw'").fetchone()
print(f"raw views: {views}; relay nights in raw: "
      f"{len(list((here / 'raw/relay/accounts').iterdir()))}")

# One row of raw, followed back to the bytes it was read from.
row = con.sql("""
    SELECT sku, cost, file, file_sha256 FROM raw.suppliers_price_lists
    WHERE layout = 'aldridge' AND cost = '790.7'""").fetchone()
sku, cost, name, sha = row
print(f"\nraw: {sku} cost {cost}")
print(f"  from {name.removeprefix('suppliers/aldridge-co/')}, "
      f"sha256 {sha[:12]}")
data = files.read_kept("suppliers", sha, where["archive"])
cell = openpyxl.load_workbook(io.BytesIO(data)).worksheets[0]["C5"]
gz = files.kept_path("suppliers", sha, where["archive"])
print(f"  kept as {gz.name[:12]}....gz, {gz.stat().st_size:,} bytes; "
      f"unzipped, same hash: {hashlib.sha256(data).hexdigest() == sha}")
print(f"  cell {cell.coordinate} in the kept copy: {cell.value}")

on_disk = sorted(p.name for p in (LANDING / "relay").glob("20*"))
print(f"\nRelay nights still in the landing zone: {len(on_disk)}, "
      f"{on_disk[0]} to {on_disk[-1]}")

sample = con.sql("""
    SELECT name, sha256, arrived_at, status, logical_key, rows
    FROM audit.file_ledger
    WHERE name IN ('relay/2026-01-04/accounts.csv',
                   'catalogue/catalogue.csv')
       OR logical_key = 'aldridge-co/2024-03'
    ORDER BY arrived_at""").fetchall()
Path(__file__).with_suffix(".json").write_text(json.dumps({
    "sources": [dict(zip(("source", "loaded", "duplicate", "distinct",
                          "rows", "bytes"), r), kept=kept[r[0]])
                for r in summary],
    "sample": [[str(v) for v in r] for r in sample]}, indent=1))
shutil.rmtree(here)
