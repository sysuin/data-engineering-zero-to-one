# A week of Relay's account exports laid out as raw partitions, and read
# back through one view.
import shutil
from datetime import date, datetime, time, timedelta, timezone

import duckdb

from groundwork.config import LANDING, SANDBOX
from groundwork.storage import partitions, raw_views, write_partition

root = SANDBOX / "ch05" / "raw"
shutil.rmtree(root, ignore_errors=True)
con = duckdb.connect()

source_rows = {}
for i in range(7):
    night = date(2025, 12, 29) + timedelta(days=i)
    path = LANDING / "relay" / night.isoformat() / "accounts.csv"
    # Raw keeps what the source sent: every column as the text it was.
    rows = con.sql(f"FROM read_csv('{path}', all_varchar = true)")
    run = datetime.combine(night + timedelta(days=1), time(6),
                           timezone.utc)          # the nightly run, D+1
    write_partition(rows, "relay", "accounts", night, root=root,
                    loaded_at=run, load_id=f"relay-accounts-{night}",
                    source_file=str(path.relative_to(LANDING)))
    source_rows[night] = path.read_bytes().count(b"\n") - 1

for p in sorted(root.rglob("*.parquet"))[-2:]:
    print(p.relative_to(root.parent))
print(f"partitions: {len(partitions('relay', 'accounts', root=root))}")

print(raw_views(con, root=root))
print("load_date   rows in raw  rows in file  _source_file")
for day, n, src in con.execute("""SELECT load_date, count(*),
        any_value(_source_file) FROM raw.relay_accounts
        GROUP BY 1 ORDER BY 1""").fetchall():
    print(f"{day}  {n:>11,}  {source_rows[day]:>12,}  {src}")

# A filter on load_date is answered from folder names: one file read.
plan = con.execute("""EXPLAIN ANALYZE SELECT count(*)
    FROM raw.relay_accounts WHERE load_date = DATE '2026-01-04'
    """).fetchone()[1]
files = [ln.strip(" │") for ln in plan.splitlines() if "Files" in ln]
print("plan:", "; ".join(f.strip() for f in files))
