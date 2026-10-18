# Tradeline's replication log: every committed change of the last seven
# days, in the order it was committed, deletes included.
import json
import sqlite3
from datetime import timedelta
from pathlib import Path

from _watermark import DB, FIRST
from meridian.sources import tradeline

moment = FIRST + timedelta(days=30)          # the thirtieth night
tradeline.at(moment, DB)
src = sqlite3.connect(f"file:{DB}?mode=ro", uri=True)
(n, lo, hi, first, last) = src.execute(
    "SELECT count(*), min(lsn), max(lsn), min(committed_at), "
    "max(committed_at) FROM replication_log").fetchone()
print(f"replication_log at {moment:%Y-%m-%d %H:%M}: {n:,} entries")
print(f"  lsn {lo:,} to {hi:,}")
print(f"  committed {first} to {last}")
print(f"{'':<12} {'I':>6} {'U':>6} {'D':>6}")
for table in ("orders", "order_lines"):
    ops = dict(src.execute("SELECT op, count(*) FROM replication_log "
                           "WHERE table_name = ? GROUP BY op",
                           (table,)).fetchall())
    print(f"{table:<12}", *(f"{ops.get(o, 0):>6,}" for o in "IUD"))

ENTRIES = """SELECT lsn, committed_at, table_name, op, row_json
    FROM replication_log WHERE json_extract(row_json, '$.id') = ?
       OR json_extract(row_json, '$.order_id') = ? ORDER BY lsn"""
saved = {}
for label, oid in (("amended by the job", 440930),   # listing 03's
                   ("a test order", 440723)):        # listing 07's
    print(f"order {oid}, {label}:")
    saved[oid] = []
    for lsn, at, table, op, row in src.execute(ENTRIES, (oid, oid)):
        row = json.loads(row)
        what = (f"line {row['line_no']}" if "line_no" in row
                else "order")
        detail = ("" if op == "D" else f"qty {row['qty']}, "
                  if "qty" in row else f"{row['status']}, ")
        if op != "D":
            detail += f"updated {row['updated_at'][11:]}"
        print(f"  {lsn:>9,} {at} {op} {what:<7} {detail}".rstrip())
        saved[oid].append([lsn, at, table, op, row])
Path(__file__).with_suffix(".json").write_text(
    json.dumps(saved, indent=1) + "\n")
