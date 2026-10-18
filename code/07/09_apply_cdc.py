# Snapshot plus log: copy Tradeline once, note the log position, then
# apply each night's entries in lsn order; and a handover after a gap.
import json
import sqlite3
from datetime import timedelta
from pathlib import Path

import duckdb
import pyarrow as pa

from _watermark import DB, FIRST, KEYS, nights
from meridian.sources import tradeline

db = duckdb.connect()
db.execute(f"ATTACH '{DB}' AS src (TYPE sqlite, READ_ONLY)")
COLS = {t: [c for (c,) in db.execute(
    f"SELECT name FROM pragma_table_info('src.{t}')").fetchall()]
    for t in KEYS}
db.execute("DETACH src")


def snapshot(src):
    """Every row, and the log position the copy stands at. (This copy
    is still while it is read; a live database needs one transaction
    for both, as Groundwork's loader uses.)"""
    (at,) = src.execute("SELECT max(lsn) FROM replication_log"
                        ).fetchone()
    for t in KEYS:
        db.execute(f"CREATE OR REPLACE TABLE snap_{t} AS SELECT * "
                   f"FROM sqlite_scan('{DB}', '{t}')")
        db.execute(f"CREATE OR REPLACE TABLE log_{t} AS SELECT *, "
                   f"0 AS lsn, '' AS op FROM snap_{t} LIMIT 0")
    return at


def apply(src, at):
    """Keep the log's entries after position `at`; return the new."""
    for t in KEYS:
        entries = src.execute(
            "SELECT lsn, op, row_json FROM replication_log WHERE "
            "table_name = ? AND lsn > ? ORDER BY lsn", (t, at))
        rows = [{**json.loads(js), "lsn": lsn, "op": op}
                for lsn, op, js in entries]
        if rows:
            db.register("batch", pa.Table.from_pylist(rows))
            db.execute(f"INSERT INTO log_{t} BY NAME FROM batch")
    (top,) = src.execute("SELECT max(lsn) FROM replication_log"
                         ).fetchone()
    return max(at, top)


def copy(t, base, newest_first=False):
    """The snapshot with every later entry applied: last one wins."""
    cols, key = ", ".join(COLS[t]), KEYS[t]
    order = "lsn" if newest_first else "lsn DESC"
    return (f"(SELECT {cols} FROM (SELECT *, {base} AS lsn, 'I' AS op "
            f"FROM snap_{t} UNION ALL BY NAME SELECT * FROM log_{t} "
            f"WHERE lsn > {base}) QUALIFY row_number() OVER (PARTITION "
            f"BY {key} ORDER BY lsn = {base}, {order}) = 1 "
            "AND op <> 'D')")


def differ(base, newest_first=False):
    """Rows on one side and not the other, against Tradeline now."""
    db.execute(f"ATTACH '{DB}' AS src (TYPE sqlite, READ_ONLY)")
    n = 0
    for t in KEYS:
        ours = copy(t, base, newest_first)
        for a, b in ((ours, f"src.{t}"), (f"src.{t}", ours)):
            n += db.execute(f"SELECT count(*) FROM (FROM {a} "
                            f"EXCEPT FROM {b})").fetchone()[0]
    db.execute("DETACH src")
    return n


for night, moment, src in nights():
    if night == 0:
        base = at = snapshot(src)
        print(f"snapshot {moment:%Y-%m-%d %H:%M}, at lsn {at:,}")
    else:
        at = apply(src, at)
ops = dict(db.execute("SELECT op, count(*) FROM (FROM log_orders "
                      "UNION ALL FROM log_order_lines) GROUP BY op"
                      ).fetchall())
print(f"30 nights of the log, to lsn {at:,}: " +
      ", ".join(f"{ops[op]:,} {op}" for op in "IUD"))
print(f"rows that differ from Tradeline, {moment:%Y-%m-%d}: "
      f"{differ(base)}")
print(f"  the same entries applied newest first: "
      f"{differ(base, newest_first=True):,}")

away = moment + timedelta(days=9)          # the loader misses 9 nights
tradeline.at(away, DB)
src = sqlite3.connect(f"file:{DB}?mode=ro", uri=True)
(lo,) = src.execute("SELECT min(lsn) FROM replication_log").fetchone()
print(f"away until {away:%Y-%m-%d %H:%M}: the log now starts at "
      f"lsn {lo:,}")
print(f"  {lo - at - 1:,} changes after lsn {at:,} are gone from it")
start, base = base, snapshot(src)
print(f"  a new snapshot at lsn {base:,}; rows that differ: "
      f"{differ(base)}")
Path(__file__).with_suffix(".json").write_text(json.dumps(
    {"first": [str(FIRST), start], "applied_to": at, "log_from": lo,
     "away": str(away), "resnapshot": base}, indent=1) + "\n")
