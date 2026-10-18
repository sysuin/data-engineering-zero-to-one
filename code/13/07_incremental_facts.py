# Two weeks of nightly fact loads from Tradeline as raw saw it at
# 02:05: append new orders only, merge by key with no lookback, merge
# with an hour's lookback, each set beside a full rebuild.
# timeout: 1500
import json
import sqlite3
from datetime import datetime, timedelta
from pathlib import Path

import duckdb
from meridian.sources import tradeline

from groundwork.config import SANDBOX

COPY = SANDBOX / "ch13" / "tradeline.db"
FIRST, NIGHTS = datetime(2025, 2, 1, 2, 5), 14
LINES = """SELECT l.order_id, l.line_no, l.qty, l.unit_price_cents
           FROM order_lines AS l JOIN orders AS o ON o.id = l.order_id
           WHERE o.status = 'placed' AND o.placed_at >= '2025-01-01'
             AND o.{col} >= ?"""
ORDERS = """SELECT id FROM orders
            WHERE placed_at >= '2025-01-01' AND {col} >= ?"""
WAYS = {"rebuild": None, "append": "created_at",
        "merge, no lookback": timedelta(0),
        "merge, 1 h lookback": timedelta(hours=1)}


def stamp(moment):
    return moment.strftime("%Y-%m-%d %H:%M:%S")


def source(moment):
    """Tradeline as it stood at a moment: a read-only connection."""
    tradeline.at(moment, COPY)
    return sqlite3.connect(f"file:{COPY}?mode=ro", uri=True)


db = duckdb.connect()
src = source(FIRST)
first = src.execute(LINES.format(col="created_at"), ["2000"]).fetchall()
for k, way in enumerate(WAYS):
    db.execute(f"CREATE TABLE t{k} (order_id BIGINT, line_no INT, "
               "qty INT, cents BIGINT)")
    db.executemany(f"INSERT INTO t{k} VALUES (?, ?, ?, ?)", first)

print(f"{'run (UTC)':<17}{'new':>5}{'changed':>9}{'lookback only':>15}")
log = []
for night in range(1, NIGHTS + 1):
    was = FIRST + timedelta(days=night - 1)
    src = source(was + timedelta(days=1))
    new = src.execute(LINES.format(col="created_at"),
                      [stamp(was)]).fetchall()
    db.executemany("INSERT INTO t1 VALUES (?, ?, ?, ?)",
                   [r for r in new])
    seen = {}
    for k, back in ((2, WAYS["merge, no lookback"]),
                    (3, WAYS["merge, 1 h lookback"])):
        since = [stamp(was - back)]
        ids = [i for (i,) in src.execute(
            ORDERS.format(col="updated_at"), since)]
        db.execute(f"DELETE FROM t{k} WHERE list_contains(?, order_id)",
                   [ids])
        db.executemany(f"INSERT INTO t{k} VALUES (?, ?, ?, ?)",
                       src.execute(LINES.format(col="updated_at"),
                                   since).fetchall())
        seen[k] = len(ids)
    n_new = len({r[0] for r in new})
    late = seen[3] - seen[2]
    log.append([stamp(was + timedelta(days=1)), n_new, seen[3], late])
    print(f"{was + timedelta(days=1):%a %d %b %H:%M}{n_new:>6}"
          f"{seen[3]:>9}{late:>15}")

db.execute("DELETE FROM t0")
db.executemany("INSERT INTO t0 VALUES (?, ?, ?, ?)",
               src.execute(LINES.format(col="created_at"),
                           ["2000"]).fetchall())
src.close()
print(f"\n{'after 14 nights':<21}{'lines':>8}{'revenue':>15}"
      f"{'lines wrong':>13}")
result = {}
for k, way in enumerate(WAYS):
    n, cents = db.execute(f"SELECT count(*), sum(qty * cents) "
                          f"FROM t{k}").fetchone()
    (bad,) = db.execute(f"""SELECT count(*) FROM (
        (SELECT * FROM t{k} EXCEPT ALL SELECT * FROM t0) UNION ALL
        (SELECT * FROM t0 EXCEPT ALL SELECT * FROM t{k}))""").fetchone()
    result[way] = [n, cents, bad]
    print(f"{way:<21}{n:>8,}{cents / 100:>15,.2f}{bad:>13,}")
Path(__file__).with_suffix(".json").write_text(json.dumps(
    {"nights": log, "result": result}, indent=1))
COPY.unlink()
