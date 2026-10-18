# Lazy evaluation: nothing runs until an answer is asked for, and then
# the engine plans the whole job at once, as Spark does.
import itertools
import json
import shutil
import sqlite3

import duckdb

from groundwork.config import SANDBOX, TRADELINE_DB

# 1. Laziness in plain Python: generators do no work until asked.
read = 0


def order_lines():
    global read
    con = sqlite3.connect(f"file:{TRADELINE_DB}?mode=ro", uri=True)
    for row in con.execute("SELECT order_id, qty, unit_price_cents "
                           "FROM order_lines ORDER BY order_id"):
        read += 1
        yield row


big = (r for r in order_lines() if r[1] * r[2] > 100_000)
cents = ((oid, q * c) for oid, q, c in big)
print(f"pipeline built; rows read so far: {read}")
first = list(itertools.islice(cents, 3))
print(f"asked for 3 lines over $1,000; rows read: {read:,}")

# 2. The same idea in an engine: build, then plan, then run.
out = SANDBOX / "ch20" / "lines"
out.parent.mkdir(parents=True, exist_ok=True)
db = duckdb.connect()
db.execute(f"ATTACH '{TRADELINE_DB}' AS t (TYPE sqlite, READ_ONLY)")
db.execute(f"""
    COPY (SELECT l.*, o.account_id, o.status,
                 substr(o.placed_at, 1, 7) AS utc_month
          FROM t.order_lines l JOIN t.orders o ON o.id = l.order_id)
    TO '{out}' (FORMAT parquet, PARTITION_BY (utc_month))""")
lines = db.read_parquet(f"{out}/*/*.parquet", hive_partitioning=True)
job = (lines.filter("utc_month = '2025-11' AND status = 'placed'")
            .aggregate("account_id, SUM(qty * unit_price_cents) AS c")
            .order("c DESC").limit(3))              # nothing read yet
(plan,) = db.execute("EXPLAIN (ANALYZE, FORMAT json) "
                     + job.sql_query()).fetchall()[0][1:]
BREAKERS = {"HASH_GROUP_BY", "TOP_N", "ORDER_BY", "HASH_JOIN"}


def show(node, depth=0):
    name = node["operator_name"].strip()
    mark = "  <- waits for all input" if name in BREAKERS else ""
    print(f"{'  ' * depth}{name:<{22 - 2 * depth}}"
          f"{node['operator_cardinality']:>9,} rows{mark}")
    info = node["extra_info"]
    if "Scanning Files" in info:
        pad = " " * (2 * depth + 2)
        print(f"{pad}files {info['Scanning Files']}, "
              f"filter {info['Filters']}")
    for child in node["children"]:
        show(child, depth + 1)


print()
show(json.loads(plan)["children"][0]["children"][0])
(total,) = db.execute(f"SELECT count(*) FROM '{out}/*/*.parquet'"
                      ).fetchone()
print(f"rows in the dataset: {total:,}")
print("top account:", job.fetchall()[0][0])
shutil.rmtree(SANDBOX / "ch20")
