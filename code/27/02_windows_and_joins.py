# A CTE, three window functions, and the same as-of join written twice.
import json

import duckdb

from groundwork.config import LANDING, REFERENCE, TRADELINE_DB

con = duckdb.connect()
con.execute(f"ATTACH '{TRADELINE_DB}' AS tl (TYPE sqlite, READ_ONLY)")
con.execute(f"""
CREATE TABLE nights AS
SELECT CAST(parse_path(filename)[-2] AS DATE) AS night,
       account_id, region, status, updated_at
FROM read_csv('{LANDING}/relay/20*/accounts.csv', filename = true,
              types = {{'updated_at': 'TIMESTAMP'}})""")

# Versions: a row for each night an account's status differed from the
# night before, valid until the next version starts (LAG, then LEAD).
con.execute("""
CREATE TABLE versions AS
WITH changes AS (
    SELECT *, LAG(status) OVER w AS before
    FROM nights
    WINDOW w AS (PARTITION BY account_id ORDER BY night))
SELECT account_id, region, status, updated_at, night AS valid_from,
       LEAD(night) OVER (PARTITION BY account_id ORDER BY night)
           AS valid_to
FROM changes
WHERE before IS NULL OR status <> before""")

# Each order's day is its depot's day, from the region's time zone.
con.execute(f"""
CREATE TABLE orders AS
FROM (SELECT o.id AS order_id, o.account_id, o.placed_at,
             CAST(timezone(r.timezone, timezone('UTC',
                  CAST(o.placed_at AS TIMESTAMP))) AS DATE) AS order_day
      FROM tl.orders AS o
      JOIN nights AS a ON a.account_id = o.account_id
       AND a.night = '2026-01-04'
      JOIN read_csv('{REFERENCE}/regions.csv') AS r
        ON r.name = a.region
      WHERE o.placed_at >= '2025-12-21')
WHERE order_day >= '2025-12-22'""")

RANGE = """
SELECT o.*, v.status, v.updated_at
FROM orders AS o JOIN versions AS v
  ON v.account_id = o.account_id AND o.order_day >= v.valid_from
 AND (o.order_day < v.valid_to OR v.valid_to IS NULL)"""
ASOF = """
SELECT o.*, v.status, v.updated_at
FROM orders AS o ASOF JOIN versions AS v
  ON v.account_id = o.account_id AND o.order_day >= v.valid_from"""

for label, sql in [
        ("rows in the stack of nights", "FROM nights"),
        ("versions (LAG finds changes)", "FROM versions"),
        ("latest row per account (QUALIFY)", """FROM (FROM nights
         QUALIFY ROW_NUMBER() OVER (PARTITION BY account_id
                                    ORDER BY night DESC) = 1)"""),
        ("orders from 22 December, depot day", "FROM orders"),
        ("matched by the range join", f"FROM ({RANGE})"),
        ("matched by the ASOF join", f"FROM ({ASOF})"),
        ("rows the two joins disagree on",
         f"FROM (({RANGE}) EXCEPT ({ASOF}))")]:
    (n,) = con.execute(f"SELECT count(*) {sql}").fetchone()
    print(f"{label:<38}{n:>8,}")

print("orders by the account's status on the order's day:")
for status, n in con.execute(f"SELECT status, count(*) FROM ({ASOF}) "
                             "GROUP BY ALL ORDER BY status").fetchall():
    print(f"  {status:<36}{n:>8,}")
for order, account, placed, closed in con.execute(
        "SELECT order_id, account_id, placed_at, updated_at "
        f"FROM ({ASOF}) WHERE status = 'closed' ORDER BY 1").fetchall():
    print(f"  order {order} placed {placed[:16]} UTC; account {account}"
          f"\n    closed {closed:%Y-%m-%d %H:%M} UTC")


def operators(node):
    """Every operator's name in a JSON plan, top first."""
    yield node["name"]
    for child in node["children"]:
        yield from operators(child)


for label, sql in [("range", RANGE), ("ASOF", ASOF)]:
    (_, plan), = con.execute(f"EXPLAIN (FORMAT json) {sql}").fetchall()
    names = [n for n in operators(json.loads(plan)[0])
             if n != "PROJECTION"]
    print(f"plan, {label + ' join:':<12}{' > '.join(names)}")
