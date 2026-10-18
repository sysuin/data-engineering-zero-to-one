# Joining orders to the account version valid on their day: the right
# way, DuckDB's ASOF join, and two ways that look right and are not.
import json
from pathlib import Path

from _dims import built, tidy

con = built("pit")
con.execute("""
CREATE TEMP TABLE orders AS
SELECT o.order_id, i.account_id, o.placed_at,
       CAST(timezone(r.timezone, timezone('UTC', o.placed_at))
            AS DATE) AS order_date
FROM staging.stg_tradeline__orders AS o
JOIN intermediate.int_account_identity AS i ON i.crm_id = o.account_id
JOIN marts.dim_account AS a ON a.account_id = i.account_id
 AND a.is_current
JOIN marts.dim_region AS r USING (region_key)
WHERE o.status = 'placed'""")
pit = Path(__file__).with_name("pit_join.sql").read_text()

def matches(sql):
    """How many orders found no version, one, or more than one."""
    return con.execute(f"""
        WITH j AS ({sql}), n AS (
            SELECT o.order_id, count(j.order_id) AS k FROM orders AS o
            LEFT JOIN j USING (order_id) GROUP BY ALL)
        SELECT count(*) FILTER (k = 0), count(*) FILTER (k = 1),
               count(*) FILTER (k > 1) FROM n""").fetchone()

(n,) = con.execute("SELECT count(*) FROM orders").fetchone()
print(f"placed orders {n:,}")
print(f"{'join':<34}{'none':>8}{'one':>10}{'two':>8}")
asof = """SELECT o.order_id, o.order_date, v.account_key, v.owner
FROM orders AS o ASOF JOIN marts.dim_account AS v
  ON v.account_id = o.account_id AND o.order_date >= v.valid_from"""
inclusive = pit.replace("o.order_date < v.valid_to",
                        "o.order_date <= v.valid_to")
utc_day = pit.replace("o.order_date", "CAST(o.placed_at AS DATE)")
for name, sql in (("valid_from <= day < valid_to", pit),
                  ("ASOF JOIN", asof),
                  ("day <= valid_to (inclusive end)", inclusive),
                  ("the UTC date, not the depot's", utc_day)):
    none, one, more = matches(sql)
    print(f"{name:<34}{none:>8,}{one:>10,}{more:>8,}")

(differ,) = con.execute(f"""
    SELECT count(*) FROM ({pit}) AS a JOIN ({utc_day}) AS b
    USING (order_id) WHERE a.account_key <> b.account_key""").fetchone()
print(f"orders the UTC date joins to another version: {differ}")

# Account 1130's versions and orders, for Figure 12.2.
rows = con.execute(f"SELECT order_date, owner FROM ({pit}) WHERE "
                   "order_id IN (SELECT order_id FROM orders WHERE "
                   "account_id = 1130) ORDER BY 1").fetchall()
versions = con.execute(
    "SELECT valid_from, valid_to, owner, status, account_key FROM "
    "marts.dim_account WHERE account_id = 1130 ORDER BY 1").fetchall()
Path(__file__).with_suffix(".json").write_text(json.dumps({
    "orders": [[str(d), o] for d, o in rows],
    "versions": [[str(a), str(b) if b else None, o, s, k]
                 for a, b, o, s, k in versions]}, indent=1))
print(f"account 1130: {len(rows)} orders, last on {rows[-1][0]}")
tidy("pit")
