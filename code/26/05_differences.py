# Where Groundwork differs from Book 2 on purpose, measured: the old
# CRM's ids, the history's grain, columns the marts left out, what
# Groundwork keeps that Book 2 never had, and how money is added up.
import sqlite3

import duckdb
from _sandbox import fresh

from groundwork import reconcile_books as rb
from groundwork.config import BOOK2_WAREHOUSE, THREADS

con = duckdb.connect(str(fresh()), read_only=True,
                     config={"threads": THREADS})
one = lambda sql: con.execute(sql).fetchone()            # noqa: E731

print("1. The old CRM's ids: resolved in the marts, kept in compat")
got = dict(con.execute("""SELECT matched_by, count(*) FROM
                         intermediate.int_account_identity GROUP BY 1"""
                      ).fetchall())
print(f"  CRM records {sum(got.values()):,}: Relay's own "
      f"{got['relay']:,}, IT's map {got['it_map']}, the rule "
      f"{got['rule']}")
orders, ids = one("SELECT count(*), count(DISTINCT account_id) FROM "
                  "compat.orders WHERE account_id >= 90000")
tickets, tids = one("SELECT count(*), count(DISTINCT account_id) FROM "
                    "compat.tickets WHERE account_id >= 90000")
print(f"  placed under an old id: {orders:,} orders on {ids} ids,")
print(f"    {tickets:,} tickets on {tids}")
for aid, name, n, rev in con.execute("""
        SELECT account_id, account, count(DISTINCT order_id),
               sum(revenue) FROM compat.v_sales
        WHERE account_id IN (3574, 90056) GROUP BY ALL ORDER BY 1"""
                                     ).fetchall():
    print(f"  Book 2's v_sales  {aid:>6} {name:<15}{n:>5} orders "
          f"{rev:>12,.2f}")
n, rev = one("""SELECT count(DISTINCT order_id), sum(revenue)
                FROM marts.fct_order_lines JOIN marts.dim_account
                USING (account_key) WHERE account_id = 3574""")
print(f"  Groundwork's marts  3574 {'Bexham Estates':<15}{n:>5} orders "
      f"{rev:>12,.2f}")
rb._attach(con, BOOK2_WAREHOUSE, "book2")
*_, differ = rb.compare(con, "book2.orders", """(
    SELECT o.order_number AS order_id, a.account_id,
           strftime(o.order_date, '%Y-%m-%d') AS order_date, o.channel
    FROM marts.fct_orders AS o JOIN marts.dim_account AS a
    USING (account_key))""")
print(f"  orders with the marts' ids instead: {differ:,} rows differ")

print("\n2. The history's grain")
versions, status = one("""
    WITH v AS (SELECT (segment, owner) IS NOT DISTINCT FROM
                      lag((segment, owner)) OVER (
                          PARTITION BY account_id ORDER BY valid_from)
                      AS same
               FROM marts.dim_account WHERE account_id > 0)
    SELECT count(*), count(*) FILTER (same) FROM v""")
(rows,) = one("SELECT count(*) FROM compat.account_history")
print(f"  dim_account versions {versions:,}, of which status alone "
      f"{status:,}")
print(f"  compat.account_history rows {rows:,}")

print("\n3. Columns the marts left out, read from staging")
print("  products.launched_on: {} of {} set".format(*one(
    "SELECT count(launched_on), count(*) FROM compat.products")))
print("  tickets the marts give the unknown product: {:,};\n"
      "    with the desk's SKU in compat: {:,}".format(*one("""
    SELECT count(*), count(t.sku) FROM marts.fct_tickets AS f
    JOIN marts.dim_product AS p USING (product_key)
    JOIN compat.tickets AS t USING (ticket_id)
    WHERE p.sku = 'UNKNOWN'""")))

print("\n4. Kept by Groundwork, never in Book 2")
for what, sql in (
        ("cancelled orders", "SELECT count(*) FROM staging."
         "stg_tradeline__orders WHERE status = 'cancelled'"),
        ("ticket comments", "SELECT count(*) FROM staging."
         "stg_frontline__ticket_comments"),
        ("contract month ends", "SELECT count(*) FROM "
         "marts.fct_contract_month"),
        ("portal sessions", "SELECT count(*) FROM "
         "marts.fct_portal_sessions")):
    print(f"  {what:<22}{one(sql)[0]:>10,}")

print("\n5. Money: the same 1,229,062 lines, added up three ways")
(marts,) = one("SELECT sum(revenue) FROM marts.fct_order_lines")
(compat,) = one("""SELECT sum(revenue ORDER BY order_id, sku, revenue)
                   FROM compat.v_sales""")         # in a fixed order
b2 = sqlite3.connect(f"file:{BOOK2_WAREHOUSE}?mode=ro", uri=True)
(theirs,) = b2.execute("SELECT sum(revenue) FROM v_sales").fetchone()
b2.close()
con.close()
print(f"  {'the marts, DECIMAL':<34}{marts!s:>20}")
print(f"  {'compat.v_sales, DOUBLE, in DuckDB':<34}{compat!r:>20}")
print(f"  {"Book 2's v_sales, REAL, in SQLite":<34}{theirs!r:>20}")
