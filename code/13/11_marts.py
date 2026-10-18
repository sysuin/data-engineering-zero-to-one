# The Build in a sandbox: every fact held to marts.sql, each count set
# beside a second source, and revenue tied to the ledger's journal.
# timeout: 600
import sqlite3
from datetime import date

from groundwork import dimensions, facts
from groundwork.config import BOOK2_WAREHOUSE

from _facts import built

con = built()
con.execute(f"ATTACH '{BOOK2_WAREHOUSE}' AS b2 (TYPE sqlite, "
            "READ_ONLY)")
one = lambda sql: con.execute(sql).fetchone()[0]          # noqa: E731
b2 = sqlite3.connect(f"file:{BOOK2_WAREHOUSE}?mode=ro", uri=True)

# Contract-month rows counted independently, in Python, from Book 2.
ends = [date(y, m, 1) for y in range(2022, 2026) for m in range(1, 13)]
ends = [date(d.year + d.month // 12, d.month % 12 + 1, 1)
        .toordinal() - 1 for d in ends]
months = sum(sum(date.fromisoformat(s).toordinal() <= e
                 <= date.fromisoformat(f).toordinal() for e in ends)
             for s, f in b2.execute("SELECT start_date, end_date "
                                    "FROM contracts"))
pulled = one("""SELECT source_count FROM audit.load_log
                WHERE source = 'frontline' AND table_name = 'tickets'
                ORDER BY load_id DESC LIMIT 1""")
checks = [
    ("fct_order_lines", "Book 2's order_lines",
     one("SELECT count(*) FROM b2.order_lines")),
    ("fct_orders", "Book 2's orders",
     one("SELECT count(*) FROM b2.orders")),
    ("fct_tickets", "the API's total", pulled),
    ("fct_contracts", "Book 2's contracts",
     one("SELECT count(*) FROM b2.contracts")),
    ("fct_contract_month", "counted from Book 2", months),
]
print(f"{'built':<20}{'rows':>10}  {'second source':<21}{'rows':>10}")
for table, other, m in checks:
    n = one(f"SELECT count(*) FROM marts.{table}")
    print(f"{table:<20}{n:>10,}  {other:<21}{m:>10,} "
          f"{'=' if n == m else '!'}")

(differ,) = con.execute("""
    SELECT count(*) FROM (
        (SELECT order_number, line_no, p.sku, qty, unit_price,
                f.unit_cost, discount_pct
         FROM marts.fct_order_lines AS f
         JOIN marts.dim_product AS p USING (product_key)
         EXCEPT
         SELECT order_id, line_no, sku, qty,
                CAST(round(unit_price, 2) AS DECIMAL(18, 2)),
                CAST(round(unit_cost, 2) AS DECIMAL(18, 2)),
                discount_pct
         FROM b2.order_lines)
        UNION ALL
        (SELECT order_id, line_no, sku, qty,
                CAST(round(unit_price, 2) AS DECIMAL(18, 2)),
                CAST(round(unit_cost, 2) AS DECIMAL(18, 2)),
                discount_pct
         FROM b2.order_lines
         EXCEPT
         SELECT order_number, line_no, p.sku, qty, unit_price,
                f.unit_cost, discount_pct
         FROM marts.fct_order_lines AS f
         JOIN marts.dim_product AS p USING (product_key)))
    """).fetchone()
print(f"order lines in one and not the other, every column: {differ}")

# Revenue by month and region against the ledger's sales journal.
cells = con.execute("""
    WITH ours AS (
        SELECT date_trunc('month', f.order_date) AS m, r.region_code,
               sum(f.revenue) * 100 AS cents
        FROM marts.fct_order_lines AS f
        JOIN marts.dim_region AS r USING (region_key) GROUP BY ALL),
    ledger AS (
        SELECT period_month AS m, region_code,
               sum(amount_cents) AS cents
        FROM staging.stg_ledger__gl_revenue
        WHERE description = 'Sales journal' GROUP BY ALL)
    SELECT l.m, l.region_code, o.cents, l.cents
    FROM ledger AS l LEFT JOIN ours AS o USING (m, region_code)
    ORDER BY 1, 2""").fetchall()
tied = sum(o == g for _, _, o, g in cells)
print(f"\nsales journal: {len(cells)} months x regions, "
      f"{tied} equal to the cent")
print(f"{'October 2025':<14}{'fct_order_lines':>17}{'ledger':>17}")
for m, code, ours, ledger in cells:
    if m == date(2025, 10, 1):
        print(f"{code:<14}{ours / 100:>17,.2f}{ledger / 100:>17,.2f}")
problems = dimensions.check(con) + facts.check(con)
print("checks:", "every promise kept" if not problems else problems)
