# The grain decides what a fact row may carry: the five grains as the
# warehouse records them, and an order-level measure put on lines.
# timeout: 600
import json
from pathlib import Path

from _facts import built

con = built()
print(f"{'table':<20}one row per ...")
for table, grain in con.execute(
        "SELECT table_name, comment FROM duckdb_tables() WHERE "
        "schema_name = 'marts' AND table_name LIKE 'fct_%' AND "
        "table_name <> 'fct_portal_sessions' ORDER BY 1").fetchall():
    print(f"{table:<20}{grain.removeprefix('One row per ')}")

lines, orders, per = con.execute("""
    SELECT count(*), count(DISTINCT order_id),
           count(*) / count(DISTINCT order_id)
    FROM marts.fct_order_lines""").fetchone()
print(f"\n{lines:,} lines in {orders:,} orders, "
      f"{per:.2f} lines an order")

# A wide table at line grain that also carries each order's total.
right, wrong = con.execute("""
    WITH wide AS (
        SELECT l.*, o.revenue AS order_revenue
        FROM marts.fct_order_lines AS l
        JOIN marts.fct_orders AS o USING (order_id)
        WHERE l.order_date BETWEEN '2025-01-01' AND '2025-12-31')
    SELECT sum(revenue), sum(order_revenue) FROM wide""").fetchone()
print(f"2025 revenue, summed at line grain   {right:>16,.2f}")
print(f"order totals carried on each line    {wrong:>16,.2f}")
print(f"overstated {wrong / right:.2f}x")

# Three kinds of fact row for one account, Walstone Schools, in the
# last quarter of 2025 (Figure 13.1).
WHERE = """JOIN marts.dim_account AS a USING (account_key)
           WHERE a.account_id = 2429"""
orders = con.execute(f"""
    SELECT strftime(order_date, '%Y-%m-%d'), revenue
    FROM marts.fct_orders {WHERE} AND order_date >= '2025-10-01'
    ORDER BY 1""").fetchall()
months = con.execute(f"""SELECT strftime(month_end, '%Y-%m-%d'),
    contract_id, months_in_force, months_remaining
    FROM marts.fct_contract_month {WHERE}
    AND month_end >= '2025-10-01' ORDER BY 1""").fetchall()
tickets = con.execute(f"""SELECT ticket_id,
    strftime(opened_at, '%Y-%m-%d %H:%M')
    FROM marts.fct_tickets {WHERE} AND opened_at >= '2025-10-01'
    ORDER BY 1""").fetchall()
print("\naccount 2429, October to December 2025:")
print(f"  fct_orders {len(orders)} rows, fct_contract_month "
      f"{len(months)}, fct_tickets {len(tickets)}")
Path(__file__).with_suffix(".json").write_text(json.dumps(
    {"orders": [[d, float(r)] for d, r in orders], "months": months,
     "tickets": tickets}, indent=1))
