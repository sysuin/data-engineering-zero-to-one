# A join to a finer grain multiplies what sits on the coarser side.
import duckdb

from groundwork.config import LANDING, TRADELINE_DB

con = duckdb.connect()
con.execute(f"ATTACH '{TRADELINE_DB}' AS tradeline "
            "(TYPE sqlite, READ_ONLY)")
relay = LANDING / "relay" / "2026-01-04"
con.execute(f"CREATE VIEW contracts AS "
            f"SELECT * FROM read_csv('{relay}/contracts.csv')")
con.execute("""
    CREATE VIEW orders_2025 AS
    SELECT * FROM tradeline.orders
    WHERE status = 'placed'
      AND placed_at >= '2025-01-01' AND placed_at < '2026-01-01'
""")

# 1. Count orders after joining them to their lines.
(orders,) = con.execute("SELECT COUNT(*) FROM orders_2025").fetchone()
(joined,) = con.execute("""
    SELECT COUNT(*) FROM orders_2025 o
    JOIN tradeline.order_lines l ON l.order_id = o.id
""").fetchone()
print(f"orders in 2025                    {orders:>12,}")
print(f"COUNT(*) after joining lines      {joined:>12,}")

# 2. Revenue, then revenue by the discount on the account's contract.
con.execute("""
    CREATE TABLE lines AS
    SELECT o.account_id, CAST(o.placed_at AS DATE) AS day,
           l.qty * l.unit_price_cents AS cents
    FROM orders_2025 o
    JOIN tradeline.order_lines l ON l.order_id = o.id
""")
(right,) = con.execute("SELECT SUM(cents) FROM lines").fetchone()
fanned = con.execute("""
    SELECT COUNT(*), SUM(l.cents) FROM lines l
    JOIN contracts c ON c.account_id = l.account_id
""").fetchone()
in_force = con.execute("""
    SELECT COUNT(*), SUM(l.cents) FROM lines l
    JOIN contracts c ON c.account_id = l.account_id
     AND l.day BETWEEN c.start_date AND c.end_date
""").fetchone()
(n,) = con.execute("SELECT COUNT(*) FROM lines").fetchone()
print(f"\n{'revenue, 2025':<30}{'rows':>10}{'dollars':>18}")
print(f"{'lines alone':<30}{n:>10,}{right / 100:>18,.2f}")
print(f"{'join on account_id':<30}{fanned[0]:>10,}"
      f"{fanned[1] / 100:>18,.2f}")
print(f"{'join to contract in force':<30}{in_force[0]:>10,}"
      f"{in_force[1] / 100:>18,.2f}")
print(f"inflated {fanned[1] / right:.2f} times by the first join")

# One account shows the mechanism: every line meets every contract.
acct, cents, contracts = con.execute("""
    SELECT l.account_id, SUM(l.cents) // COUNT(DISTINCT c.contract_id),
           COUNT(DISTINCT c.contract_id)
    FROM lines l JOIN contracts c ON c.account_id = l.account_id
    GROUP BY l.account_id
    ORDER BY COUNT(DISTINCT c.contract_id) DESC, l.account_id
    LIMIT 1
""").fetchone()
print(f"\naccount {acct}: {contracts} contracts on record; its "
      f"${cents / 100:,.2f}")
print(f"is counted {contracts} times, as "
      f"${cents * contracts / 100:,.2f}")
