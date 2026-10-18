# Which of 2025's measures add up across months, and which do not.
import json

import duckdb

from groundwork.config import LANDING, SANDBOX, TRADELINE_DB

con = duckdb.connect(str(SANDBOX / "ch04" / "listings.duckdb"),
                     read_only=True)
con.execute(f"ATTACH '{TRADELINE_DB}' AS tradeline "
            "(TYPE sqlite, READ_ONLY)")
contracts = LANDING / "relay" / "2026-01-04" / "contracts.csv"

# Four measures for each month of 2025, orders dated by the depot.
months = con.execute(f"""
    WITH m AS (
        SELECT date AS month_end, month FROM marts.dim_date
        WHERE year = 2025 AND is_month_end),
    o AS (
        SELECT month(o.local_date) AS month, o.id, o.account_id,
               SUM(l.qty * l.unit_price_cents) AS cents
        FROM orders_local o
        JOIN tradeline.order_lines l ON l.order_id = o.id
        WHERE year(o.local_date) = 2025
        GROUP BY ALL)
    SELECT m.month,
           (SELECT SUM(cents) FROM o WHERE o.month = m.month),
           (SELECT COUNT(DISTINCT account_id) FROM o
            WHERE o.month = m.month),
           (SELECT COUNT(*) FROM o WHERE o.month = m.month),
           (SELECT COUNT(*) FROM read_csv('{contracts}') c
            WHERE m.month_end BETWEEN c.start_date AND c.end_date)
    FROM m ORDER BY m.month
""").fetchall()

# The same measures computed once over the whole year.
cents, accounts, orders = con.execute("""
    SELECT SUM(l.qty * l.unit_price_cents),
           COUNT(DISTINCT o.account_id), COUNT(DISTINCT o.id)
    FROM orders_local o
    JOIN tradeline.order_lines l ON l.order_id = o.id
    WHERE year(o.local_date) = 2025
""").fetchone()
(contracts_2025,) = con.execute(f"""
    SELECT COUNT(*) FROM read_csv('{contracts}')
    WHERE start_date <= '2025-12-31' AND end_date >= '2025-01-31'
""").fetchone()

print(f"{'month':<7}{'revenue':>16}{'accounts':>10}{'avg order':>11}"
      f"{'in force':>10}")
for m, c, a, o, f in months:
    print(f"2025-{m:02d}{c / 100:>16,.2f}{a:>10,}{c / o / 100:>11,.2f}"
          f"{f:>10,}")
add = [sum(r[i] for r in months) for i in (1, 2, 4)]
avg = sum(r[1] / r[3] for r in months) / len(months) / 100
print(f"{'summed':<7}{add[0] / 100:>16,.2f}{add[1]:>10,}{'':>11}"
      f"{add[2]:>10,}")
print(f"{'mean':<7}{'':>16}{'':>10}{avg:>11,.2f}{add[2] / 12:>10,.0f}")
print(f"{'year':<7}{cents / 100:>16,.2f}{accounts:>10,}"
      f"{cents / orders / 100:>11,.2f}{contracts_2025:>10,}")

with open("code/04/06_additivity.json", "w") as f:
    json.dump({"months": [{"month": m, "cents": c, "accounts": a,
                           "orders": o, "in_force": x}
                          for m, c, a, o, x in months],
               "year": {"cents": cents, "accounts": accounts,
                        "orders": orders,
                        "contracts": contracts_2025}}, f, indent=1)
