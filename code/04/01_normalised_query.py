# 2025's revenue by region, asked of the systems themselves.
import duckdb

from groundwork.config import LANDING, REFERENCE, TRADELINE_DB

con = duckdb.connect()
con.execute(f"ATTACH '{TRADELINE_DB}' AS tradeline "
            "(TYPE sqlite, READ_ONLY)")
accounts = LANDING / "relay" / "2026-01-04" / "accounts.csv"
regions = REFERENCE / "regions.csv"

# The four tables the question needs, and the key each is stored by.
tables = [("tradeline.orders", "tradeline.orders", "id"),
          ("tradeline.order_lines", "tradeline.order_lines",
           "order_id, line_no"),
          ("relay accounts.csv", f"read_csv('{accounts}')",
           "account_id"),
          ("regions.csv", f"read_csv('{regions}')", "region_id")]
for name, source, key in tables:
    (n,) = con.execute(f"SELECT COUNT(*) FROM {source}").fetchone()
    print(f"{name:<22} {n:>10,} rows   key: {key}")

# Three joins, a status filter, a date range and cents to dollars.
rows = con.execute(f"""
    SELECT r.name,
           COUNT(DISTINCT o.id)             AS orders,
           SUM(l.qty * l.unit_price_cents)  AS cents
    FROM tradeline.orders o
    JOIN tradeline.order_lines l ON l.order_id = o.id
    JOIN read_csv('{accounts}') a ON a.account_id = o.account_id
    JOIN read_csv('{regions}') r ON r.name = a.region
    WHERE o.status = 'placed'
      AND o.placed_at >= '2025-01-01'
      AND o.placed_at <  '2026-01-01'
    GROUP BY r.name
    ORDER BY cents DESC
""").fetchall()
print(f"\n{'region':<12}{'orders':>10}{'revenue':>20}")
for region, orders, cents in rows:
    print(f"{region:<12}{orders:>10,}{cents / 100:>20,.2f}")
total = sum(cents for *_, cents in rows)
print(f"{'total':<12}{sum(r[1] for r in rows):>10,}"
      f"{total / 100:>20,.2f}")

# The same lines without the joins to Relay: did the joins lose any?
(check,) = con.execute("""
    SELECT SUM(l.qty * l.unit_price_cents)
    FROM tradeline.orders o
    JOIN tradeline.order_lines l ON l.order_id = o.id
    WHERE o.status = 'placed'
      AND o.placed_at >= '2025-01-01'
      AND o.placed_at <  '2026-01-01'
""").fetchone()
print(f"{'lines alone':<22}{check / 100:>20,.2f}")
