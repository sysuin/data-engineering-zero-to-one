# Tradeline's status codes, its channels, and the clock placed_at is in.
from meridian.sources import tradeline

con = tradeline.connect()

print("status      orders    share")
for status, n, share in con.execute("""
        SELECT status, COUNT(*),
               100.0 * COUNT(*) / SUM(COUNT(*)) OVER ()
        FROM orders GROUP BY status ORDER BY status"""):
    print(f"{status:<10}{n:>9,}{share:>8.2f}%")

(stored, first, last, in_2026) = con.execute("""
    SELECT typeof(placed_at), MIN(placed_at), MAX(placed_at),
           SUM(placed_at >= '2026-01-01') FROM orders""").fetchone()
print(f"\nplaced_at is stored as {stored}")
print(f"first {first}   last {last}")
print(f"orders stamped 2026 or later: {in_2026}")

# Orders by the hour written in placed_at, whatever clock that is.
hours = dict(con.execute("""
    SELECT CAST(substr(placed_at, 12, 2) AS INTEGER), COUNT(*)
    FROM orders GROUP BY 1""").fetchall())
print("\norders by the hour in placed_at")
for row in range(4):
    print("  ".join(f"{h:02d} {hours.get(h, 0):>6,}"
                    for h in range(row * 6, row * 6 + 6)))
