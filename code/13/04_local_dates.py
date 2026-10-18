# The order's day is its depot's day: how often that differs from the
# UTC date, by region, and whether it matches Book 2's order dates.
# timeout: 600
from groundwork.config import BOOK2_WAREHOUSE

from _facts import built

con = built()
con.execute(f"ATTACH '{BOOK2_WAREHOUSE}' AS b2 (TYPE sqlite, "
            "READ_ONLY)")
print(f"{'region':<11}{'timezone':<21}{'orders':>9}{'UTC differs':>13}")
rows = con.execute("""
    SELECT r.region_name, r.timezone, count(*),
           count(*) FILTER (o.order_date <> CAST(o.placed_at AS DATE))
    FROM marts.fct_orders AS o JOIN marts.dim_region AS r
    USING (region_key) GROUP BY ALL
    ORDER BY r.region_name""").fetchall()
for name, tz, n, moved in rows:
    pct = 100 * moved / n
    print(f"{name:<11}{tz:<21}{n:>9,}{moved:>8,} {pct:>3.0f}%")
n, moved = (sum(r[i] for r in rows) for i in (2, 3))
print(f"{'all':<32}{n:>9,}{moved:>8,} {100 * moved / n:>3.0f}%")

months, days = con.execute("""
    SELECT count(*) FILTER (date_trunc('month', order_date)
                            <> date_trunc('month', placed_at)),
           count(*) FILTER (order_date < CAST(placed_at AS DATE))
    FROM marts.fct_orders""").fetchone()
print(f"orders the UTC date puts in the next month {months:,}")
print(f"orders whose depot day is before the UTC day {days:,}")

(differ,) = con.execute("""
    SELECT count(*) FROM marts.fct_orders AS f
    JOIN b2.orders AS b ON b.order_id = f.order_number
    WHERE strftime(f.order_date, '%Y-%m-%d') <> b.order_date
    """).fetchone()
(b2,) = con.execute("SELECT count(*) FROM b2.orders").fetchone()
print(f"Book 2's {b2:,} orders: our date differs on {differ}")
