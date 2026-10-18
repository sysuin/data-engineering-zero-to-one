# Exercise 3: reconcile the lines' count and value by UTC day of
# placed_at, for the loader with no lookback, after thirty nights.
from _watermark import Loader, compare, nights

loader = Loader("updated_at", ">")
for night, moment, src in nights():
    loader.run(night, src)
wrong, kinds = compare(loader)            # attaches Tradeline as src
BY_DAY = """SELECT substr(o.placed_at, 1, 10) AS day, count(*) AS n,
                   sum(l.qty * l.unit_price_cents) AS cents
            FROM {o} o JOIN {l} l ON l.order_id = o.id GROUP BY day"""
rows = loader.db.execute(f"""
    SELECT day, c.n - s.n, c.cents - s.cents
    FROM ({BY_DAY.format(o='copy_orders', l='copy_order_lines')}) c
    FULL JOIN ({BY_DAY.format(o='src.orders', l='src.order_lines')}) s
    USING (day)
    WHERE c.n IS DISTINCT FROM s.n OR c.cents IS DISTINCT FROM s.cents
    ORDER BY day""").fetchall()
print(f"days that disagree: {len(rows)} (orders wrong: {len(wrong)})")
print(f"{'placed (UTC)':<13} {'lines':>6} {'value':>12}")
for day, n, cents in rows[:8]:
    print(f"{day:<13} {n:>+6} {cents / 100:>+12,.2f}")
if len(rows) > 8:
    print(f"... and {len(rows) - 8} more")
