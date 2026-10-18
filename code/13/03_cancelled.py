# What did not happen stays out: cancelled orders by rule, and test
# orders because the source deleted them and raw keeps up.
# timeout: 600
from meridian.sources import tradeline

from _facts import built

con = built()
print(f"{'status':<10}{'orders':>9}{'lines':>11}{'revenue':>18}")
for status, n, lines, cents in con.execute("""
        SELECT o.status, count(DISTINCT o.order_id), count(*),
               sum(l.qty * l.unit_price_cents)
        FROM staging.stg_tradeline__orders AS o
        JOIN staging.stg_tradeline__order_lines AS l USING (order_id)
        GROUP BY 1 ORDER BY 1 DESC""").fetchall():
    print(f"{status:<10}{n:>9,}{lines:>11,}{cents / 100:>18,.2f}")

n, cents = con.execute("""
    SELECT count(DISTINCT order_id), sum(revenue) * 100
    FROM marts.fct_order_lines""").fetchone()
print(f"{'the fact':<10}{n:>9,}{'':>11}{cents / 100:>18,.2f}")
(cancelled,) = con.execute("""
    SELECT count(*) FROM marts.fct_orders AS f
    JOIN staging.stg_tradeline__orders AS o USING (order_id)
    WHERE o.status <> 'placed'""").fetchone()
print(f"cancelled orders in the fact: {cancelled}")

# Test orders: staff placed them on Ardenwood Retail, account 2, and
# deleted them the next day. Raw's current view follows the deletes.
src = tradeline.connect()
today = {i for (i,) in src.execute("SELECT id FROM orders")}
raw = {i for (i,) in con.execute(
    "SELECT id FROM raw.tradeline_orders_current").fetchall()}
print(f"\nTradeline today {len(today):,} orders, raw current "
      f"{len(raw):,}")
print(f"  only in raw {len(raw - today)}, "
      f"only in Tradeline {len(today - raw)}")
gaps = src.execute("""
    SELECT count(*) FROM orders AS o
    WHERE NOT EXISTS (SELECT 1 FROM orders WHERE id = o.id - 1)
      AND o.id > 1""").fetchone()[0]
(top,) = src.execute("SELECT max(id) FROM orders").fetchone()
print(f"ids run to {top:,}: {top - len(today)} ids missing, "
      f"in {gaps} gaps")
