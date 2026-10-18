# Chapter 1's two numbers for November 2025, bridged one measured
# cause at a time; then the same causes taken in the other order.
from datetime import date

from groundwork import reconcile
from groundwork.config import TRADELINE_DB

from _sandbox import base, connect, save

NOV = date(2025, 11, 1)
con = connect(base())
# The dashboard's own query, on Tradeline, as Chapter 1 ran it.
start, exact, orders = reconcile.dashboard(con, NOV,
                                           tradeline=TRADELINE_DB)
b = reconcile.bridge(con, NOV, start=start, exact=exact, orders=orders)

money = reconcile.money
print(f"{'November 2025':<28}{'orders':>7}{'step':>15}{'running':>17}")
print(f"{'the dashboard (Chapter 1)':<28}{orders:>7,}{b.start!s:>32}")
running, rows = b.start, []
for s in b.steps:
    running += s.amount
    n = f"{s.orders:,}" if s.orders is not None else ""
    step = ("+" if s.amount > 0 else "") + money(s.amount)
    print(f"{s.name:<28}{n:>7}{step:>15}{money(running):>17}")
    rows.append([s.name, str(s.amount), s.orders, str(running), s.note])
print(f"{'the ledger':<28}{money(b.ledger):>39}")
print(f"{'remaining':<28}{money(b.remaining):>39}")

ours = reconcile.dollars(con.execute("""
    SELECT CAST(sum(revenue) * 100 AS BIGINT)
    FROM marts.fct_order_lines
    WHERE order_date >= '2025-11-01' AND order_date < '2025-12-01'
    """).fetchone()[0])
print(f"\nfct_order_lines, November by the depot's day: {ours:,}")

# The last order of each calendar step: UTC, and the depot's clock.
examples = con.execute("""
    SELECT DISTINCT ON (step) step, order_number, r.region_name,
           placed_at, (placed_at AT TIME ZONE 'UTC')
                      AT TIME ZONE r.timezone AS local
    FROM (SELECT *, if(order_date < '2025-11-01', 'out', 'in') AS step
          FROM marts.fct_order_lines
          WHERE placed_at >= '2025-11-01' AND order_date < '2025-11-01'
             OR placed_at >= '2025-12-01' AND order_date < '2025-12-01'
                AND order_date >= '2025-11-01') AS f
    JOIN marts.dim_region AS r USING (region_key)
    ORDER BY step, placed_at DESC""").fetchall()
for step, number, region, utc, local in examples:
    print(f"  order {number} ({region}): {utc:%d %b %H:%M} UTC, "
          f"{local:%d %b %H:%M} there")

# The other way round: calendar first, over every order, then the
# cancelled by the depot's month. The steps change; the end does not.
other = con.execute("""
    WITH o AS (
        SELECT o.status, o.placed_at,
               CAST((o.placed_at AT TIME ZONE 'UTC')
                    AT TIME ZONE r.timezone AS DATE) AS day,
               l.qty * l.unit_price_cents AS cents
        FROM staging.stg_tradeline__orders AS o
        JOIN staging.stg_tradeline__order_lines AS l USING (order_id)
        LEFT JOIN intermediate.int_account_identity AS i
            ON i.crm_id = o.account_id
        JOIN marts.dim_account AS a
            ON a.account_id = coalesce(i.account_id, -1)
           AND a.is_current
        JOIN marts.dim_region AS r USING (region_key)
        WHERE o.placed_at >= '2025-10-31'
          AND o.placed_at < '2025-12-02')
    SELECT
      sum(cents) FILTER (WHERE placed_at >= '2025-11-01'
                         AND placed_at < '2025-12-01'
                         AND day < '2025-11-01'),
      sum(cents) FILTER (WHERE placed_at >= '2025-12-01'
                         AND day < '2025-12-01'),
      sum(cents) FILTER (WHERE status = 'cancelled'
                         AND day >= '2025-11-01' AND day < '2025-12-01')
    FROM o""").fetchone()
d = reconcile.dollars
swapped = [-d(other[0]), d(other[1]), -d(other[2])]
print("\ncalendar first:  " + "  ".join(f"{x:+,}" for x in swapped))
same = sum(swapped) == sum(s.amount for s in b.steps[1:4])
print(f"same total: {same}")

save("02_revenue_bridge", {
    "start": str(b.start), "ledger": str(b.ledger), "orders": orders,
    "steps": rows, "warehouse": str(ours),
    "swapped": [str(x) for x in swapped],
    "examples": [[s, n, r, f"{u:%Y-%m-%d %H:%M}",
                  f"{lo:%Y-%m-%d %H:%M}"]
                 for s, n, r, u, lo in examples]})
