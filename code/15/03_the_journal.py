# The ledger's lines that no source sends: what they are, a search for
# a source row behind them, and finance's register of them.
from groundwork import reconcile

from _sandbox import base, connect

con = connect(base())
d = reconcile.dollars
manual = con.execute("""
    SELECT period_month, region_code, amount_cents, description
    FROM staging.stg_ledger__gl_revenue
    WHERE description <> 'Sales journal' ORDER BY 1""").fetchall()
(lines,) = con.execute("SELECT count(*) FROM "
                       "staging.stg_ledger__gl_revenue").fetchone()
print(f"ledger lines: {lines}; not the sales journal: {len(manual)},")
print("each described as 'Manual adjustment: ...'")
for period, region, cents, text in manual:
    print(f"  {period:%Y-%m} {region} {d(cents):>11,}  "
          f"{text.removeprefix('Manual adjustment: ')}")

# Is there anything in Tradeline it could be? Credits, or an order,
# a day or an account's month in that region worth that much.
print("\nTradeline rows that could carry them:")
print("  lines with a negative quantity or price:", con.execute("""
    SELECT count(*) FROM staging.stg_tradeline__order_lines
    WHERE qty < 0 OR unit_price_cents < 0""").fetchone()[0])
for period, region, cents, _ in manual:
    hits = con.execute("""
        WITH f AS (
            SELECT f.*, a.account_id FROM marts.fct_order_lines AS f
            JOIN marts.dim_region AS r USING (region_key)
            JOIN marts.dim_account AS a USING (account_key)
            WHERE r.region_code = $region
              AND date_trunc('month', f.order_date) = $period)
        SELECT (SELECT count(*) FROM (
                    SELECT order_id FROM f GROUP BY 1
                    HAVING sum(revenue) * 100 = abs($cents))),
               (SELECT count(*) FROM (
                    SELECT order_date FROM f GROUP BY 1
                    HAVING sum(revenue) * 100 = abs($cents))),
               (SELECT count(*) FROM (
                    SELECT account_id FROM f GROUP BY 1
                    HAVING sum(revenue) * 100 = abs($cents)))""",
        {"region": region, "period": period, "cents": cents}).fetchone()
    print(f"  {period:%Y-%m} {region}: orders, days, accounts worth "
          f"{d(abs(cents)):,}: {hits[0]}, {hits[1]}, {hits[2]}")

cells = {(c.period, c.region): c for c in reconcile.revenue(con)}
print(f"\n{'November 2025':<14}{'warehouse':>15}{'ledger':>15}"
      f"{'difference':>13}")
for (period, region), c in sorted(cells.items()):
    if f"{period:%Y-%m}" == "2025-11":
        print(f"{region:<14}{d(c.warehouse):>15,}{d(c.ledger):>15,}"
              f"{d(c.ledger - c.warehouse):>13,}")

register = reconcile.journals()
print(f"\nfinance's register, {reconcile.JOURNALS.name}: "
      f"{len(register)} journals")
for j in register:
    print(f"  {j.period:%Y-%m} {j.region} {d(j.cents):>11,}  "
          f"confirmed by {j.confirmed_by}")
for name, reg in (("with the register", register),
                  ("without its November line", register[:1])):
    cs = reconcile.revenue(con, reg)
    print(f"{name}: {sum(c.closes for c in cs)} of {len(cs)} cells "
          f"reach the ledger")
    print("  posted but not documented: " + (", ".join(
        f"{c.period:%Y-%m} {c.region}" for c in cs
        if c.manual and not c.documented_ok) or "none"))
