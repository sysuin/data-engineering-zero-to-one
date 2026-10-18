# Exercise 5: the region-by-month tile for a month, in billing units.
from _plans import connect

REFRESHES = 96 * 30            # every fifteen minutes for thirty days
GB_PER_CORE_H = 30             # assumed, as Chapter 25 and listing 10
MINIMUM_S, SUSPEND_S = 60, 300  # assumed, as listing 10
con = connect()
con.execute("""
    CREATE TEMP TABLE monthly AS
    SELECT region_key, date_trunc('month', order_date) AS order_date,
           sum(revenue) AS revenue
    FROM marts.fct_order_lines GROUP BY ALL""")


def size(table):
    """Bytes read for the three columns: 8 a number or date, text +2."""
    return con.execute(f"""SELECT sum(strlen(region_key) + 2 + 8 + 8)
                           FROM {table}""").fetchone()[0]


lines, rollup = size("marts.fct_order_lines"), size("monthly")
print(f"{'tile from':<14}{'bytes a refresh':>16}{'GB a month':>12}"
      f"{'billed core-h':>15}")
for label, per, build in (("the lines", lines, 0),
                          ("the rollup", rollup, lines * 30)):
    month = REFRESHES * per + build
    busy = per / 1e9 / GB_PER_CORE_H * 3600
    billed = REFRESHES * (max(busy, MINIMUM_S) + SUSPEND_S) / 3600
    print(f"{label:<14}{per:>16,}{month / 1e9:>12.2f}{billed:>15.1f}")
