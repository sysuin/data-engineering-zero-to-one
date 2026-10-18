# Every order line's revenue summed three ways: DOUBLE, cents, DECIMAL.
import math

import duckdb

from groundwork.config import THREADS, TRADELINE_DB

print(f"0.1 + 0.2 = {0.1 + 0.2!r}")

# One thread, so the floating-point sum is added in one fixed order.
con = duckdb.connect(config={"threads": THREADS})
con.execute(f"ATTACH '{TRADELINE_DB}' AS t (TYPE sqlite, READ_ONLY)")
dollars, cents, decimal = con.execute("""SELECT
    sum(qty * unit_price_cents / 100.0),      -- DOUBLE
    sum(qty * unit_price_cents),              -- BIGINT, in cents
    sum(qty * unit_price_cents * 0.01)        -- DECIMAL
    FROM t.order_lines""").fetchone()
print(f"\nDOUBLE    {dollars!r}")
print(f"cents     {cents:,}")
print(f"DECIMAL   {decimal}")
print(f"DOUBLE equals the cents? {dollars == cents / 100}")

# A DECIMAL divided is a DOUBLE again.
for expr in ("qty * unit_price_cents / 100.0",
             "(qty * unit_price_cents)::DECIMAL(18,2) / 100",
             "qty * unit_price_cents * 0.01"):
    kind = con.execute(f"SELECT typeof({expr}) FROM t.order_lines "
                       "LIMIT 1").fetchone()[0]
    print(f"  {expr:<45} {kind}")


def add_up(values):
    """Running addition, one value at a time, as a database does."""
    total = 0.0
    for v in values:
        total += v
    return total


# The same doubles added in three orders give three totals.
rows = con.execute("""SELECT qty * unit_price_cents / 100.0
    FROM t.order_lines ORDER BY order_id, line_no""").fetchall()
amounts = [a for (a,) in rows]
print(f"\nin order_id order   {add_up(amounts)!r}")
print(f"largest first       {add_up(sorted(amounts, reverse=True))!r}")
print(f"smallest first      {add_up(sorted(amounts))!r}")
print(f"math.fsum           {math.fsum(amounts)!r}")
