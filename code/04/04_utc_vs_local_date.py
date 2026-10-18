# How many orders fall on a different day in UTC than at their depot.
import duckdb

from groundwork.config import LANDING, REFERENCE, SANDBOX, TRADELINE_DB
from meridian.sources import relay

db = SANDBOX / "ch04" / "listings.duckdb"
db.parent.mkdir(parents=True, exist_ok=True)
db.unlink(missing_ok=True)
con = duckdb.connect(str(db))
con.execute(f"ATTACH '{TRADELINE_DB}' AS tradeline "
            "(TYPE sqlite, READ_ONLY)")

# Each account's region, from last night's Relay export. Orders before
# March 2024 also use 208 account ids that only the old CRM's last
# export lists; Chapter 12 explains them. Here we need their region.
con.execute(f"""
    CREATE TABLE region_of AS SELECT account_id, region
    FROM read_csv('{LANDING}/relay/2026-01-04/accounts.csv')""")
old = relay.export("2024-02-29", SANDBOX / "ch04" / "relay")[0]
con.execute(f"""
    INSERT INTO region_of SELECT "Acct ID", "Region"
    FROM read_csv('{old}', delim=';', encoding='latin-1')
    WHERE "Acct ID" NOT IN (SELECT account_id FROM region_of)""")

# placed_at is UTC. The depot's date is the same moment on its clock.
con.execute(f"""
    CREATE TABLE orders_local AS
    SELECT o.id, o.order_number, o.account_id, r.region_id,
           r.name AS region, r.depot, r.timezone,
           CAST(o.placed_at AS TIMESTAMP) AS placed_at,
           CAST(o.placed_at AS DATE) AS utc_date,
           CAST(timezone(r.timezone, CAST(o.placed_at AS TIMESTAMP)
                AT TIME ZONE 'UTC') AS DATE) AS local_date
    FROM tradeline.orders o
    JOIN region_of a ON a.account_id = o.account_id
    JOIN read_csv('{REFERENCE}/regions.csv') r ON r.name = a.region
    WHERE o.status = 'placed'
""")
(placed,) = con.execute("SELECT COUNT(*) FROM tradeline.orders "
                        "WHERE status = 'placed'").fetchone()
n, moved, lo, hi = con.execute("""
    SELECT COUNT(*), COUNT(*) FILTER (WHERE utc_date <> local_date),
           MIN(hour(placed_at)) FILTER (WHERE utc_date <> local_date),
           MAX(hour(placed_at)) FILTER (WHERE utc_date <> local_date)
    FROM orders_local
""").fetchone()
print(f"placed orders in Tradeline         {placed:>9,}")
print(f"placed orders with a depot         {n:>9,}")
print(f"UTC date is not the depot's date   {moved:>9,}"
      f"  ({moved / n:.1%})")
print(f"  all placed between {lo:02d}:00 and {hi:02d}:59 UTC")

print(f"\n{'region':<11}{'depot':<12}{'orders':>9}{'moved':>9}"
      f"{'share':>8}")
for region, depot, orders, m in con.execute("""
    SELECT region, depot, COUNT(*),
           COUNT(*) FILTER (WHERE utc_date <> local_date)
    FROM orders_local GROUP BY region_id, region, depot
    ORDER BY region_id
""").fetchall():
    print(f"{region:<11}{depot:<12}{orders:>9,}{m:>9,}"
          f"{m / orders:>8.1%}")

# The year's edge: the last order of 2025, and 2025 counted both ways.
num, region, at, local = con.execute("""
    SELECT order_number, region, placed_at,
           timezone(timezone, placed_at AT TIME ZONE 'UTC')
    FROM orders_local ORDER BY placed_at DESC LIMIT 1
""").fetchone()
print(f"\norder {num} ({region}) placed {at:%Y-%m-%d %H:%M} UTC,")
print(f"which is {local:%Y-%m-%d %H:%M} at the depot")
print(f"\n{'2025, dated by':<18}{'orders':>9}{'revenue':>18}")
for label, col in (("the UTC date", "utc_date"),
                   ("the depot's date", "local_date")):
    orders, cents = con.execute(f"""
        SELECT COUNT(DISTINCT o.id), SUM(l.qty * l.unit_price_cents)
        FROM orders_local o
        JOIN tradeline.order_lines l ON l.order_id = o.id
        WHERE year({col}) = 2025
    """).fetchone()
    print(f"{label:<18}{orders:>9,}{cents / 100:>18,.2f}")
