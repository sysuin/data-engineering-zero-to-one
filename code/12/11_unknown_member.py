# Orders whose account the warehouse had not met when the order arrived:
# replayed night by night, and the unknown member that holds a place.
import json
from pathlib import Path

from _dims import SNAPSHOTS, built, tidy

con = built("unknown")
con.execute(f"ATTACH '{SNAPSHOTS}' AS s (READ_ONLY)")
# The run at 06:00 UTC on day R loads every order placed before it, and
# reads the newest export it has: the night before, or earlier if that
# night's never came.
con.execute("""
CREATE TEMP TABLE runs AS
SELECT o.order_id, o.account_id AS crm_id, o.placed_at,
       CAST(o.placed_at - INTERVAL 6 HOUR AS DATE) + 1 AS run_day
FROM staging.stg_tradeline__orders AS o""")
con.execute("""
CREATE TEMP TABLE exports AS
SELECT DISTINCT night FROM s.snapshots""")
late = con.execute("""
WITH first_seen AS (
    SELECT account_id AS crm_id, min(night) AS first_night
    FROM s.snapshots GROUP BY 1
), used AS (
    SELECT r.*, (SELECT max(night) FROM exports
                 WHERE night < r.run_day) AS export_night
    FROM runs AS r
)
SELECT u.order_id, u.crm_id, u.placed_at, u.export_night, f.first_night
FROM used AS u LEFT JOIN first_seen AS f USING (crm_id)
WHERE f.first_night IS NULL OR f.first_night > u.export_night
ORDER BY u.placed_at""").fetchall()
(n,) = con.execute("SELECT count(*) FROM runs").fetchone()
print(f"orders replayed through the nightly run {n:,}")
print(f"orders whose account the run's export lacked: {len(late)}")
print(f"  {'order':>7}{'account':>9}  {'placed (UTC)':<18}"
      f"{'export used':<13}{'account first in'}")
for oid, crm, at, used, first in late[:5]:
    print(f"  {oid:>7}{crm:>9}  {at:%Y-%m-%d %H:%M}  {used}   {first}")

# A rebuild with IT's map alone: the old ids it missed have no account.
con.execute("DELETE FROM intermediate.int_account_identity "
            "WHERE matched_by = 'rule'")
(lost,) = con.execute("""
    SELECT count(*) FROM staging.stg_tradeline__orders AS o
    LEFT JOIN intermediate.int_account_identity AS i
        ON i.crm_id = o.account_id
    WHERE o.status = 'placed' AND i.crm_id IS NULL""").fetchone()
print(f"\nwith IT's map alone, placed orders with no account: {lost:,}")
print("each would point at the unknown member:")
row = con.execute("SELECT account_key, account_id, account_name, "
                  "segment, status, valid_from, valid_to FROM "
                  "marts.dim_account WHERE account_id = -1").fetchone()
print("  " + " | ".join(str(x) for x in row[:5]))
print(f"  valid_from {row[5]}, valid_to {row[6]}")
Path(__file__).with_suffix(".json").write_text(json.dumps({
    "orders": n, "map_alone": lost,
    "late": [{"order": o, "account": a, "placed": f"{p:%Y-%m-%d %H:%M}",
              "used": str(u), "first": str(f)}
             for o, a, p, u, f in late]}, indent=1))
tidy("unknown")
