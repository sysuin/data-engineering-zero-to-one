# Snapshot history against the audit's, the eight nights with no export,
# and what one missing night on a busy day would have done.
import json
from pathlib import Path

from _dims import SNAPSHOTS, built, tidy

con = built("missing")
con.execute(f"ATTACH '{SNAPSHOTS}' AS s (READ_ONLY)")
con.execute("CREATE TEMP TABLE audit_v AS SELECT account_id, "
            "valid_from, segment, owner, status FROM marts.dim_account "
            "WHERE account_id > 0")
key = "account_id, valid_from, segment, owner, status"
(same,) = con.execute(f"SELECT count(*) FROM audit_v JOIN s.versions "
                      f"USING ({key})").fetchone()
(n,) = con.execute("SELECT count(*) FROM audit_v").fetchone()
print(f"versions from the audit {n:,}; the same in the snapshots "
      f"{same:,}")

print("\nnight with no export   account changes   contract changes")
for night, acc, con_ in con.execute("""
    SELECT m.night,
           (SELECT count(*) FROM intermediate.int_account_changes c
            WHERE c.change_date = m.night
              AND c.field_name IN ('segment', 'owner', 'status')),
           (SELECT count(*) FROM staging.stg_relay__field_history h
            WHERE h.record_type = 'contract' AND CAST(timezone(
                'America/New_York', timezone('UTC', h.changed_at))
                AS DATE) = m.night)
    FROM (SELECT night FROM generate_series(DATE '2022-01-01',
                 DATE '2026-01-04', INTERVAL 1 DAY) AS g(night)
          WHERE CAST(night AS DATE) NOT IN
                (SELECT night FROM s.snapshots)) AS m
    ORDER BY 1""").fetchall():
    print(f"  {night:%Y-%m-%d %a}{acc:>16}{con_:>19}")

# Had the export failed on a busy night instead: 1 October 2025.
gone = "2025-10-01"
sql = Path(__file__).with_name("scd2_snapshots.sql").read_text()
sql = sql.replace("FROM snapshots", "FROM s.snapshots WHERE night <> "
                  f"DATE '{gone}' AND account_id < 90000")
con.execute(f"CREATE TEMP TABLE without AS {sql}")
moved = con.execute("""
    SELECT a.account_id, a.valid_from, w.valid_from
    FROM audit_v AS a JOIN without AS w
        USING (account_id, segment, owner, status)
    WHERE w.valid_from <> a.valid_from
      AND w.valid_from - a.valid_from BETWEEN 1 AND 3
    ORDER BY 1""").fetchall()
late = sorted({(w - a).days for _, a, w in moved})
print(f"\nwithout {gone}'s export: {len(moved)} versions begin "
      + " or ".join(f"{d} day{'s' * (d > 1)}" for d in late) + " late")
(orders,) = con.execute(f"""
    SELECT count(*) FROM staging.stg_tradeline__orders AS o
    JOIN marts.dim_account AS a ON a.account_id = o.account_id
     AND a.is_current
    JOIN marts.dim_region AS r USING (region_key)
    WHERE o.status = 'placed' AND o.account_id IN
          ({", ".join(str(m[0]) for m in moved)})
      AND CAST(timezone(r.timezone, timezone('UTC', o.placed_at))
               AS DATE) = DATE '{gone}'""").fetchone()
print(f"their placed orders that day: {orders}, each joined to the "
      "old version")

example = moved[0][0]
Path(__file__).with_suffix(".json").write_text(json.dumps({
    "account": example, "gone": gone, "moved": len(moved),
    "orders": orders,
    "audit": [[str(r[0]), r[1]] for r in con.execute(
        "SELECT valid_from, owner FROM audit_v WHERE account_id = ? "
        "ORDER BY 1", [example]).fetchall()],
    "changes": [[str(r[0]), r[1], r[2], r[3]] for r in con.execute(
        "SELECT changed_at, field_name, old_value, new_value FROM "
        "intermediate.int_account_changes WHERE account_id = ? AND "
        "change_date = DATE '2025-10-01'", [example]).fetchall()],
}, indent=1))
tidy("missing")
