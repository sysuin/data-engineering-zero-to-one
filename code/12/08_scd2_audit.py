# History from Relay's audit: today's state walked backwards through
# every change, for one account and then for all of them.
from _dims import built, tidy

con = built("audit")
print("account 1130 in today's export:")
print("  {} | {} | {}".format(*con.execute(
    "SELECT segment, owner_name, status "
    "FROM staging.stg_relay__accounts "
    "WHERE account_id = 1130 ORDER BY snapshot_date DESC LIMIT 1"
).fetchone()))
print("its audit, newest first:")
for at, field, old, new in con.execute(
        "SELECT changed_at, field_name, old_value, new_value "
        "FROM intermediate.int_account_changes WHERE account_id = 1130 "
        "ORDER BY changed_at DESC").fetchall():
    print(f"  {at:%Y-%m-%d %H:%M} {field:<9} {old or '-':<14} -> "
          f"{new or '-'}")
print("its versions:")
for vf, vt, seg, owner, status in con.execute(
        "SELECT valid_from, valid_to, segment, owner, status "
        "FROM marts.dim_account WHERE account_id = 1130 ORDER BY 1"
).fetchall():
    print(f"  {vf} to {str(vt or '(current)'):<10} {seg:<15}"
          f"{owner:<15}{status}")

print("\nall accounts:")
for field, n in con.execute(
        "SELECT field_name, count(*) "
        "FROM intermediate.int_account_changes "
        "GROUP BY 1 ORDER BY 2 DESC, 1").fetchall():
    print(f"  {'audit rows, ' + field:<44}{n:>8,}")
created, older, versions = con.execute("""
    SELECT count(DISTINCT account_id) FILTER (field_name = 'created'),
           (SELECT count(*) FROM marts.dim_account AS d
            WHERE d.is_current AND d.account_id > 0 AND NOT EXISTS (
                SELECT 1 FROM intermediate.int_account_changes AS c
                WHERE c.account_id = d.account_id
                  AND c.field_name = 'created')),
           (SELECT count(*) FROM marts.dim_account WHERE account_id > 0)
    FROM intermediate.int_account_changes""").fetchone()
for label, n in (("accounts created since 1 January 2022", created),
                 ("accounts older, known from the first night", older),
                 ("versions", versions)):
    print(f"  {label:<44}{n:>8,}")

# A second source: every staged night must show the version valid then.
nights, agree = con.execute("""
    SELECT count(*), count(*) FILTER (
        s.segment = v.segment AND s.owner_name = v.owner
        AND s.status = v.status)
    FROM staging.stg_relay__accounts AS s
    JOIN marts.dim_account AS v
      ON v.account_id = s.account_id AND s.snapshot_date >= v.valid_from
     AND (s.snapshot_date < v.valid_to OR v.valid_to IS NULL)
    WHERE s.source_format = 'relay'""").fetchone()
print(f"staged account-nights {nights:,}; the version valid that "
      f"night\nagrees on {agree:,}")
tidy("audit")
