# A dbt snapshot of Relay's accounts, run once for each of the fourteen
# nights raw holds, set beside Chapter 12's dim_account, which was
# rebuilt from Relay's audit.
# timeout: 1800
from _dbt import built, connect, copy, dbt, tidy

path = copy("snapshot", source=built())
con = connect(path, read_only=False)
con.execute("DROP SCHEMA IF EXISTS snapshots CASCADE")
nights = [str(d) for (d,) in con.execute("""
    SELECT DISTINCT snapshot_date FROM staging.stg_relay__accounts
    WHERE snapshot_date >= DATE '2025-01-01' ORDER BY 1""").fetchall()]
con.close()
for night in nights:                      # one run a night, as it would
    dbt("snapshot", path=path, vars={"night": night})
print(f"nights replayed {len(nights)}, {nights[0]} to {nights[-1]}")

con = connect(path)
n = lambda sql: con.execute(sql).fetchone()[0]           # noqa: E731
SNAP = """(SELECT account_id, CAST(dbt_valid_from AS DATE) AS day,
                  dbt_valid_to, segment, owner_name AS owner, status
           FROM snapshots.snp_relay__accounts)"""
DIM = """(SELECT account_id, valid_from AS day, segment, owner, status
          FROM marts.dim_account WHERE account_id > 0)"""
first = f"DATE '{nights[0]}'"
rows = (n(f"SELECT count(*) FROM {SNAP}"),
        n(f"SELECT count(DISTINCT account_id) FROM {SNAP}"),
        n(f"SELECT count(*) FROM {SNAP} WHERE day = {first}"),
        n(f"SELECT count(*) FROM {SNAP} WHERE day > {first}"),
        n(f"SELECT count(*) FROM {SNAP} WHERE dbt_valid_to IS NULL"))
print(f"snapshot rows {rows[0]:,}, accounts {rows[1]:,}")
print(f"  opened on the first night {rows[2]:>8,}")
print(f"  opened on a later night   {rows[3]:>8,}")
print(f"  current (dbt_valid_to NULL) {rows[4]:>6,}")

later = n(f"SELECT count(*) FROM {DIM} WHERE day > {first}")
same = n(f"""SELECT count(*) FROM {SNAP} AS s JOIN {DIM} AS d
             USING (account_id, day, segment, owner, status)
             WHERE day > {first}""")
before = (n(f"SELECT count(*) FROM {DIM} WHERE day < {first}"),
          n(f"SELECT count(*) FROM {SNAP} WHERE day < {first}"))
print(f"\ndim_account versions beginning after {nights[0]} {later:>5,}")
print(f"  the same account, day and values in the snapshot {same:>5,}")
print(f"versions beginning before {nights[0]}: dim_account "
      f"{before[0]:,}, snapshot {before[1]:,}")

acct = n(f"SELECT min(account_id) FROM {SNAP} WHERE day > {first}")
print(f"\naccount {acct}:")
print(f"  {'':<12}{'from':<12}{'to':<12}{'owner':<20}status")
for label, sql in (("snapshot", f"""
        SELECT CAST(dbt_valid_from AS DATE), CAST(dbt_valid_to AS DATE),
               owner_name, status FROM snapshots.snp_relay__accounts
        WHERE account_id = {acct} ORDER BY 1"""), ("dim_account", f"""
        SELECT valid_from, valid_to, owner, status
        FROM marts.dim_account WHERE account_id = {acct}
          AND coalesce(valid_to, DATE '9999-12-31') > DATE '2025-06-01'
        ORDER BY 1""")):
    for a, b, owner, status in con.execute(sql).fetchall():
        print(f"  {label:<12}{str(a):<12}{str(b or ''):<12}"
              f"{owner:<20}{status}")
        label = ""
con.close()
tidy("snapshot")
