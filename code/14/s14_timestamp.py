# The accounts snapshot with the timestamp strategy, trusting Relay's
# updated_at, beside the check strategy on segment, owner and status,
# over the same fourteen nights. In a copy of the project.
# timeout: 1800
import shutil

from _dbt import HERE, PROJECT, built, connect, copy, dbt, tidy

project = HERE / "project"
shutil.rmtree(project, ignore_errors=True)
shutil.copytree(PROJECT, project, ignore=shutil.ignore_patterns(
    "target", "logs"))
snap = project / "snapshots" / "snp_relay__accounts.sql"
text = snap.read_text()
for a, b in (("strategy='check',", "strategy='timestamp',"),
             ("check_cols=['segment', 'owner_name', 'status'],",
              "updated_at='updated_at',"),
             ("status, snapshot_date",
              "status, snapshot_date, updated_at")):
    text = text.replace(a, b)
snap.write_text(text)
path = copy("timestamp", source=built())
con = connect(path, read_only=False)
con.execute("DROP SCHEMA IF EXISTS snapshots CASCADE")
nights = [str(d) for (d,) in con.execute("""
    SELECT DISTINCT snapshot_date FROM staging.stg_relay__accounts
    WHERE snapshot_date >= DATE '2025-01-01' ORDER BY 1""").fetchall()]
con.close()
for night in nights:
    dbt("snapshot", path=path, project=project, vars={"night": night})

con = connect(path)
n = lambda sql: con.execute(sql).fetchone()[0]           # noqa: E731
T = "snapshots.snp_relay__accounts"
was = f"""SELECT *, lag((segment, owner_name, status)) OVER (
              PARTITION BY account_id ORDER BY dbt_valid_from) AS was
          FROM {T}"""
rows = n(f"SELECT count(*) FROM {T}")
later = n(f"SELECT count(*) FROM ({was}) WHERE was IS NOT NULL")
same = n(f"""SELECT count(*) FROM ({was})
             WHERE was = (segment, owner_name, status)""")
print(f"timestamp strategy, {len(nights)} nights: {rows:,} rows")
print(f"  versions after an account's first          {later:>6,}")
print(f"  of which segment, owner and status the same {same:>5,}")
print(f"  earliest dbt_valid_from "
      f"{n(f'SELECT min(dbt_valid_from) FROM {T}')}")
con.close()
shutil.rmtree(project)
tidy("timestamp")
