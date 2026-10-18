# One staging model on the night the CRM changed: two formats in raw,
# one meaning in staging.
import shutil
from datetime import date

import duckdb

from groundwork.config import SANDBOX
from groundwork.ingest import relay
from groundwork.transform import runner
from meridian.sources import relay as crm

here = SANDBOX / "ch11" / "migration"
shutil.rmtree(here, ignore_errors=True)
where = dict(landing=here / "landing", root=here / "raw",
             warehouse=here / "warehouse.duckdb",
             archive=here / "landed")
nights = [date(2024, 2, 29), date(2024, 3, 1)]
for night in nights:
    crm.export(night, where["landing"] / "relay")
    relay.load(night, **where)
runner.run(where["warehouse"], select=["stg_relay__accounts"])

con = duckdb.connect(str(where["warehouse"]))
cols = "postcode, customer_since, key_account, status, contact_name"
print("raw.relay_accounts, account 2485:")
for row in con.execute(
        f"SELECT load_date, source_format, {cols} "
        "FROM raw.relay_accounts WHERE account_id = '2485' "
        "ORDER BY load_date").fetchall():
    print(f"  {row[0]:%d %b} {row[1]:<6}",
          " | ".join(map(str, row[2:])))
cols = ("postcode, customer_since_date, is_key_account, status, "
        "contact_name")
print("staging.stg_relay__accounts, account 2485:")
for row in con.execute(
        f"SELECT snapshot_date, source_format, {cols} "
        "FROM staging.stg_relay__accounts WHERE account_id = 2485 "
        "ORDER BY snapshot_date").fetchall():
    print(f"  {row[0]:%d %b} {row[1]:<6}",
          " | ".join(map(str, row[2:])))

print("\nrows by night         raw   staging")
for day, raw, stg in con.execute("""
        SELECT load_date, count(*),
               (SELECT count(*) FROM staging.stg_relay__accounts s
                WHERE s.snapshot_date = r.load_date)
        FROM raw.relay_accounts r GROUP BY load_date ORDER BY 1
        """).fetchall():
    print(f"  {day}     {raw:>6,}  {stg:>8,}")

compare = ["postcode", "customer_since_date", "is_key_account",
           "status", "closed_date", "contact_name", "account_name"]
both = """FROM staging.stg_relay__accounts a
          JOIN staging.stg_relay__accounts b USING (account_id)
          WHERE a.snapshot_date = '2024-02-29'
            AND b.snapshot_date = '2024-03-01'"""
(n,) = con.execute(f"SELECT count(*) {both}").fetchone()
print(f"\naccounts in both exports: {n:,}; agreeing column by column:")
for c in compare:
    (same,) = con.execute(
        f"SELECT count(*) {both} AND a.{c} IS NOT DISTINCT FROM b.{c}"
    ).fetchone()
    print(f"  {c:<20} {same:>6,} of {n:,}")
raw_same = con.execute("""
    SELECT count(*) FROM raw.relay_accounts a
    JOIN raw.relay_accounts b USING (account_id)
    WHERE a.load_date = '2024-02-29' AND b.load_date = '2024-03-01'
      AND a.customer_since = b.customer_since""").fetchone()[0]
print(f"  customer_since as text in raw: {raw_same:,} of {n:,}")
con.close()
shutil.rmtree(here)
