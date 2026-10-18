# The week the CRM changed: the old loader on the new export, and
# Groundwork's Relay loader reading both formats into one table.
import csv
import json
import shutil
from datetime import date, timedelta
from pathlib import Path

import duckdb

from groundwork.config import SANDBOX
from groundwork.ingest import files, relay
from meridian.sources import relay as crm

here = SANDBOX / "ch09" / "migration"
shutil.rmtree(here, ignore_errors=True)
where = dict(landing=here / "landing", root=here / "raw",
             warehouse=here / "warehouse.duckdb",
             archive=here / "landed")
nights = [date(2024, 2, 27) + timedelta(days=i) for i in range(6)]
for night in nights:
    crm.export(night, where["landing"] / "relay")


def old_loader(path):
    """The loader that read the old CRM's export for two years."""
    with open(path, encoding="cp1252", newline="") as f:
        return [r["Acct ID"] for r in csv.DictReader(f, delimiter=";")]


print("The old loader:")
for night in nights[1:4]:
    path = where["landing"] / "relay" / str(night) / "accounts.csv"
    try:
        print(f"  {night}  {len(old_loader(path)):,} accounts")
    except FileNotFoundError:
        print(f"  {night}  no file")
    except Exception as e:
        print(f"  {night}  {type(e).__name__}: {str(e)[:40]}")

print("\nGroundwork's Relay loader:")
for night in nights:
    try:
        loads = relay.load(night, **where)
    except files.MissingFile:
        print(f"  {night}  MISSING: load failed, logged")
        continue
    got = {x.table_name: x for x in loads}
    con = duckdb.connect(str(where["warehouse"]))
    (fmt,) = con.execute(
        "SELECT note FROM audit.file_ledger WHERE name = ?",
        [f"relay/{night}/accounts.csv"]).fetchone()
    con.close()
    a, c = got["accounts"], got["contracts"]
    print(f"  {night}  {fmt:<6}  accounts {a.rows_written:,}  "
          f"contracts {c.rows_written:,}")

con = duckdb.connect(str(where["warehouse"]))
ids = {d: {r[0]: r[1] for r in con.execute(
    "SELECT account_id, customer_since FROM raw.relay_accounts "
    "WHERE load_date = ?", [d]).fetchall()}
    for d in (date(2024, 2, 29), date(2024, 3, 1))}
before, after = ids.values()
gone, new = set(before) - set(after), set(after) - set(before)
print(f"\nonly on 29 Feb: {len(gone)}, ids {min(gone, key=int)} to "
      f"{max(gone, key=int)}")
print(f"only on 1 Mar: {len(new)}, customer_since "
      + ", ".join(sorted({after[i] for i in new})))
print("\nAccount 2485 in raw.relay_accounts, either side:")
cols = ("postcode", "customer_since", "contact_name", "updated_at")
rows = con.execute(
    f"SELECT load_date, source_format, {', '.join(cols)} "
    "FROM raw.relay_accounts WHERE account_id = '2485' "
    "AND load_date IN ('2024-02-29', '2024-03-01') "
    "ORDER BY load_date").fetchall()
for day, fmt, *values in rows:
    print(f"{day:%d %b} {fmt:<6} " + "  ".join(map(str, values)))

# The two lines as the files hold them, for Figure 9.3.
lines = {}
for night, enc in (("2024-02-29", "cp1252"),
                   ("2024-03-01", "utf-8-sig")):
    text = (where["landing"] / "relay" / night / "accounts.csv"
            ).read_text(encoding=enc).splitlines()
    lines[night] = [text[0]] + [t for t in text
                                if t.startswith(("2485;", "2485,"))]
Path(__file__).with_suffix(".json").write_text(json.dumps(
    {"lines": lines, "raw": [[str(v) for v in r] for r in rows],
     "columns": ["load_date", "source_format", *cols]}, indent=1,
    ensure_ascii=False))
shutil.rmtree(here)
