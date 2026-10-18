# timeout: 900
# History from nightly snapshots: every export since 2022 replayed from
# IT's archive, each night compared with the one before.
import csv
import io
import shutil
from datetime import date, timedelta
from pathlib import Path

import duckdb
import pandas as pd

from groundwork.config import SANDBOX
from groundwork.ingest.relay import LEGACY, sniff
from meridian.sources import relay as crm

here = SANDBOX / "ch12" / "snapshots"
shutil.rmtree(here, ignore_errors=True)
here.mkdir(parents=True)
con = duckdb.connect(str(here / "snapshots.duckdb"))
con.execute("CREATE TABLE snapshots (night DATE, account_id INTEGER, "
            "segment VARCHAR, owner VARCHAR, status VARCHAR)")


def accounts(path):
    """One night's accounts.csv, either CRM's format, as tuples."""
    data = path.read_bytes()
    fmt, encoding, delimiter = sniff(data)
    rows = csv.reader(io.StringIO(data.decode(encoding)),
                      delimiter=delimiter)
    header = [LEGACY["accounts"].get(h, h) for h in next(rows)]
    for r in rows:
        r = dict(zip(header, r))
        yield (int(r["account_id"]), r["segment"], r["owner"].strip(),
               r["status"].lower())


night, last, nights, missing = date(2022, 1, 1), date(2026, 1, 4), 0, []
while night <= last:
    if crm.export(night, here / "landing"):
        path = here / "landing" / night.isoformat() / "accounts.csv"
        snap = pd.DataFrame(accounts(path),
                            columns=["account_id", "segment", "owner",
                                     "status"])
        snap.insert(0, "night", night)
        con.execute("INSERT INTO snapshots SELECT * FROM snap")
        nights += 1
    else:
        missing.append(night)
    shutil.rmtree(here / "landing", ignore_errors=True)
    night += timedelta(days=1)

(rows,) = con.execute("SELECT count(*) FROM snapshots").fetchone()
print(f"nights replayed {nights:,}, missing {len(missing)}; "
      f"account-nights {rows:,}")

sql = Path(__file__).with_name("scd2_snapshots.sql").read_text()
con.execute(f"CREATE TABLE versions AS {sql}")
print(f"\n{'accounts':<20}{'how many':>10}{'versions':>10}"
      f"{'begun later':>13}")
for label, where in (("every account", "true"),
                     ("in today's export", "account_id IN (SELECT "
                      "account_id FROM snapshots WHERE night = "
                      f"DATE '{last}')")):
    n, ids, opened = con.execute(
        "SELECT count(*), count(DISTINCT account_id), "
        f"count(*) FILTER (valid_from > DATE '2022-01-01') "
        f"FROM versions WHERE {where}").fetchone()
    print(f"{label:<20}{ids:>10,}{n:>10,}{opened:>13,}")
