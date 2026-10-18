# Supplier lists loaded night by night, each file once, with a ledger
# of every file seen; a rerun finds nothing to do.
import shutil
from datetime import date, datetime, time, timedelta

import duckdb

from groundwork import loadlog
from groundwork.config import SANDBOX
from groundwork.ingest import files, suppliers
from meridian.sources import landing

here = SANDBOX / "ch09" / "load_once"
shutil.rmtree(here, ignore_errors=True)
where = dict(landing=here / "landing", root=here / "raw",
             warehouse=here / "warehouse.duckdb",
             archive=here / "landed")


def run(night, at=None):
    """The nightly run for `night`, at 06:00 the next morning."""
    at = at or datetime.combine(night + timedelta(days=1), time(6))
    landing.stage(at, where["landing"], under="suppliers")
    return suppliers.load(night, run_at=at, **where)


print(f"{'load date':<11} {'new':>4} {'rows':>5}  "
      f"{'ledger':>6}  {'kept':>4}  run at")
night = date(2024, 2, 28)
while night <= date(2024, 3, 4):
    x = run(night)
    con = duckdb.connect(str(where["warehouse"]))
    ledger = len(files.ledger(con, "suppliers"))
    con.close()
    kept = len(list(where["archive"].rglob("*.gz")))
    print(f"{night}  {x.extra['new_files']:>4} {x.rows_written:>5}  "
          f"{ledger:>6}  {kept:>4}  {x.started_at:%m-%d %H:%M}")
    night += timedelta(days=1)

# The operator reruns the last night at 06:40.
x = run(date(2024, 3, 4), datetime(2024, 3, 5, 6, 40))
print(f"rerun {x.load_id}: {x.extra['new_files']} new, "
      f"{x.rows_written} rows written")

con = duckdb.connect(str(where["warehouse"]))
print("\nThe ledger's rows for Aldridge's March list:")
for s in files.ledger(con, "suppliers"):
    if s.logical_key == "aldridge-co/2024-03":
        print(f"  {s.name.split('/')[-1]:<34} {s.status}, "
              f"{s.rows} rows")
        print(f"    arrived {s.arrived_at:%m-%d %H:%M}, load date "
              f"{s.load_date}, {s.sha256[:12]}")
(n,) = con.sql("SELECT count(*) FROM raw.suppliers_price_lists "
               "WHERE file LIKE '%Aldridge%Mar 2024%'").fetchone()
print(f"raw rows from those two files: {n}")
runs = loadlog.history(con, "suppliers")
print(f"load log: {len(runs)} attempts, "
      f"{sum(r.reconciled for r in runs)} reconciled")
con.close()
shutil.rmtree(here)
