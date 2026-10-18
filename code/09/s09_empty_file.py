# Two empty price lists, what the supplier loader makes of each, a
# check over the ledger, and a reader that refuses an empty list.
import os
import shutil
from datetime import date, datetime

import duckdb

from groundwork.config import SANDBOX
from groundwork.ingest import files, suppliers
from meridian.sources import landing

here = SANDBOX / "ch09" / "s_empty"
shutil.rmtree(here, ignore_errors=True)
here.mkdir(parents=True)
where = dict(root=here / "raw", archive=here / "landed")
con = duckdb.connect(str(here / "w.duckdb"))
folder = here / "landing" / "suppliers" / "voss-industrial"


def run(day, at, read=suppliers.read):
    """The supplier load for `day`, at `at`, with the given reader."""
    try:
        x = files.load(con, "suppliers", "price_lists",
                       files.discover(here / "landing", "suppliers",
                                      by=at), day, run_at=at,
                       read=read, folder="suppliers", **where)
        return (f"succeeded, {x.extra['new_files']} new, "
                f"{x.rows_written} rows")
    except Exception as e:
        return f"failed: {str(e)[:44]}"


def arrive(name, data, at):
    (folder / name).write_bytes(data)
    os.utime(folder / name, (at.timestamp(), at.timestamp()))


landing.stage(datetime(2024, 3, 2, 6), here / "landing", "suppliers")
print(f"1 Mar: {run(date(2024, 3, 1), datetime(2024, 3, 2, 6))}")
arrive("voss-industrial_2024-04.csv",
       b"sku,description,unit_cost,currency,effective_from\n",
       datetime(2024, 4, 1, 9))
print(f"header only: {run(date(2024, 4, 1), datetime(2024, 4, 2, 6))}")
arrive("voss-industrial_2024-05.csv", b"", datetime(2024, 5, 1, 9))
print(f"no bytes:    {run(date(2024, 5, 1), datetime(2024, 5, 2, 6))}")

print("\nthe check over the ledger finds:")
for name, size, rows, status in con.execute("""
        SELECT name, size, rows, status FROM audit.file_ledger
        WHERE size = 0 OR rows = 0 ORDER BY name""").fetchall():
    print(f"  {name.split('/')[-1]}  {size} bytes, rows {rows}, "
          f"{status}")


def strict(data, arrival):
    """The supplier reader, refusing a file with no rows."""
    if not data.strip():
        raise ValueError(f"{arrival.path.name} is empty")
    parsed = suppliers.read(data, arrival)
    if not parsed.rows:
        raise ValueError(f"{arrival.path.name}: no rows")
    return parsed


con.execute("DELETE FROM audit.file_ledger WHERE rows = 0 OR "
            "status = 'failed'")
print("\nwith the strict reader, the April run again:")
print(f"  {run(date(2024, 4, 1), datetime(2024, 4, 2, 6), strict)}")
con.close()
shutil.rmtree(here)
