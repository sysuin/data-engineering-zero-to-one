# A closed month sent twice: the second version stops the load rather
# than taking over, and says which month and which files.
import os
import shutil
from datetime import date, datetime

import duckdb

from groundwork.config import LANDING, SANDBOX
from groundwork.ingest import files, ledger

here = SANDBOX / "ch09" / "s_ledger"
shutil.rmtree(here, ignore_errors=True)
(here / "landing" / "ledger").mkdir(parents=True)
where = dict(root=here / "raw", archive=here / "landed")
con = duckdb.connect(str(here / "w.duckdb"))
src = LANDING / "ledger" / "gl_revenue_2025-10.csv"
shutil.copy2(src, here / "landing" / "ledger" / src.name)


def once_per_period(data, arrival):
    """The ledger reader, refusing a second version of a month."""
    parsed = ledger.read(data, arrival)
    earlier = con.execute(
        "SELECT name FROM audit.file_ledger WHERE source = 'ledger' "
        "AND logical_key = ? AND status = 'loaded'",
        [parsed.logical_key]).fetchall()
    if earlier:
        raise ValueError(f"{parsed.logical_key} is closed; loaded "
                         f"from {earlier[0][0].split('/')[-1]}")
    return parsed


def run(day, at):
    try:
        found = files.discover(here / "landing", "ledger", by=at)
        x = files.load(con, "ledger", "gl_revenue", found,
                       day, run_at=at, read=once_per_period,
                       folder="ledger", **where)
        return f"succeeded, {x.rows_written} rows"
    except ValueError as e:
        return f"failed:\n  {e}"


print(f"October: {run(date(2025, 11, 7), datetime(2025, 11, 8, 6))}")
again = here / "landing" / "ledger" / "gl_revenue_2025-10 (v2).csv"
text = src.read_text().replace("Sales journal", "Sales journal (rev)")
again.write_text(text)
t = datetime(2025, 11, 20, 9).timestamp()
os.utime(again, (t, t))
print(f"resend:  {run(date(2025, 11, 20), datetime(2025, 11, 21, 6))}")
con.close()
shutil.rmtree(here)
