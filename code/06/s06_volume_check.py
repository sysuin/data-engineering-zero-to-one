# Exercise 6.3: a load that reconciles perfectly with a stale source,
# and a check over the load log that catches the fall in its count.
import shutil
from datetime import date, datetime

import duckdb

from groundwork.config import SANDBOX
from groundwork.ingest import tradeline_full
from meridian.sources import tradeline

HERE = SANDBOX / "ch06" / "s06"
DB = HERE / "tl.db"
shutil.rmtree(HERE, ignore_errors=True)
nights = [("2025-12-01", "2025-12-02 02:05"),
          ("2025-12-02", "2025-12-03 02:05"),
          ("2025-12-03", "2025-06-01 02:05")]    # a stale replica
for day, moment in nights:
    tradeline.at(moment, DB)
    tradeline_full.load(date.fromisoformat(day), source=DB,
                        root=HERE / "raw", warehouse=HERE / "w.duckdb",
                        run_at=datetime.fromisoformat(moment))

CHECK = """
    SELECT load_id, source_count,
           lag(source_count) OVER w AS before,
           source_count < 0.99 * lag(source_count) OVER w AS fell
    FROM audit.load_log WHERE status = 'succeeded'
    WINDOW w AS (PARTITION BY source, table_name
                 ORDER BY load_date, load_id)
    ORDER BY table_name DESC, load_date"""
con = duckdb.connect(str(HERE / "w.duckdb"))
print(f"{'load_id':<34} {'count':>9} {'before':>9}  flag")
for load_id, n, before, fell in con.execute(CHECK).fetchall():
    b = "" if before is None else f"{before:,}"
    flag = "FELL" if fell else ""
    print(f"{load_id:<34} {n:>9,} {b:>9}  {flag}".rstrip())
con.close()
shutil.rmtree(HERE)
