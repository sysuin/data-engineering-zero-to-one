# timeout: 1800
# Groundwork v0.4's ingest for the book's last full day, from nothing,
# into a sandbox: every source, every table, each against a contract.
import shutil

import duckdb

from groundwork.config import NOW, SANDBOX
from groundwork.ingest.__main__ import report, run

here = SANDBOX / "ch10" / "v04"
shutil.rmtree(here, ignore_errors=True)
day = NOW.date().replace(day=4)
lines = run(day, run_at=NOW, root=here / "raw",
            warehouse=here / "warehouse.duckdb",
            archive=here / "landed")
print(report(lines, day, NOW))

con = duckdb.connect(str(here / "warehouse.duckdb"))
print("\naudit.contract_runs:")
for status, n, rows in con.execute("""
    SELECT status, count(*), sum(rows) FROM audit.contract_runs
    GROUP BY 1 ORDER BY 1""").fetchall():
    print(f"  {status:<7}{n:>5} checks {rows:>11,} rows")
print("audit.quarantine:")
for table, file, n, reason in con.execute("""
    SELECT table_name, file, row_number, reason
    FROM audit.quarantine ORDER BY 1, 2, 3""").fetchall():
    print(f"  {table}: {file.split('/')[-1]}, row {n}\n    {reason}")
con.close()
shutil.rmtree(here)
