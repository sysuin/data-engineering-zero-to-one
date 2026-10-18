# Exercise 6.2: a transactional refresh killed for real, by a process
# that ends itself half way; the table is whole when it is reopened.
import subprocess
import sys

import duckdb
from _first_pipeline import WAREHOUSE

from groundwork.config import TRADELINE_DB


def fingerprint(con):
    return con.execute("SELECT count(*), sum(hash(t)) "
                       "FROM order_lines t").fetchone()


WAREHOUSE.unlink(missing_ok=True)
con = duckdb.connect(str(WAREHOUSE))
con.execute("SET enable_progress_bar = false")
con.execute(f"ATTACH '{TRADELINE_DB}' AS tl (TYPE sqlite, READ_ONLY)")
con.execute("CREATE TABLE order_lines AS SELECT order_id, line_no, "
            "qty, unit_price_cents FROM tl.order_lines")
before = fingerprint(con)
con.close()

CHILD = f"""
import os, duckdb
con = duckdb.connect({str(WAREHOUSE)!r})
con.execute("SET enable_progress_bar = false")
con.execute("ATTACH '{TRADELINE_DB}' AS tl (TYPE sqlite, READ_ONLY)")
con.execute("BEGIN")
con.execute("DELETE FROM order_lines")
for i in range(2):
    con.execute("INSERT INTO order_lines SELECT order_id, line_no, "
                "qty, unit_price_cents FROM tl.order_lines "
                "ORDER BY order_id, line_no LIMIT 250000 OFFSET ?",
                [i * 250000])
print("inside the transaction:",
      con.execute("SELECT count(*) FROM order_lines").fetchone()[0])
os._exit(1)                        # no ROLLBACK, no close, no goodbye
"""
done = subprocess.run([sys.executable, "-c", CHILD],
                      capture_output=True, text=True)
print(done.stdout.strip(), f"(exit code {done.returncode})")

con = duckdb.connect(str(WAREHOUSE))
after = fingerprint(con)
print(f"rows before {before[0]:,}, after reopening {after[0]:,}")
print("fingerprint unchanged:", before == after)
con.close()
WAREHOUSE.unlink()
