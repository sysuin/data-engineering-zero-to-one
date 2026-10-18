# nondeterministic: timings vary from run to run and machine to machine
# One staging model as a view and as a table, and what each costs to
# build, to store and to read.
import json
import time
from pathlib import Path

import duckdb

from groundwork.transform import runner

from _pack import sandbox, tidy

warehouse, models = sandbox("materialise", pack=False)
runner.run(warehouse, folder=models)      # all of staging, as views
con = duckdb.connect(str(warehouse))
query = """SELECT sku, sum(qty * unit_price_cents) AS cents
           FROM staging.stg_tradeline__order_lines
           GROUP BY sku ORDER BY sku"""
found = runner.discover(models)
sql = runner.compile_sql(found["stg_tradeline__order_lines"], found)


def timed(statement, repeat=1):
    best = None
    for _ in range(repeat):
        t = time.perf_counter()
        result = con.execute(statement).fetchall()
        took = time.perf_counter() - t
        best = took if best is None else min(best, took)
    return best, result


def blocks():
    return con.execute("SELECT used_blocks * block_size "
                       "FROM pragma_database_size()").fetchone()[0]


before = blocks()
view_build, _ = timed("CREATE OR REPLACE VIEW staging.stg_tradeline__"
                      f"order_lines AS {sql}")
view_read, a = timed(query, repeat=3)
con.execute("DROP VIEW staging.stg_tradeline__order_lines")
table_build, _ = timed("CREATE TABLE staging.stg_tradeline__"
                       f"order_lines AS {sql}")
con.execute("CHECKPOINT")
table_read, b = timed(query, repeat=3)
grew = blocks() - before
assert a == b, "the view and the table disagree"

print(f"{'':<8}{'build':>10}{'one read':>10}{'stored':>16}")
print(f"{'view':<8}{view_build:>9.3f}s{view_read:>9.3f}s{0:>14,} B")
print(f"{'table':<8}{table_build:>9.3f}s{table_read:>9.3f}s"
      f"{grew:>14,} B")
print(f"a read of the view costs {view_read / table_read:.0f}x a read "
      "of the table;")
print(f"building the table costs {table_build / table_read:.0f} reads "
      "of the table")
print(f"both give {len(a)} SKUs, "
      f"{sum(r[1] for r in a):,} cents")
Path(__file__).with_suffix(".json").write_text(json.dumps(dict(
    view=[view_build, view_read], table=[table_build, table_read],
    stored=grew), indent=1))
con.close()
tidy("materialise")
