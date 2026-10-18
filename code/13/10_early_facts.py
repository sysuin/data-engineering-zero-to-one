# Orders that arrive before their account: the run of 1 January 2023
# points them at the unknown member, and the next run re-points them.
# timeout: 600
import shutil

import duckdb

from groundwork import facts
from groundwork.config import NOW
from groundwork.transform import runner

from _facts import HERE, SHARED, built, tidy

built().close()
tidy("early")
(HERE / "early").mkdir(parents=True)
shutil.copy(SHARED / "warehouse.duckdb", HERE / "early" / "w.duckdb")
con = duckdb.connect(str(HERE / "early" / "w.duckdb"))
models = runner.discover()
sql = {m: runner.compile_sql(models[m], models)
       for m in ("dim_account", "fct_order_lines", "fct_orders")}
RUN = "TIMESTAMP '2023-01-01 06:00'"
placed = ("(SELECT * FROM intermediate.int_order_lines "
          f"WHERE placed_at < {RUN})")

# The run at 06:00 on 1 January 2023 read the export of 31 December:
# no account created since then was in it.
con.execute("""DELETE FROM marts.dim_account WHERE account_id IN (
    SELECT account_id FROM marts.dim_account GROUP BY 1
    HAVING min(valid_from) >= DATE '2023-01-01')""")
for m in ("fct_order_lines", "fct_orders"):
    con.execute(f"DELETE FROM marts.{m}")
    con.execute(f"INSERT INTO marts.{m} "
                + sql[m].replace("intermediate.int_order_lines",
                                 placed))

SHOW = """SELECT o.order_id, i.account_id, o.date_key, o.account_key,
                 r.region_name, o.revenue
          FROM marts.fct_orders AS o
          JOIN staging.stg_tradeline__orders AS s USING (order_id)
          JOIN intermediate.int_account_identity AS i
            ON i.crm_id = s.account_id
          JOIN marts.dim_region AS r USING (region_key)
          WHERE o.account_key = (SELECT account_key
                                 FROM marts.dim_account
                                 WHERE account_id = -1)
             OR o.order_id IN (85260, 85261)
          ORDER BY 1"""


def mine(problems):
    """Only the order facts: this sandbox rebuilt nothing else."""
    return [p for p in problems if p.startswith("fct_order")]


def show(title):
    print(title)
    for r in con.execute(SHOW).fetchall():
        print(f"  {r[0]}  account {r[1]:>2}  {r[2]}  {r[3]}  "
              f"{r[4]:<9}{r[5]:>7,.2f}")


(n,) = con.execute("SELECT count(*) FROM marts.fct_orders").fetchone()
print(f"run of 1 January 2023, 06:00 UTC: {n:,} orders in the fact")
show("on the unknown member:")
print(f"facts.check: {mine(facts.check(con))}")

# The next night's export carries the accounts; the next run re-points.
con.execute("DELETE FROM marts.dim_account")
con.execute(f"INSERT INTO marts.dim_account {sql['dim_account']}")
got = facts.incremental(con, since=NOW)
print(f"\nnext run: restated {got['orders']} orders: {got['unknown']} "
      f"unknown, {got['changed']} changed, {got['stale']} stale")
show("the same orders now:")
print(f"facts.check: {mine(facts.check(con)) or 'every promise kept'}")
con.close()
tidy("early")
