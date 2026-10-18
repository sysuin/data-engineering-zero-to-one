# Retries with backoff, a fatal error, a timeout, and a retry after a
# timeout that came too late: five tasks on a simulated clock.
import shutil
from datetime import datetime

import duckdb

from groundwork.config import SANDBOX, TRADELINE_DB
from groundwork.orchestrate import runner
from groundwork.orchestrate.dag import DAG, Fatal, Task

HERE = SANDBOX / "ch17" / "04"
shutil.rmtree(HERE, ignore_errors=True)
HERE.mkdir(parents=True)
env = runner.Env(warehouse=HERE / "warehouse.duckdb")
tries = {}


def flaky(ctx):                 # the desk drops the first two calls
    ctx.clock.sleep(60)
    if ctx.try_number <= 2:
        raise ConnectionError("connection reset by peer")
    return "pulled on try 3"


def broken(ctx):                # a contract broke: no retry helps
    raise Fatal("relay.accounts: column 'region' missing")


def hangs(ctx):                 # waits on a lock that never frees
    ctx.clock.sleep(3 * 3600)


NOV = f"""SELECT id FROM sqlite_scan('{TRADELINE_DB}', 'orders')
          WHERE placed_at >= '2025-11-01' AND placed_at < '2025-12-01'
            AND status = 'placed'"""


def load(how):
    """Load November's orders, then finish slowly on the first try."""
    def fn(ctx):
        con = duckdb.connect(str(HERE / "reporting.duckdb"))
        con.execute(f"CREATE TABLE IF NOT EXISTS {how} (id BIGINT)")
        if how == "replaced":       # Chapter 6: replace the partition
            con.execute(f"DELETE FROM {how}")
        con.execute(f"INSERT INTO {how} {NOV}")
        con.close()
        ctx.clock.sleep(45 * 60 if ctx.try_number == 1 else 4 * 60)
        return "loaded"
    return fn


dag = DAG("retries", [
    Task("flaky", flaky, retries=3, retry_delay=300),
    Task("broken", broken, retries=3, retry_delay=300),
    Task("hangs", hangs, retries=1, retry_delay=300, timeout=3600),
    Task("appended", load("appended"), retries=1, retry_delay=300,
         timeout=1800),
    Task("replaced", load("replaced"), retries=1, retry_delay=300,
         timeout=1800)])
r = runner.run(dag, "2026-01-04", env=env,
               clock=runner.SimClock(datetime(2026, 1, 5, 6, 0)))
rows = runner.history(env.warehouse, "retries", "2026-01-04")
for name in dag.order():
    t = dag.tasks[name]
    limit = f"{t.timeout / 60:.0f} min" if t.timeout else "none"
    print(f"{name}: retries {t.retries}, timeout {limit}")
    for _, task, n, state, start, end, detail in rows:
        if task == name:
            line = f"  try {n}  {start:%H:%M}-{end:%H:%M}  {state}"
            print(f"{line:<34}{detail}" if state == "succeeded"
                  else f"{line}\n{'':>9}{detail}")
print(f"run {r.state}")

con = duckdb.connect(str(HERE / "reporting.duckdb"))
(n,) = con.execute(f"SELECT count(*) FROM ({NOV})").fetchone()
print(f"\nNovember's orders in Tradeline {n:>8,}")
for how in ("appended", "replaced"):
    (k,) = con.execute(f"SELECT count(*) FROM {how}").fetchone()
    print(f"  rows in {how:<21}{k:>8,}  ({r.tasks[how].state})")
con.close()
shutil.rmtree(HERE)
