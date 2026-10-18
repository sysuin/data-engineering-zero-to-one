# Exercise 1: on a night with no Relay export, record the missing file
# after six hours and build anyway, on a simulated clock.
import shutil
from datetime import datetime, timedelta

from groundwork.config import SANDBOX
from groundwork.ingest.__main__ import run as ingest
from groundwork.orchestrate import runner
from groundwork.orchestrate.dag import DAG, Task, nightly, relay_export

HERE = SANDBOX / "ch17" / "s17"
shutil.rmtree(HERE, ignore_errors=True)
(HERE / "landing" / "relay").mkdir(parents=True)
GIVE_UP = timedelta(hours=6)


def export_or_give_up(ctx):
    """Yes when the export is there, or when six hours have passed."""
    return relay_export(ctx) or ctx.now() - ctx.run_at >= GIVE_UP


def relay_or_note(ctx):
    """Chapter 10's Relay ingest; a missing night is recorded (as a
    failed load, as Chapter 9 does) and is not the task's failure."""
    lines = ingest(ctx.day, run_at=ctx.now(), only=["relay"],
                   landing=ctx.env.landing, root=HERE / "raw",
                   warehouse=ctx.env.warehouse, archive=HERE / "landed")
    missing = [x for x in lines if x.status == "missing"]
    if missing and len(missing) < 2:
        raise RuntimeError("half an export")
    return "no export: built without it" if missing else "loaded"


dag = nightly()
fns = {n: (lambda ctx: "stand-in") for n in dag.tasks}
fns["wait_relay"] = export_or_give_up
fns["ingest_relay"] = relay_or_note
soft = DAG("soft", [Task(t.name, fns[t.name], t.upstream,
                         retry_delay=t.retry_delay,
                         timeout=t.timeout + 600 if t.sensor else None,
                         sensor=t.sensor) for t in dag.tasks.values()])
due = datetime(2025, 9, 8, 6, 0)
r = runner.run(soft, "2025-09-07", env=runner.Env(
    warehouse=HERE / "warehouse.duckdb", landing=HERE / "landing"),
    clock=runner.SimClock(due))
for name in ("wait_relay", "ingest_relay", "dbt_build", "publish"):
    t = r.tasks[name]
    print(f"{name:<14}{t.state:<11}{t.detail}")
print(f"run {r.state}")
shutil.rmtree(HERE)
