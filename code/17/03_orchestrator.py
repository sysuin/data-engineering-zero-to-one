# The runner on the nightly graph with stand-in tasks on a simulated
# clock: a state for every task, and a failure that stops only what
# waits for it.
import shutil
from datetime import datetime

from groundwork.config import SANDBOX
from groundwork.orchestrate import runner
from groundwork.orchestrate.dag import Fatal, nightly

HERE = SANDBOX / "ch17" / "03"
shutil.rmtree(HERE, ignore_errors=True)
env = runner.Env(warehouse=HERE / "warehouse.duckdb")
DUE = datetime(2026, 1, 5, 6, 0)


def stand_in(minutes, fail=None):
    """A task that takes `minutes` of the simulated clock."""
    def fn(ctx):
        ctx.clock.sleep(minutes * 60)
        if fail:
            raise Fatal(fail)
        return f"{minutes} min"
    return fn


def stand_ins(**fail):
    fns = {n: stand_in(4, fail.get(n)) for n in nightly().tasks}
    fns["wait_relay"] = lambda ctx: True        # the file is there
    return nightly().stubbed(fns)


r = runner.run(stand_ins(), "2026-01-04", env=env,
               clock=runner.SimClock(DUE))
print(f"{r.run_id}: {r.state}")
for row in runner.history(env.warehouse, "nightly", "2026-01-04"):
    _, task, n, state, start, end, _ = row
    print(f"  {start:%H:%M}-{end:%H:%M}  {task:<17} {state}")

bad = stand_ins(ingest_ledger="no header row")
r = runner.run(bad, "2026-01-04", env=env, clock=runner.SimClock(DUE))
print()
print(r.report())

with runner.lock(env, "nightly"):           # a run already going
    try:
        runner.run(stand_ins(), "2026-01-04", env=env,
                   clock=runner.SimClock(DUE))
    except runner.RunInProgress as e:
        print(f"\nRunInProgress: {e.args[0].split(' holds')[0]}")
shutil.rmtree(HERE)
