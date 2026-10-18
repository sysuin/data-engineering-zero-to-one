# The Relay sensor against real export times: the night before the
# clocks went back, the night after, and a night with no export.
import shutil
from datetime import date, datetime, time, timedelta

from groundwork.config import SANDBOX
from groundwork.ingest import files
from groundwork.orchestrate import runner
from groundwork.orchestrate.dag import DAG, Task, nightly
from meridian.sources import relay

HERE = SANDBOX / "ch17" / "06"
shutil.rmtree(HERE, ignore_errors=True)
landing = HERE / "landing"
CRON = time(3, 15)                       # the crontab's Relay step


def ingest_relay(ctx):                   # stands in for the real one
    return f"started {ctx.now():%H:%M}"


sensor = nightly().tasks["wait_relay"]
dag = DAG("relay_only", [sensor, Task("ingest_relay", ingest_relay,
                                      ("wait_relay",))])
print(f"wait_relay: every {sensor.retry_delay / 60:.0f} min, for up "
      f"to {sensor.timeout / 3600:.0f} h; both start at {CRON:%H:%M}")
for night in (date(2025, 11, 1), date(2025, 11, 2), date(2025, 9, 7)):
    paths = relay.export(night, landing / "relay")  # mtime = written
    came = files.arrived_at(paths[0]) if paths else None
    start = datetime.combine(night + timedelta(days=1), CRON)
    cron = files.discover(landing, f"relay/{night}", by=start)
    env = runner.Env(warehouse=HERE / "warehouse.duckdb",
                     landing=landing)
    r = runner.run(dag, night, run_at=start, env=env,
                   clock=runner.SimClock(start))
    w, i = r.tasks["wait_relay"], r.tasks["ingest_relay"]
    came = f"written {came:%d %b %H:%M}" if came else "never written"
    print(f"\nnight {night}, export {came}")
    print(f"  {'cron':<14}{'found it' if cron else 'found nothing'}")
    print(f"  {'wait_relay':<14}{w.state}: {w.detail}")
    print(f"  {'ingest_relay':<14}{i.detail or i.state}")
shutil.rmtree(HERE)
