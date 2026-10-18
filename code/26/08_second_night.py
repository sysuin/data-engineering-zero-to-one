# timeout: 3600
# nondeterministic: it prints how long the second night took
# The same night run a second time on a copy of the build from nothing,
# compared with the first table by table; then the watch at 07:00,
# and what it opens on launch morning.
import shutil
import time
from datetime import datetime

import duckdb
from _sandbox import FRESH, HERE, fresh

from groundwork import diff, nightly, storage
from groundwork.observe import alerts
from groundwork.orchestrate import runner

COPY = HERE / "second"


def second_night():
    shutil.rmtree(COPY, ignore_errors=True)
    shutil.copytree(FRESH, COPY)
    env = nightly.folder_env(COPY)
    with duckdb.connect(str(env.warehouse)) as con:
        storage.raw_views(con, root=env.root)     # raw from the copy
    began = time.monotonic()
    run = runner.run(nightly.nightly(), "2026-01-04", env=env)
    return env, run, (time.monotonic() - began) / 60


def main():
    first = fresh()
    env, run, minutes = second_night()
    tries = all(t.tries == 1 for t in run.tasks.values())
    print(f"{run.run_id}: {run.state}; every task first try: {tries}")
    for name in ("ingest_tradeline", "ingest_frontline", "ingest_relay",
                 "sessions"):
        print(f"  {name:<17}{run.tasks[name].detail}")
    print(f"  about {minutes:.0f} minutes")

    diffs = diff.compare(first, env.warehouse,
                         schemas=("marts", "features", "compat"))
    rows = sum(d.rows_b or 0 for d in diffs)
    print(f"against the first night: {sum(d.same for d in diffs)} of "
          f"{len(diffs)} tables and views identical,")
    print(f"  {rows:,} rows in them")
    for d in diffs:
        if not d.same:
            print(f"  {d.name}: {d.changed:,} changed, {d.added:,} new")

    at = datetime(2026, 1, 5, 7)
    with duckdb.connect(str(env.warehouse)) as con:
        w = alerts.watch(con, at, root=env.root)
    print(f"\nthe watch at {at:%H:%M} on 5 January opens "
          f"{len(w.opened)}:")
    for a in w.opened:
        print(f"  {a.severity:<7}{a.owner:<18}{a.subject}")
    shutil.rmtree(COPY)


if __name__ == "__main__":       # the night's tasks start processes
    main()
