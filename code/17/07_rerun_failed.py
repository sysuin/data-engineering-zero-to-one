# timeout: 2400
# A real night in a sandbox that fails at Frontline, then the rerun
# that runs only what failed and what waits for it.
import json
from dataclasses import replace
from pathlib import Path
from datetime import datetime

from _sandbox import env, tidy, work

from groundwork.config import NOW
from groundwork.orchestrate import runner
from groundwork.orchestrate.dag import DAG, nightly
from meridian.sources import Clock, frontline

if __name__ == "__main__":
    # The listing waits 5 seconds between tries, not 5 minutes.
    dag = nightly()
    dag = DAG(dag.name, [replace(t, retry_delay=5)
                         if t.name == "ingest_frontline" else t
                         for t in dag.tasks.values()])
    copy = work("07")
    down = frontline.serve(Clock(NOW), fail_every=1)   # every 503
    first = runner.run(dag, "2026-01-04",
                       env=env(copy, frontline_base=down))
    print(first.report() + "\n")
    again = runner.run(dag, "2026-01-04", env=env(copy), rerun=True,
                       run_at=datetime(2026, 1, 5, 9, 30))
    print(again.report() + "\n")
    rows = runner.history(copy / "warehouse.duckdb", "nightly",
                          "2026-01-04")
    print(f"audit.task_runs for 2026-01-04: {len(rows)} rows, of which")
    print(f"  {'run':<4}{'task':<18}{'try':<4}{'state':<14}detail")
    for run_id, task, n, state, _, _, detail in rows:
        if task == "ingest_frontline":
            note = detail.rsplit("; ", 1)[-1] if state != "failed" \
                else "(the error above)"
            print(f"  {run_id[-1]:<4}{task:<18}{n:<4}{state:<14}{note}")
    Path(__file__).with_suffix(".json").write_text(json.dumps(
        [[run_id, task, n, state, detail]
         for run_id, task, n, state, _, _, detail in rows], indent=0))
    tidy("07")
