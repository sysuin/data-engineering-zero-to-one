"""
Run Groundwork's nightly DAG for a logical date.

    python -m groundwork.orchestrate run --day D [--at T] [--rerun]
    python -m groundwork.orchestrate history --day D
    python -m groundwork.orchestrate show
    python -m groundwork.orchestrate backfill --from D1 --to D2

`run` is the night for logical date D, due at 06:00 UTC on D+1 (or
--at). Without --day it is the latest night already due: started by
cron at 06:00 UTC, or late after a machine was down, it works out
the logical date once, from the schedule, and every task is given
it. `--rerun` keeps what the latest run of D finished and runs
only what failed and what waits for it. `history` prints every try
of every run of D. `show` prints the DAG. Exit 1 if the run failed.

`backfill` is Chapter 18's: it runs the DAG for every logical date
in a range, oldest first, and lives in groundwork/orchestrate/
backfill.py, whose main(argv) this command hands its arguments to.
`drill` (Chapter 18 too) rebuilds random days in isolation and
compares them with what is there: drill.py's main(argv).

    python -m groundwork.orchestrate drill [--days 20]
"""
from __future__ import annotations

import argparse
import sys
from datetime import date, datetime, time, timedelta, timezone

from groundwork.orchestrate import runner
from groundwork.orchestrate.dag import DAGS


def latest_due(now: datetime, at: time = time(6)) -> date:
    """The logical date of the latest run due by `now` (UTC): the
    run for D is due at 06:00 on D+1."""
    due = datetime.combine(now.date(), at)
    if now < due:
        due -= timedelta(days=1)
    return due.date() - timedelta(days=1)


def show(dag) -> str:
    lv = dag.levels()
    out = [f"{dag.name}: {len(dag.tasks)} tasks"]
    for n in dag.order():
        ups = ", ".join(dag.tasks[n].upstream) or "-"
        out.append(f"  {lv[n]}  {n:<17} after {ups}")
    return "\n".join(out)


def main(argv=None) -> int:
    argv = sys.argv[1:] if argv is None else argv
    if argv[:1] == ["backfill"]:
        try:
            from groundwork.orchestrate import backfill
        except ImportError:
            print("Backfills arrive in Chapter 18 "
                  "(groundwork/orchestrate/backfill.py).")
            return 2
        return backfill.main(argv[1:])
    if argv[:1] == ["drill"]:                # Chapter 18's, too
        from groundwork.orchestrate import drill
        return drill.main(argv[1:])
    p = argparse.ArgumentParser(description=__doc__.split("\n")[1])
    p.add_argument("command", choices=("run", "history", "show"))
    p.add_argument("--dag", default="nightly", choices=sorted(DAGS))
    p.add_argument("--day", type=date.fromisoformat,
                   help="the logical date")
    p.add_argument("--at", type=datetime.fromisoformat,
                   help="when the run is due, UTC (default 06:00 on "
                        "the day after --day)")
    p.add_argument("--rerun", action="store_true",
                   help="run only what failed in the last run of --day")
    a = p.parse_args(argv)
    dag = DAGS[a.dag]()
    if a.command == "show":
        print(show(dag))
        return 0
    if a.day is None:
        if a.command == "history":
            p.error("--day is required")
        now = datetime.now(timezone.utc).replace(tzinfo=None)
        a.day = latest_due(now)       # the one place the clock is read
    if a.command == "history":
        env = runner.Env()
        for row in runner.history(env.warehouse, dag.name, a.day):
            run_id, task, n, state, start, end, detail = row
            print(f"{run_id}  {task:<17}{n:>2} {state:<16}{detail}")
        return 0
    r = runner.run(dag, a.day, run_at=a.at, rerun=a.rerun)
    print(r.report())
    return 0 if r.state == "succeeded" else 1


if __name__ == "__main__":
    raise SystemExit(main())
