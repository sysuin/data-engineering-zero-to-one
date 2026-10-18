"""
Groundwork v1.0's nightly run, and the one command that builds a
Groundwork from nothing.

    python -m groundwork.nightly show
    python -m groundwork.nightly run [--day D] [--at T] [--rerun]
    python -m groundwork.nightly build [--folder F] [--day D]

Chapter 17 left a hook for the chapters after it: `nightly(checks=)`
runs extra tasks after dbt_build and before publish. v1.0 puts five
tasks there and one before the build, and says what each waits for:

    restore_old_crm  the old CRM's last night, from IT's archive, if
                     raw does not hold it yet (Chapter 12)
    observe          the night's four signals (Chapter 21)
    sessions         the portal's stream into fct_portal_sessions
                     (Chapter 19), after the build: it needs dim_account
    serve            the metrics schema and the feature table
                     (Chapter 22), after sessions: one metric reads them
    compat           Books 1 and 2's tables, as views (Chapter 26)
    govern           every column classified, the masked tables, the
                     roles checked (Chapter 24), after everything that
                     makes a table

The DAG keeps Chapter 17's name, so a night run by this module follows
the nights before it in audit.dag_runs and reads its `since` from them.

`build` is a first night on an empty folder: the same DAG, run once
for the newest logical date the sources have, with nothing in raw.
It refuses a folder that already holds a warehouse.
"""
from __future__ import annotations

import argparse
import sys
from dataclasses import replace
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

from groundwork.config import GROUNDWORK, TODAY
from groundwork.observe import MONITORS
from groundwork.orchestrate import runner
from groundwork.orchestrate.dag import DAG, Fatal, Task
from groundwork.orchestrate.dag import nightly as _nightly

LAST_NIGHT = TODAY - timedelta(days=1)
"""The newest logical date the sources have: the night before TODAY."""


# ----------------------------------------------------------- the tasks
def restore_old_crm(ctx) -> str:
    """Chapter 12's restore: nothing to do when raw has the night."""
    from groundwork import identity
    n = identity.restore(warehouse=ctx.env.warehouse, root=ctx.env.root,
                         archive=ctx.env.archive, run_at=ctx.now())
    return f"{n:,} accounts restored" if n else "already in raw"


def portal_sessions(ctx) -> str:
    """Chapter 19's stream, over the files that have arrived."""
    from groundwork.stream import portal
    n = portal.run(ctx.env.warehouse, ctx.env.landing, by=ctx.now())
    if n.keyless:
        raise Fatal(f"{n.keyless:,} sessions with no account version")
    return (f"{n.files:,} files, {n.written:,} rows written, "
            f"{n.open} open")


def serve(ctx) -> str:
    """Chapter 22's publish and feature table, checked. The API keys its
    cache on audit.model_runs, which only Chapter 11's runner writes; a
    Groundwork that dbt alone has built has none until this makes it."""
    import duckdb

    from groundwork.serve import features, metrics
    from groundwork.transform.runner import DDL
    con = duckdb.connect(str(ctx.env.warehouse))
    try:
        con.execute("SET enable_progress_bar = false")
        con.execute(DDL)
        n = metrics.publish(con)
        rows = features.build(con, today=ctx.day + timedelta(days=1))
        problems = features.check(con)
    finally:
        con.close()
    if problems:
        raise Fatal("features: " + "; ".join(problems[:3]))
    return f"{n} metrics, {rows:,} feature rows"


def compat(ctx) -> str:
    """Books 1 and 2's tables, as views over the marts."""
    import duckdb

    from groundwork import reconcile_books
    con = duckdb.connect(str(ctx.env.warehouse))
    try:
        views = reconcile_books.install(con)
    finally:
        con.close()
    return f"{len(views)} views"


def govern(ctx) -> str:
    """Chapter 24's nightly step. Retention is reported, not applied:
    `python -m groundwork.govern --retain` applies it, once the
    policies' owners have agreed to them."""
    import duckdb

    from groundwork import storage
    from groundwork.govern import classify, masking, retention, roles
    try:
        masking.key()
    except RuntimeError as e:
        raise Fatal(str(e)) from None
    con = duckdb.connect(str(ctx.env.warehouse))
    try:
        con.execute("SET enable_progress_bar = false")
        storage.raw_views(con, root=ctx.env.root)
        columns = classify.classify(con)
        classify.save(con, columns)
        masking.build(con)
        roles.create(con)
        problems = roles.check(con)
        due = [a for a in retention.run(con, ctx.env.root,
                                        ctx.env.archive,
                                        ctx.day + timedelta(days=1),
                                        dry_run=True)
               if a.action == "deleted"]
    finally:
        con.close()
    if problems:
        raise Fatal("access: " + "; ".join(problems[:3]))
    return f"{len(columns)} columns, {len(due)} retention due"


# ------------------------------------------------------------- the DAG
CHECKS = (*MONITORS,
          Task("sessions", portal_sessions, timeout=3600, isolate=True),
          Task("serve", serve, timeout=1800, isolate=True),
          Task("compat", compat, timeout=300, isolate=True),
          Task("govern", govern, timeout=1800, isolate=True))

AFTER = {"serve": ("sessions",),
         "govern": ("observe", "serve", "compat")}


def nightly() -> DAG:
    """Chapter 17's nightly DAG with v1.0's tasks in it."""
    base = _nightly(checks=CHECKS)
    restore = Task("restore_old_crm", restore_old_crm, timeout=1800,
                   isolate=True)
    tasks = []
    for t in base.tasks.values():
        if t.name == "dbt_build":
            tasks.append(restore)
            t = replace(t, upstream=(*t.upstream, restore.name))
        if t.name in AFTER:
            t = replace(t, upstream=AFTER[t.name])
        tasks.append(t)
    return DAG(base.name, tasks)


def folder_env(folder: Path) -> runner.Env:
    """The real Groundwork for data/groundwork; any other folder is a
    Groundwork of its own, built with dbt's dev target."""
    if Path(folder).resolve() == GROUNDWORK.resolve():
        return runner.Env()
    from groundwork.orchestrate.backfill import sandbox_env
    return sandbox_env(Path(folder))


def build(folder: Path = GROUNDWORK, day: date = LAST_NIGHT,
          run_at: datetime | None = None) -> runner.Run:
    """A Groundwork from nothing: the nightly DAG on an empty folder."""
    env = folder_env(folder)
    if Path(env.warehouse).exists():
        raise FileExistsError(f"{env.warehouse} exists: build makes "
                              "a Groundwork from nothing, and this is "
                              "not nothing")
    Path(env.warehouse).parent.mkdir(parents=True, exist_ok=True)
    return runner.run(nightly(), day, run_at=run_at, env=env)


def show(dag: DAG) -> str:
    lv = dag.levels()
    out = [f"{dag.name}: {len(dag.tasks)} tasks"]
    for n in dag.order():
        ups = ", ".join(dag.tasks[n].upstream) or "-"
        out.append(f"  {lv[n]}  {n:<17} after {ups}")
    return "\n".join(out)


def main(argv=None) -> int:
    p = argparse.ArgumentParser(prog="python -m groundwork.nightly",
                                description=__doc__.split("\n")[1])
    p.add_argument("command", choices=("run", "build", "show"))
    p.add_argument("--day", type=date.fromisoformat,
                   help="the logical date (default: the latest due)")
    p.add_argument("--at", type=datetime.fromisoformat,
                   help="when the run is due, UTC")
    p.add_argument("--rerun", action="store_true",
                   help="run only what failed in the last run of --day")
    p.add_argument("--folder", type=Path, default=GROUNDWORK,
                   help="build: the empty folder to build in")
    a = p.parse_args(argv)
    dag = nightly()
    if a.command == "show":
        print(show(dag))
        return 0
    if a.command == "build":
        try:
            r = build(a.folder, a.day or LAST_NIGHT, a.at)
        except FileExistsError as e:
            print(e)
            return 2
    else:
        from groundwork.orchestrate.__main__ import latest_due
        if a.day is None:                     # the one clock read
            a.day = latest_due(datetime.now(timezone.utc)
                               .replace(tzinfo=None))
        r = runner.run(dag, a.day, run_at=a.at, rerun=a.rerun)
    print(r.report())
    return 0 if r.state == "succeeded" else 1


if __name__ == "__main__":
    sys.exit(main())
