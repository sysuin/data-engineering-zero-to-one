"""
Watch Groundwork, run its observed night, and ask where a column
comes from and what a change to it would reach.

    python -m groundwork.observe run --day D [--at T] [--rerun]
    python -m groundwork.observe watch [--at T]
    python -m groundwork.observe signals --day D [--at T]
    python -m groundwork.observe slo [--at T]
    python -m groundwork.observe lineage COLUMN [--down]
    python -m groundwork.observe impact COLUMN

`run` is Chapter 17's nightly DAG with the monitor task between the
build and publish. `watch` evaluates every alert rule as of T (default:
now, the one place the clock is read), records what opened, repeated
and resolved, and prints what was sent; it exits 1 if anything is
open. Run it from cron an hour after the night is due, on its own
line, so that it runs whether or not the night did:

    0 6 * * *  ... -m groundwork.observe run
    0 7 * * *  ... -m groundwork.observe watch

`lineage` and `impact` compile the dbt project into the warehouse's
folder (target/ beside it) and read the columns from the warehouse.
COLUMN is schema.relation.column, or a system's source.table.column.
"""
from __future__ import annotations

import argparse
import json
import sys
from datetime import date, datetime, timezone
from pathlib import Path

import duckdb

from groundwork.config import RAW, WAREHOUSE


def _now() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)


def graphs(warehouse: Path):
    """The column and table graphs, from a fresh compile of the dbt
    project against the warehouse."""
    from groundwork.observe import lineage
    from groundwork.orchestrate.dag import nightly
    target = Path(warehouse).parent / "target"
    manifest = json.loads(lineage.compile_project(
        warehouse, target).read_text())
    con = duckdb.connect(str(warehouse), read_only=True)
    try:
        schema = lineage.schema_of(con)
    finally:
        con.close()
    return (lineage.columns(manifest, schema),
            lineage.tables(nightly(), manifest), manifest)


def main(argv=None) -> int:
    argv = sys.argv[1:] if argv is None else argv
    p = argparse.ArgumentParser(prog="python -m groundwork.observe",
                                description=__doc__.split("\n")[1])
    p.add_argument("command", choices=("run", "watch", "signals", "slo",
                                       "lineage", "impact"))
    p.add_argument("column", nargs="?")
    p.add_argument("--day", type=date.fromisoformat)
    p.add_argument("--at", type=datetime.fromisoformat)
    p.add_argument("--rerun", action="store_true")
    p.add_argument("--down", action="store_true",
                   help="lineage: what is built from COLUMN instead")
    p.add_argument("--warehouse", type=Path, default=WAREHOUSE)
    p.add_argument("--raw", type=Path, default=RAW)
    a = p.parse_args(argv)

    if a.command == "run":
        from groundwork.observe import nightly
        from groundwork.orchestrate import runner
        from groundwork.orchestrate.__main__ import latest_due
        day = a.day or latest_due(_now())
        r = runner.run(nightly(), day, run_at=a.at, rerun=a.rerun,
                       env=runner.Env(warehouse=a.warehouse, root=a.raw))
        print(r.report())
        return 0 if r.state == "succeeded" else 1

    if a.command in ("watch", "slo", "signals"):
        from groundwork.observe import alerts, metrics
        at = a.at or _now()
        con = duckdb.connect(str(a.warehouse),
                             read_only=a.command != "watch")
        try:
            if a.command == "signals":
                day = a.day or metrics.quality.expected(
                    metrics.quality.RULES[0], at)
                for s in metrics.collect(con, day, a.at, a.raw):
                    v = "-" if s.value is None else f"{s.value:,.4g}"
                    print(f"{s.signal:<13}{s.subject:<26}{s.status:<6}"
                          f"{v:>9}  {s.measure}")
                return 0
            if a.command == "slo":
                b = alerts.budget(con, at)
                print(f"{b.slo.name}, target {b.slo.target:.0%} over "
                      f"{b.slo.window} days ({b.first} to {b.last}): "
                      f"{b.good} good, {b.bad} late; budget "
                      f"{b.slo.budget:g}, left {b.left:g}")
                return 0 if b.left >= 0 else 1
            w = alerts.watch(con, at, a.raw)
            for s in w.sent:
                print(f"{s.at:%Y-%m-%d %H:%M}  {s.kind:<8} {s.to}: "
                      f"{s.text}")
            (still,) = con.execute("SELECT count(*) FROM audit.alerts "
                                   "WHERE resolved_at IS NULL").fetchone()
            print(f"{len(w.opened)} opened, {len(w.repeated)} still "
                  f"open, {len(w.resolved)} resolved")
            return 1 if still else 0
        finally:
            con.close()

    if not a.column:
        p.error(f"{a.command} needs a COLUMN")
    from groundwork.observe import lineage
    cols, tables, manifest = graphs(a.warehouse)
    if a.command == "lineage":
        found = (cols.downstream(a.column, ("value",)) if a.down
                 else cols.upstream(a.column, ("value",)))
        for c in sorted(found):
            print(c)
        return 0
    i = lineage.impact(cols, tables, a.column, manifest)
    for rel, names in i["by_relation"].items():
        print(f"{rel}: {', '.join(names)}")
    print(f"{len(i['columns'])} columns in {len(i['relations'])} "
          f"relations; {len(i['tests'])} tests read them")
    for k, v in i["owners"].items():
        print(f"{k}: {v}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
