"""
The transformation step of Groundwork's nightly run: raw to marts.

    python -m groundwork.transform [--full] [--at T] [--check]

One command does what Chapters 11 to 13 built, in order:

1.  the runner builds every model that changed: seeds, staging views,
    intermediate models, then the dimensions and facts in marts;
2.  dim_date is filled from the calendar (no source sends it);
3.  every mart is checked against marts.sql and its own promises:
    dimensions.check() and facts.check().

It prints one line per layer and one per mart, and exits 1 if a model
failed or was blocked, or if any promise is broken, so that whatever
runs it next (Chapter 17's orchestrator) can stop.
"""
from __future__ import annotations

import argparse
from collections import Counter
from datetime import datetime

import duckdb

from groundwork import dates, ddl, dimensions, facts
from groundwork.config import NOW, WAREHOUSE
from groundwork.transform import runner

LAYERS = ("seeds", "staging", "intermediate", "marts")


def transform(warehouse=WAREHOUSE, *, run_at: datetime = NOW,
              full: bool = False, check_only: bool = False
              ) -> tuple[list, list[str], dict]:
    """Build, fill dim_date, check: (results, problems, rows)."""
    results = []
    if not check_only:
        results = runner.run(warehouse, run_at=run_at, full=full)
        con = duckdb.connect(str(warehouse))
        try:
            ddl.create(con, "marts")        # IF NOT EXISTS: dim_date
            dates.load(con)
        finally:
            con.close()
    con = duckdb.connect(str(warehouse), read_only=True)
    try:
        problems = dimensions.check(con) + facts.check(con)
        rows = {t: con.execute(f"SELECT count(*) FROM marts.{t}")
                .fetchone()[0]
                for t in dimensions.DIMENSIONS + facts.FACTS}
    finally:
        con.close()
    return results, problems, rows


def main(argv=None) -> int:
    p = argparse.ArgumentParser(description=__doc__.split("\n")[1])
    p.add_argument("--full", action="store_true",
                   help="rebuild every model, changed or not")
    p.add_argument("--at", default=NOW.strftime("%Y-%m-%d %H:%M"),
                   help="the run's moment, UTC (default: config.NOW)")
    p.add_argument("--check", action="store_true",
                   help="check what is built; build nothing")
    a = p.parse_args(argv)
    results, problems, rows = transform(
        run_at=datetime.fromisoformat(a.at), full=a.full,
        check_only=a.check)
    for layer in LAYERS:
        mine = [r for r in results
                if r.relation.split(".")[0] == layer]
        if mine:
            n = Counter(r.status for r in mine)
            print(f"{layer:<13}" + ", ".join(
                f"{n[s]} {s}" for s in ("built", "skipped", "failed",
                                        "blocked") if n[s]))
    for r in results:
        if r.status in ("failed", "blocked"):
            print(f"  {r.status} {r.relation}: {r.error}")
    for table, n in rows.items():
        print(f"marts.{table:<20}{n:>10,} rows")
    for line in problems:
        print("PROBLEM", line)
    bad = [r for r in results if r.status in ("failed", "blocked")]
    print("every promise kept" if not (problems or bad)
          else f"{len(problems) + len(bad)} problems")
    return 1 if problems or bad else 0


if __name__ == "__main__":
    raise SystemExit(main())
