"""
Publish what Groundwork serves from tables: the metrics schema (the
wide sales view, the definitions, a monthly view of every metric) and
features.renewal_snapshot, checked. The API reads the warehouse
directly and needs nothing published.

    python -m groundwork.serve [--warehouse PATH] [--today YYYY-MM-DD]

Exits 1 if the feature table breaks a promise.
"""
from __future__ import annotations

import argparse
import sys
from datetime import date
from pathlib import Path

import duckdb

from groundwork.config import TODAY, WAREHOUSE
from groundwork.serve import features, metrics


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(prog="python -m groundwork.serve")
    ap.add_argument("--warehouse", type=Path, default=WAREHOUSE)
    ap.add_argument("--today", type=date.fromisoformat, default=TODAY)
    args = ap.parse_args(argv)
    with duckdb.connect(str(args.warehouse)) as con:
        n = metrics.publish(con)
        print(f"metrics: {n} definitions, {n} monthly views, "
              "metrics.sales")
        rows = features.build(con, today=args.today)
        problems = features.check(con)
        print(f"features.renewal_snapshot: {rows:,} rows")
        for p in problems:
            print(f"  {p}")
        print("every promise kept" if not problems else
              f"{len(problems)} problems")
    return 1 if problems else 0


if __name__ == "__main__":
    sys.exit(main())
