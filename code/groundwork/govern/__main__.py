"""
Governance, in two commands.

    python -m groundwork.govern [--warehouse W] [--raw R] [--landed L]
                                [--today D] [--retain]
        the nightly step, after the transform: classify every column,
        rebuild the masked tables, recreate the role schemas, check
        them, and show (or, with --retain, apply) what retention is due.
        Exits 1 if a role can read what it should not.

    python -m groundwork.govern forget ACCOUNT --request ID
                                [--warehouse W] [--raw R] [--landed L]
                                [--received T] [--at T]
        forget the contact of one account: find, erase, sweep, record.
        Exits 1 if anything that belongs to them alone is left.
"""
from __future__ import annotations

import argparse
import sys
from datetime import date, datetime
from pathlib import Path

import duckdb

from groundwork import storage
from groundwork.config import GROUNDWORK, NOW, RAW, TODAY, WAREHOUSE
from groundwork.govern import classify, forget, masking, retention, roles


def nightly(con, raw, landed, today: date, retain: bool) -> int:
    columns = classify.classify(con)
    classify.save(con, columns)
    n = {c: sum(1 for x in columns if x.cls == c)
         for c in (classify.PERSONAL, classify.CONFIDENTIAL,
                   classify.PUBLIC)}
    print(f"classified {len(columns)} columns: "
          + ", ".join(f"{v} {k}" for k, v in n.items()))
    rows = masking.build(con)
    print(f"masked: {len(rows)} tables, {sum(rows.values()):,} rows")
    made = roles.create(con)
    print("roles: " + ", ".join(f"{k} {v}" for k, v in made.items()))
    problems = roles.check(con)
    for p in problems:
        print(f"  problem: {p}")
    done = retention.run(con, raw, landed, today, dry_run=not retain)
    due = [a for a in done if a.action == "deleted"]
    verb = "removed" if retain else "due (run with --retain)"
    print(f"retention: {len(due)} {verb}, "
          f"{sum(a.bytes for a in due) / 1e6:.1f} MB")
    print("access as written" if not problems else "ACCESS PROBLEMS")
    return 1 if problems else 0


def forget_one(con, raw, landed, account: int, request: str,
               received: datetime, at: datetime) -> int:
    s = forget.subject(con, account)
    done = forget.erase(con, s, raw, landed)
    left = forget.sweep(con, s, raw, landed)
    forget.record(con, request, s, received, at, done, left, "")
    strong = {k for k, _ in s.strong()}
    alone = sum(n for w in left.values() for k, n in w.items()
                if k in strong)
    for p in done:
        print(f"  {p.action:<8}{p.rows:>8,}  {p.location}")
    print(f"{request}: {len(done)} places; left that is theirs alone: "
          f"{alone}")
    return 1 if alone else 0


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(prog="python -m groundwork.govern")
    ap.add_argument("--warehouse", type=Path, default=WAREHOUSE)
    ap.add_argument("--raw", type=Path, default=RAW)
    ap.add_argument("--landed", type=Path, default=GROUNDWORK / "landed")
    sub = ap.add_subparsers(dest="command")
    ap.add_argument("--today", type=date.fromisoformat, default=TODAY)
    ap.add_argument("--retain", action="store_true")
    f = sub.add_parser("forget")
    f.add_argument("account", type=int)
    f.add_argument("--request", required=True)
    f.add_argument("--received", type=datetime.fromisoformat,
                   default=NOW.replace(tzinfo=None))
    f.add_argument("--at", type=datetime.fromisoformat,
                   default=NOW.replace(tzinfo=None))
    args = ap.parse_args(argv)
    con = duckdb.connect(str(args.warehouse))
    con.execute("SET enable_progress_bar = false")
    storage.raw_views(con, root=args.raw)
    try:
        if args.command == "forget":
            return forget_one(con, args.raw, args.landed, args.account,
                              args.request, args.received, args.at)
        return nightly(con, args.raw, args.landed, args.today, args.retain)
    finally:
        con.close()


if __name__ == "__main__":
    sys.exit(main())
