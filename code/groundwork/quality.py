"""
Checks on the loads rather than the rows (Chapter 15).

dbt's tests ask whether the tables say something true. These ask
whether the tables can have heard everything: did each source's load
run for the day it should have (freshness), did it bring about as many
rows as it usually does (volume), and is every partition the load log
says was written on disk (partitions). Their evidence is the load log
(Chapter 6) and raw's files (Chapter 5), not the marts, so they can run
before anything is built from the day's loads.

    RULES                   what each source owes, and who hears
    loads(con), last(...)   the load log, read without writing
    expected(rule, at)      the newest day that should be in by `at`
    freshness(con, at)      one Check per rule
    volume(con, at)         one Check per rule that has a volume
    partitions(con, root)   one Check per table the log has written
    anomalies(series)       the robust band behind volume checks
    check(con, at, root)    all of them
    record(con, checks, at) audit.quality_checks

    python -m groundwork.quality [--at T] [--warehouse W] [--raw R]

exits 1 if any check at severity error fails. Nothing here reads the
wall clock: `at` is the moment the checks pretend to run.
"""
from __future__ import annotations

import argparse
import json
import statistics
import sys
import textwrap
from collections import defaultdict
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path

import duckdb

from groundwork import loadlog, storage
from groundwork.config import NOW, RAW, WAREHOUSE

RUN_HOUR = 6
"""The run for day D starts at 06:00 UTC on D+1 (DECISIONS §1)."""

DUE = timedelta(hours=1)
"""How long after it starts the run is given before it counts as late."""


@dataclass(frozen=True)
class Rule:
    """What a source table owes the warehouse, and who hears if not."""
    source: str
    table: str
    cadence: str            # daily: a load per day; monthly: a period
    severity: str           # if it is late: error stops the night
    owner: str              # who is told, and acts
    contact: str            # who runs the source, if it is theirs
    volume: str = ""        # if a day's rows look wrong: warn or error
    min_rows: int = 0       # fewer than this in a day is always wrong


RULES = [
    Rule("tradeline", "orders", "daily", "error", "data engineering",
         "the orders team", volume="warn", min_rows=1),
    Rule("tradeline", "order_lines", "daily", "error",
         "data engineering", "the orders team", volume="warn",
         min_rows=1),
    Rule("relay", "accounts", "daily", "warn", "data engineering",
         "sales operations", volume="error", min_rows=1),
    Rule("relay", "contracts", "daily", "warn", "data engineering",
         "sales operations", volume="error", min_rows=1),
    Rule("frontline", "tickets", "daily", "warn", "data engineering",
         "customer support"),
    Rule("suppliers", "price_lists", "daily", "warn",
         "data engineering", "procurement"),
    Rule("catalogue", "products", "daily", "warn", "data engineering",
         "merchandising"),
    Rule("ledger", "gl_revenue", "monthly", "warn", "finance",
         "finance"),
]


@dataclass(frozen=True)
class Check:
    """One verdict: what was checked, how it went, and who hears."""
    kind: str               # freshness, volume or partitions
    source: str
    table: str
    status: str             # pass, warn, error or skip
    detail: str
    owner: str = "data engineering"

    @property
    def name(self) -> str:
        return f"{self.source}.{self.table}"


# -------------------------------------------------------------- the log
def loads(con, source: str | None = None, table: str | None = None
          ) -> list[loadlog.Load]:
    """
    The load log, oldest first, read without creating anything, so the
    checks can run on a warehouse opened read-only. loadlog.history()
    makes the table first if it is missing, which needs write access.
    """
    sql = (f"SELECT {', '.join(loadlog.COLUMNS)} FROM audit.load_log "
           "WHERE (? IS NULL OR source = ?) "
           "AND (? IS NULL OR table_name = ?) "
           "ORDER BY started_at, load_date, source, table_name, load_id")
    try:
        rows = con.execute(sql, [source, source, table, table]).fetchall()
    except duckdb.CatalogException:
        return []
    out = []
    for row in rows:
        v = dict(zip(loadlog.COLUMNS, row))
        v["extra"] = json.loads(v["extra"] or "{}")
        out.append(loadlog.Load(**v))
    return out


def last(con, source: str, table: str) -> loadlog.Load | None:
    """The newest load that succeeded: by load date, then attempt."""
    good = [x for x in loads(con, source, table)
            if x.status == "succeeded"]
    return max(good, key=lambda x: (x.load_date, int(
        x.load_id.rsplit(".", 1)[1])), default=None)


# -------------------------------------------------------------- freshness
def fifth_working_day(month: date) -> date:
    """When the ledger closes the month before `month`: its 5th weekday."""
    d, n = month.replace(day=1), 0
    while True:
        if d.weekday() < 5:
            n += 1
            if n == 5:
                return d
        d += timedelta(days=1)


def _months_back(d: date, n: int) -> date:
    k = d.year * 12 + d.month - 1 - n
    return date(k // 12, k % 12 + 1, 1)


def expected(rule: Rule, at: datetime) -> date:
    """
    The newest day (daily) or month (monthly) that should be in by
    `at`. A day D is due when its run has had its hour: 07:00 UTC on
    D+1. A month is due on the fifth working day of the next.
    """
    if rule.cadence == "daily":
        return (at - timedelta(hours=RUN_HOUR) - DUE).date() \
            - timedelta(days=1)
    month = at.date().replace(day=1)
    closed = _months_back(month, 1)
    if at.date() < fifth_working_day(month):
        closed = _months_back(month, 2)
    return closed


def _newest_period(con) -> date | None:
    try:
        (p,) = con.execute("SELECT max(period) FROM raw.ledger_gl_revenue"
                           ).fetchone()
    except duckdb.CatalogException:
        return None
    return None if p is None else date.fromisoformat(p + "-01")


def freshness(con, at: datetime) -> list[Check]:
    """Has each source's newest due day (or month) arrived?"""
    out = []
    for r in RULES:
        due = expected(r, at)
        if r.cadence == "monthly":
            have = _newest_period(con)
            ok = have is not None and have >= due
            detail = (f"{have:%Y-%m}, due {due:%Y-%m}" if have
                      else f"no month, due {due:%Y-%m}")
        else:
            good = last(con, r.source, r.table)
            have = good.load_date if good else None
            ok = have is not None and have >= due
            detail = (f"{have}, due {due}" if have
                      else f"none, due {due}")
            if not ok:
                tried = [x for x in loads(con, r.source, r.table)
                         if x.load_date > (have or date.min)]
                if tried:
                    newest = tried[-1]
                    why = newest.extra.get("error", "")
                    why = why.splitlines()[0][:60] if why else ""
                    detail += f"; {newest.load_id} {newest.status}"
                    detail += f": {why}" if why else ""
                else:
                    detail += "; no load ran"
        out.append(Check("freshness", r.source, r.table,
                         "pass" if ok else r.severity, detail, r.owner))
    return out


# ------------------------------------------------------------- partitions
def _folder(load: loadlog.Load) -> str:
    """The raw table a load wrote: a cdc night writes the table's log."""
    if load.extra.get("mode") == "cdc" and not load.extra.get("snapshot"):
        return f"{load.table_name}_log"
    return load.table_name


def partitions(con, root: Path | None = None) -> list[Check]:
    """Every load the log says wrote rows has its partition on disk."""
    wrote = defaultdict(set)
    for x in loads(con):
        if x.status == "succeeded" and (x.rows_written or 0) > 0:
            wrote[(x.source, _folder(x))].add(x.load_date)
    out = []
    for (source, table), days in sorted(wrote.items()):
        on_disk = set(storage.partitions(source, table, root))
        missing = sorted(days - on_disk)
        detail = (f"{len(days)} of {len(days)} on disk" if not missing
                  else f"missing {', '.join(str(d) for d in missing[:2])}"
                  + (" ..." if len(missing) > 2 else ""))
        out.append(Check("partitions", source, table,
                         "error" if missing else "pass", detail))
    return out


# ----------------------------------------------------------------- volume
@dataclass(frozen=True)
class Point:
    """One day of a series, and the band the days before it drew."""
    day: date
    value: float
    median: float
    low: float
    high: float

    @property
    def alarm(self) -> bool:
        return not self.low <= self.value <= self.high


def anomalies(series, window: int = 28, k: float = 4.0,
              floor: float = 0.05, history: int = 14) -> list[Point]:
    """
    Each day of `series` ([(day, value)], oldest first) against the
    days before it: the median of the last `window` values, plus or
    minus k times their median absolute deviation, scaled to read like
    a standard deviation (x 1.4826), and never narrower than `floor`
    times the median. A day with fewer than `history` days before it is
    not judged. The median and the MAD are what the days usually look
    like; an outlier in the window moves them very little, so one bad
    night does not widen the band for the four weeks after it.
    """
    out = []
    values = [v for _, v in series]
    for i, (day, v) in enumerate(series):
        if i < history:
            continue
        past = values[max(0, i - window):i]
        mid = statistics.median(past)
        mad = statistics.median(abs(x - mid) for x in past) * 1.4826
        half = max(k * mad, floor * abs(mid))
        out.append(Point(day, v, mid, mid - half, mid + half))
    return out


def daily_rows(con, source: str, table: str
               ) -> list[tuple[date, int, str]]:
    """
    What each day's loads left in raw: the rows the newest succeeded
    attempt that wrote anything wrote (a rerun replaces the partition
    whole, so earlier attempts no longer count), and whether that was
    a whole snapshot or the day's own rows. [(day, rows, kind)].
    """
    days = {}
    for x in loads(con, source, table):
        if x.status != "succeeded":
            continue
        kind = "snapshot" if x.extra.get("snapshot") else "rows"
        if (x.rows_written or 0) > 0 or x.load_date not in days:
            days[x.load_date] = (x.rows_written or 0, kind)
    return [(d, n, k) for d, (n, k) in sorted(days.items())]


def volume(con, at: datetime, **band) -> list[Check]:
    """
    Did the newest due day bring about as many rows as usual? A day
    is judged against earlier days of the same kind only: a snapshot
    of the whole table is not a night's changes.
    """
    out = []
    for r in (r for r in RULES if r.volume):
        due = expected(r, at)
        days = [d for d in daily_rows(con, r.source, r.table)
                if d[0] <= due]
        if not days or days[-1][0] != due:
            out.append(Check("volume", r.source, r.table, "skip",
                             f"no load for {due}", r.owner))
            continue
        n, kind = days[-1][1], days[-1][2]
        if n < r.min_rows:
            out.append(Check("volume", r.source, r.table, "error",
                             f"{n:,} rows for {due}", r.owner))
            continue
        series = [(d, m) for d, m, k in days if k == kind]
        points = anomalies(series, **band)
        if not points or points[-1].day != due:
            before = len(series) - 1
            out.append(Check("volume", r.source, r.table, "skip",
                             f"{n:,}; " + (f"{before} like it before"
                                           if before else
                                           "the first of its kind"),
                             r.owner))
            continue
        p = points[-1]
        detail = f"{n:,}; band {p.low:,.0f}-{p.high:,.0f}"
        out.append(Check("volume", r.source, r.table,
                         r.volume if p.alarm else "pass", detail, r.owner))
    return out


# ------------------------------------------------------------------ all
def check(con, at: datetime = NOW, root: Path | None = None
          ) -> list[Check]:
    """Freshness, then volume, then partitions."""
    at = at.replace(tzinfo=None)
    return freshness(con, at) + volume(con, at) + partitions(con, root)


DDL = """
CREATE SCHEMA IF NOT EXISTS audit;
CREATE TABLE IF NOT EXISTS audit.quality_checks (
    run_at      TIMESTAMP NOT NULL,
    kind        VARCHAR NOT NULL,
    source      VARCHAR NOT NULL,
    table_name  VARCHAR NOT NULL,
    status      VARCHAR NOT NULL
                CHECK (status IN ('pass', 'warn', 'error', 'skip')),
    owner       VARCHAR NOT NULL,
    detail      VARCHAR,
    PRIMARY KEY (run_at, kind, source, table_name)
);
"""


def record(con, checks: list[Check], at: datetime) -> int:
    """Write one run's verdicts to audit.quality_checks."""
    con.execute(DDL)
    at = at.replace(tzinfo=None)
    con.execute("DELETE FROM audit.quality_checks WHERE run_at = ?", [at])
    con.executemany("INSERT INTO audit.quality_checks VALUES "
                    "(?, ?, ?, ?, ?, ?, ?)",
                    [(at, c.kind, c.source, c.table, c.status, c.owner,
                      c.detail) for c in checks])
    return len(checks)


def report(checks: list[Check], width: int = 68) -> list[str]:
    """One line a check (two if the detail is long), `width` columns."""
    lines = []
    for c in checks:
        head = f"{c.kind:<10} {c.name:<24} {c.status:<5} "
        if len(head) + len(c.detail) <= width:
            lines.append(head + c.detail)
        else:
            lines.append(head.rstrip())
            lines += ["    " + t for t in textwrap.wrap(c.detail,
                                                       width - 4)]
    return lines


def main(argv=None) -> int:
    p = argparse.ArgumentParser(prog="python -m groundwork.quality",
                                description=__doc__.split("\n\n")[0])
    p.add_argument("--warehouse", type=Path, default=WAREHOUSE)
    p.add_argument("--raw", type=Path, default=RAW)
    p.add_argument("--at", default=None, help="the moment (UTC) the "
                   "checks run as; default the book's NOW")
    p.add_argument("--no-record", action="store_true")
    a = p.parse_args(argv)
    at = datetime.fromisoformat(a.at) if a.at else NOW
    con = duckdb.connect(str(a.warehouse), read_only=a.no_record)
    checks = check(con, at, a.raw)
    print("\n".join(report(checks)))
    if not a.no_record:
        record(con, checks, at)
    con.close()
    errors = [c for c in checks if c.status == "error"]
    warns = [c for c in checks if c.status == "warn"]
    print(f"{len(checks)} checks: {len(errors)} errors, "
          f"{len(warns)} warnings")
    return 1 if errors else 0


if __name__ == "__main__":
    sys.exit(main())
