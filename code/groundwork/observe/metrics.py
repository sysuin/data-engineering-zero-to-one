"""
What each night did, measured from the records it left: the four
signals, and what was held back.

Everything here reads the audit tables and raw's partitions. Nothing
reads the marts, so a night can be measured whether or not its build
ran, and any night can be measured again later, as it stood at any
moment, which is what lets a replay of July 2024 be read like a live
month.

    freshness     is the newest day each source owes there, and is the
                  newest published day the one that is due?
    volume        did each load bring about as many rows as usual?
    schema        did a batch break its contract, or arrive in a shape
                  its sender had not used before?
    distribution  is the make-up of a day's rows like the days before:
                  a share, a median, a count of empty values?

    Signal                       one measurement and its verdict
    collect(con, day, at, root)  every signal for one logical day
    record(con, signals, ...)    audit.metrics
    held(con, day, at)           rows contracts kept out of raw
    run_record(con, run_id)      what each task of a run said
    loads_during(con, ...)       the loads a task may have made
    stream_lag(log_path, group)  a stream's freshness (Chapter 19)

`at` is the moment a night is judged as of, by default 07:00 UTC on
the day after, as Chapter 15's checks are. Nothing reads the clock.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, time, timedelta
from pathlib import Path

import duckdb

from groundwork import quality
from groundwork.config import RAW


def judged_at(day: date) -> datetime:
    """When a logical day falls due: 07:00 UTC on the next."""
    return datetime.combine(day + timedelta(days=1),
                            time(quality.RUN_HOUR)) + quality.DUE


@dataclass(frozen=True)
class Signal:
    """One measurement of one night, and how it compares."""
    signal: str             # freshness, volume, schema, distribution
    subject: str            # source.table, or "published"
    measure: str            # what was measured, in words
    value: float | None
    low: float | None       # the band, or the limit, it is held to
    high: float | None
    status: str             # ok, alarm or skip
    detail: str = ""
    severity: str = "warn"  # an alarm at error stops publish

    @property
    def alarm(self) -> bool:
        return self.status == "alarm"


# ------------------------------------------------------------ freshness
def _loads(con, at: datetime):
    """The load log as it stood at `at`: loads started by then."""
    return [x for x in quality.loads(con) if x.started_at <= at]


def freshness(con, day: date, at: datetime) -> list[Signal]:
    """Days behind, per daily source and for the published day."""
    out = []
    logs = _loads(con, at)
    for r in (r for r in quality.RULES if r.cadence == "daily"):
        good = [x.load_date for x in logs if x.status == "succeeded"
                and (x.source, x.table_name) == (r.source, r.table)]
        if not any((x.source, x.table_name) == (r.source, r.table)
                   for x in logs):
            continue                      # not a source this warehouse has
        have = max(good, default=None)
        behind = (day - have).days if have else None
        ok = behind is not None and behind <= 0
        out.append(Signal("freshness", f"{r.source}.{r.table}",
                          "days behind", behind, 0, 0,
                          "ok" if ok else "alarm",
                          f"newest {have}" if have else "nothing loaded",
                          r.severity))
    have = published(con, at)
    behind = (day - have).days if have else None
    out.append(Signal("freshness", "published", "days behind", behind,
                      0, 0, "ok" if behind is not None and behind <= 0
                      else "alarm", f"newest {have}" if have else
                      "nothing published", "error"))
    return out


def published(con, at: datetime, dag: str = "nightly") -> date | None:
    """The newest logical day a run had published by `at`."""
    try:
        (d,) = con.execute("""
            SELECT max(logical_date) FROM audit.task_runs
            WHERE dag = ? AND task = 'publish' AND state = 'succeeded'
              AND finished_at <= ?""", [dag, at]).fetchone()
    except duckdb.CatalogException:
        return None
    return d


# --------------------------------------------------------------- volume
def volume(con, day: date, at: datetime) -> list[Signal]:
    """The rows each load for `day` brought, against earlier days of
    the same kind, in Chapter 15's band."""
    out = []
    for r in (r for r in quality.RULES if r.volume):
        days = [d for d in quality.daily_rows(con, r.source, r.table)
                if d[0] <= day]
        name = f"{r.source}.{r.table}"
        if not days or days[-1][0] != day:
            if days:
                out.append(Signal("volume", name, "rows", None, None,
                                  None, "skip", f"no load for {day}",
                                  r.volume))
            continue
        n, kind = days[-1][1], days[-1][2]
        series = [(d, m) for d, m, k in days if k == kind]
        points = quality.anomalies(series)
        if n < r.min_rows:
            out.append(Signal("volume", name, "rows", n, r.min_rows, None,
                              "alarm", "nothing loaded", r.volume))
        elif not points or points[-1].day != day:
            out.append(Signal("volume", name, "rows", n, None, None,
                              "skip", f"{len(series) - 1} like it "
                              "before", r.volume))
        else:
            p = points[-1]
            out.append(Signal("volume", name, "rows", n, p.low, p.high,
                              "alarm" if p.alarm else "ok",
                              f"usually {p.median:,.0f}", r.volume))
    return out


# --------------------------------------------------------------- schema
def _batches(con, day: date, at: datetime):
    """The contract checks of `day`'s loads, made by `at`."""
    try:
        return con.execute("""
            SELECT c.source, c.table_name, c.name, c.format, c.status,
                   c.rows, c.parked, c.detail, c.ref
            FROM audit.contract_runs c
            JOIN audit.load_log l USING (load_id)
            WHERE l.load_date = ? AND c.checked_at <= ?
            ORDER BY c.source, c.table_name, c.name""",
                           [day, at]).fetchall()
    except duckdb.CatalogException:
        return []


def _sender(name: str | None) -> str | None:
    """The folder a file came from: one sender's files."""
    return name.rsplit("/", 1)[0] if name and "/" in name else None


def schema(con, day: date, at: datetime) -> list[Signal]:
    """
    Per table: batches whose shape broke the contract, and batches in
    a format the sender had never used before (allowed or not). The
    first is stopped at the door; the second passes, and is worth
    knowing about on the night it happens.
    """
    seen = {}
    try:
        for name, fmt in con.execute("""
                SELECT c.name, c.format FROM audit.contract_runs c
                JOIN audit.load_log l USING (load_id)
                WHERE l.load_date < ? AND c.status <> 'broken'
                  AND c.format IS NOT NULL""", [day]).fetchall():
            seen.setdefault(_sender(name), set()).add(fmt)
    except duckdb.CatalogException:
        return []
    per = {}
    for source, table, name, fmt, status, *_ , detail, _ref in \
            _batches(con, day, at):
        key = f"{source}.{table}"
        broke, new = per.setdefault(key, ([], []))
        if status == "broken":
            broke.append(f"{name.rsplit('/', 1)[-1]}: {detail}")
        elif fmt and _sender(name) in seen and \
                fmt not in seen[_sender(name)]:
            new.append(f"{name.rsplit('/', 1)[-1]}: new format {fmt}")
    out = []
    for key, (broke, new) in sorted(per.items()):
        n = len(broke) + len(new)
        out.append(Signal("schema", key, "batches changed", n, 0, 0,
                          "alarm" if n else "ok",
                          "; ".join(broke + new),
                          "error" if broke else "warn"))
    return out


# --------------------------------------------------------- distribution
@dataclass(frozen=True)
class Share:
    """A number that describes a day's rows, held to its own past (a
    band) or to a fixed limit."""
    source: str
    table: str              # raw's folder for the table
    measure: str
    expr: str               # SQL over the partition's rows
    where: str = "true"
    limit: float | None = None
    severity: str = "warn"


DISTRIBUTIONS = [
    Share("tradeline", "orders_log", "share cancelled",
          "avg(CASE WHEN status = 'cancelled' THEN 1 ELSE 0 END)"),
    Share("tradeline", "order_lines_log", "median price",
          "median(unit_price_cents)", where="op = 'I'"),
    Share("relay", "accounts", "share closed",
          "avg(CASE WHEN lower(status) = 'closed' THEN 1 ELSE 0 END)"),
    Share("suppliers", "price_lists", "share no cost",
          "avg(CASE WHEN nullif(trim(cost), '') IS NULL THEN 1 "
          "ELSE 0 END)", limit=0.0, severity="error"),
]


def _series(share: Share, root: Path, upto: date
            ) -> list[tuple[date, float]]:
    folder = Path(root) / share.source / share.table
    if not folder.is_dir() or not any(folder.glob("load_date=*/*.parquet")):
        return []
    con = duckdb.connect()
    try:
        rows = con.execute(f"""
            SELECT load_date, {share.expr}
            FROM read_parquet('{folder}/load_date=*/*.parquet',
                              hive_partitioning = true,
                              union_by_name = true)
            WHERE {share.where} AND load_date <= DATE '{upto}'
            GROUP BY load_date HAVING count(*) > 0
            ORDER BY load_date""").fetchall()
    finally:
        con.close()
    return [(d, float(v)) for d, v in rows if v is not None]


def distribution(day: date, root: Path | None = None) -> list[Signal]:
    """Each Share for `day`, if the day brought rows to measure."""
    out = []
    for s in DISTRIBUTIONS:
        series = _series(s, root or RAW, day)
        name = f"{s.source}.{s.table}"
        if not series or series[-1][0] != day:
            continue
        v = series[-1][1]
        if s.limit is not None:
            out.append(Signal("distribution", name, s.measure, v, None,
                              s.limit, "alarm" if v > s.limit else "ok",
                              f"limit {s.limit:g}", s.severity))
            continue
        points = quality.anomalies(series)
        if not points or points[-1].day != day:
            out.append(Signal("distribution", name, s.measure, v, None,
                              None, "skip", f"{len(series) - 1} days "
                              "before", s.severity))
            continue
        p = points[-1]
        out.append(Signal("distribution", name, s.measure, v, p.low,
                          p.high, "alarm" if p.alarm else "ok",
                          f"usually {p.median:.4g}", s.severity))
    return out


def collect(con, day, at: datetime | None = None,
            root: Path | None = None) -> list[Signal]:
    """The four signals for logical day `day`, as of `at`."""
    day = date.fromisoformat(str(day))
    at = at or judged_at(day)
    return (freshness(con, day, at) + volume(con, day, at)
            + schema(con, day, at) + distribution(day, root))


DDL = """
CREATE SCHEMA IF NOT EXISTS audit;
CREATE TABLE IF NOT EXISTS audit.metrics (
    logical_date DATE NOT NULL,
    measured_at  TIMESTAMP NOT NULL,
    signal       VARCHAR NOT NULL,
    subject      VARCHAR NOT NULL,
    measure      VARCHAR NOT NULL,
    value        DOUBLE,
    low          DOUBLE,
    high         DOUBLE,
    status       VARCHAR NOT NULL
                 CHECK (status IN ('ok', 'alarm', 'skip')),
    severity     VARCHAR NOT NULL,
    detail       VARCHAR,
    PRIMARY KEY (logical_date, signal, subject, measure)
);
"""


def record(con, signals: list[Signal], day, at: datetime) -> int:
    """One night's signals into audit.metrics; a second measurement of
    the same night replaces the first."""
    con.execute(DDL)
    con.execute("DELETE FROM audit.metrics WHERE logical_date = ?", [day])
    con.executemany(
        "INSERT INTO audit.metrics VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        [(day, at, s.signal, s.subject, s.measure, s.value, s.low, s.high,
          s.status, s.severity, s.detail) for s in signals])
    return len(signals)


# ------------------------------------------------- what was held back
@dataclass(frozen=True)
class Held:
    """Rows a table's contract kept out of raw on one night."""
    source: str
    table: str
    rows: int               # rows in the night's batches
    parked: int             # rows put in audit.quarantine
    broken: int             # rows in batches that broke the contract
    files: tuple = ()       # the broken batches' names
    refs: tuple = ()        # ... and their fingerprints

    @property
    def name(self) -> str:
        return f"{self.source}.{self.table}"

    @property
    def share(self) -> float:
        return (self.parked + self.broken) / self.rows if self.rows else 0


def held(con, day, at: datetime | None = None) -> list[Held]:
    """
    The quarantine's volume for logical day `day`: per table, the rows
    parked and the rows in broken batches, with the night's total.
    A failed file is tried again every night, so the same file can be
    held on many nights; `refs` says which bytes it was.
    """
    day = date.fromisoformat(str(day))
    at = at or judged_at(day)
    per: dict = {}
    for source, table, name, _f, status, rows, parked, _d, ref in \
            _batches(con, day, at):
        h = per.setdefault((source, table), [0, 0, 0, [], []])
        h[0] += rows
        h[1] += parked
        if status == "broken":
            h[2] += rows
            h[3].append(name.rsplit("/", 1)[-1] if name else ref)
            h[4].append(ref)
    return [Held(s, t, r, p, b, tuple(f), tuple(x))
            for (s, t), (r, p, b, f, x) in sorted(per.items())]


# --------------------------------------------- what a run left behind
@dataclass(frozen=True)
class TaskRecord:
    task: str
    state: str
    tries: int
    started: datetime | None
    seconds: float | None   # from its first try's start to its end
    detail: str


def run_record(con, run_id: str) -> list[TaskRecord]:
    """For each task of one run: its last state, how many tries, when
    it started, how long all of it took, and what it said last."""
    rows = con.execute("""
        SELECT task, arg_max(state, try_number), max(try_number),
               min(started_at) FILTER (try_number > 0),
               max(finished_at), arg_max(detail, try_number)
        FROM audit.task_runs WHERE run_id = ?
        GROUP BY task ORDER BY min(started_at), task""",
                       [run_id]).fetchall()
    return [TaskRecord(task, state, tries, start,
                       (end - start).total_seconds() if start and end
                       else None, detail or "")
            for task, state, tries, start, end, detail in rows]


def loads_during(con, source: str, start: datetime, end: datetime,
                 day=None) -> list[str]:
    """The load ids of `source` that started between two moments: all
    a run can be linked to its loads by, since a load records its day
    and its moment but not the run that made it."""
    return [r[0] for r in con.execute("""
        SELECT load_id FROM audit.load_log WHERE source = ?
          AND started_at BETWEEN ? AND ?
          AND (? IS NULL OR load_date = ?)
        ORDER BY started_at, load_id""",
                                      [source, start, end, day, day]
                                      ).fetchall()]


# ------------------------------------------------------------- streams
def stream_lag(log_path, group: str) -> dict:
    """
    A stream's freshness: how many records its log holds beyond the
    group's committed offset (Chapter 19), and the portal's own time
    on the oldest record the group has not read.
    """
    from groundwork.stream.consumer import Consumer, parse
    from groundwork.stream.log import Log
    log = Log(log_path)
    try:
        c = Consumer(log, group)
        lag = c.lag
        oldest = None
        if lag:
            (_, value), = log.read(c.committed(), 1)
            oldest = parse(value).get("received_at")
        return {"records": lag, "end": log.end_offset,
                "committed": c.committed(), "oldest_unread": oldest}
    finally:
        log.close()
