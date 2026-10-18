"""
The load log: one row for every attempt to load one table for one day.

A load that leaves no record can only be judged by looking at what it
wrote, which is the one thing a broken load gets wrong. So every
loader in Groundwork opens a row here before it touches raw, and
closes it when it is done, saying how it went:

    audit.load_log
      load_id       <source>.<table>.<load_date>.<attempt>
      source        e.g. tradeline
      table_name    e.g. orders
      load_date     the logical date the load is for (a DATE)
      started_at    when the run began (UTC, no zone)
      finished_at   when it ended; NULL while it is still running
      status        running, succeeded or failed
      rows_read     rows the loader took from the source
      rows_written  rows that landed, counted from what was written
      source_count  what the source said it held, asked separately
      extra         JSON text: anything else a loader wants to keep
                    (bytes, a watermark, the error that stopped it)

A row that stays `running` is a load that died without saying so:
the process was killed, or the machine went away. Nothing else in
Groundwork would notice.

Moments come from the run, never from the wall clock: the loader
passes `started_at` (the run's moment) and `finished_at`.

The API, which every loader from Chapter 6 on uses:

    ensure(con)                               create the table
    start(con, source, table, load_date, started_at=...) -> load_id
    finish(con, load_id, finished_at=..., rows_read=...,
           rows_written=..., source_count=..., extra=...)
    fail(con, load_id, finished_at=..., error=..., extra=...)
    last(con, source, table)                  the latest good Load
    history(con, source=None, table=None)     every Load, in order
"""
from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import date, datetime, timezone

STATUSES = ("running", "succeeded", "failed")

DDL = """
CREATE SCHEMA IF NOT EXISTS audit;
CREATE TABLE IF NOT EXISTS audit.load_log (
    load_id      VARCHAR PRIMARY KEY,
    source       VARCHAR NOT NULL,
    table_name   VARCHAR NOT NULL,
    load_date    DATE NOT NULL,
    started_at   TIMESTAMP NOT NULL,
    finished_at  TIMESTAMP,
    status       VARCHAR NOT NULL
                 CHECK (status IN ('running', 'succeeded', 'failed')),
    rows_read    BIGINT,
    rows_written BIGINT,
    source_count BIGINT,
    extra        VARCHAR
);
"""
COLUMNS = ("load_id", "source", "table_name", "load_date", "started_at",
           "finished_at", "status", "rows_read", "rows_written",
           "source_count", "extra")


@dataclass(frozen=True)
class Load:
    """One row of audit.load_log, with `extra` read back as a dict."""
    load_id: str
    source: str
    table_name: str
    load_date: date
    started_at: datetime
    finished_at: datetime | None
    status: str
    rows_read: int | None
    rows_written: int | None
    source_count: int | None
    extra: dict = field(default_factory=dict)

    @property
    def seconds(self) -> float | None:
        """How long the load took, or None if it never finished."""
        if self.finished_at is None:
            return None
        return (self.finished_at - self.started_at).total_seconds()

    @property
    def reconciled(self) -> bool:
        """True when the rows read, written and counted all agree."""
        n = {self.rows_read, self.rows_written, self.source_count}
        return len(n) == 1 and None not in n


def _utc(moment: datetime) -> datetime:
    """Stored moments are UTC with no zone; naive input is UTC."""
    if not isinstance(moment, datetime):
        raise TypeError("a moment must be a datetime")
    if moment.tzinfo is not None:
        moment = moment.astimezone(timezone.utc).replace(tzinfo=None)
    return moment


def _day(load_date) -> date:
    if isinstance(load_date, datetime):
        raise TypeError("load_date is a calendar date, not a moment")
    if isinstance(load_date, date):
        return load_date
    return date.fromisoformat(str(load_date))


def ensure(con) -> None:
    """Create audit.load_log if it is not there. Safe to run twice."""
    con.execute(DDL)


def start(con, source: str, table: str, load_date, *,
          started_at: datetime) -> str:
    """
    Record that a load has begun, and return its load_id.

    The id counts attempts: the second load of the same table for the
    same day is `...2`, so a rerun never hides the run before it.
    """
    ensure(con)
    day = _day(load_date)
    (n,) = con.execute(
        "SELECT count(*) FROM audit.load_log WHERE source = ? "
        "AND table_name = ? AND load_date = ?",
        [source, table, day]).fetchone()
    load_id = f"{source}.{table}.{day.isoformat()}.{n + 1}"
    con.execute(
        "INSERT INTO audit.load_log (load_id, source, table_name, "
        "load_date, started_at, status) VALUES (?, ?, ?, ?, ?, "
        "'running')", [load_id, source, table, day, _utc(started_at)])
    return load_id


def _close(con, load_id: str, status: str, finished_at: datetime,
           counts: dict, extra: dict | None) -> None:
    (state,) = con.execute(
        "SELECT status FROM audit.load_log WHERE load_id = ?",
        [load_id]).fetchone() or (None,)
    if state is None:
        raise KeyError(f"no load {load_id!r} in the log")
    if state != "running":
        raise ValueError(f"load {load_id!r} is already {state}")
    con.execute(
        "UPDATE audit.load_log SET finished_at = ?, status = ?, "
        "rows_read = ?, rows_written = ?, source_count = ?, "
        "extra = ? WHERE load_id = ?",
        [_utc(finished_at), status, counts.get("rows_read"),
         counts.get("rows_written"), counts.get("source_count"),
         json.dumps(extra or {}, sort_keys=True, default=str),
         load_id])


def finish(con, load_id: str, *, finished_at: datetime,
           rows_read: int | None = None,
           rows_written: int | None = None,
           source_count: int | None = None,
           extra: dict | None = None) -> None:
    """Record that a load succeeded, and what it counted."""
    _close(con, load_id, "succeeded", finished_at,
           {"rows_read": rows_read, "rows_written": rows_written,
            "source_count": source_count}, extra)


def fail(con, load_id: str, *, finished_at: datetime, error: str,
         extra: dict | None = None, **counts) -> None:
    """Record that a load failed, and why. Counts known so far may be
    passed as rows_read=, rows_written=, source_count=."""
    known = {"rows_read", "rows_written", "source_count"}
    unknown = set(counts) - known
    if unknown:
        raise TypeError(f"unexpected arguments: {sorted(unknown)}")
    _close(con, load_id, "failed", finished_at, counts,
           dict(extra or {}, error=error))


def _load(row) -> Load:
    values = dict(zip(COLUMNS, row))
    values["extra"] = json.loads(values["extra"] or "{}")
    return Load(**values)


def history(con, source: str | None = None,
            table: str | None = None) -> list[Load]:
    """Every load in the log, oldest first, optionally for one table."""
    ensure(con)
    where, params = [], []
    if source is not None:
        where.append("source = ?")
        params.append(source)
    if table is not None:
        where.append("table_name = ?")
        params.append(table)
    sql = f"SELECT {', '.join(COLUMNS)} FROM audit.load_log"
    if where:
        sql += " WHERE " + " AND ".join(where)
    sql += (" ORDER BY started_at, load_date, source, table_name,"
            " load_id")
    return [_load(r) for r in con.execute(sql, params).fetchall()]


def last(con, source: str, table: str,
         status: str | None = "succeeded") -> Load | None:
    """
    The most recent load of one table: by load date, then by attempt.
    By default only a load that succeeded; status=None for any.
    """
    if status is not None and status not in STATUSES:
        raise ValueError(f"status must be one of {STATUSES}")
    loads = [x for x in history(con, source, table)
             if status is None or x.status == status]
    if not loads:
        return None
    return max(loads, key=lambda x: (x.load_date,
                                     int(x.load_id.rsplit(".", 1)[1])))
