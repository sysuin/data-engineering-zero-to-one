"""
The portal's sessions, every night: Chapter 19's stream, run by the
orchestrator over the hourly files that have arrived.

Chapter 19 replayed the portal's files into a log in its sandbox and
consumed the log there. Groundwork's own log lives beside the
warehouse, in <warehouse folder>/stream/portal-log, and each night:

  1. appends every hourly file that has arrived and is not in the log
     yet (the log's produced.txt says which are)
  2. lets the sessions group read to the end, from its checkpoint,
     writing marts.fct_portal_sessions as sessions close
  3. fills account_key from dim_account, which Chapter 19 had to leave
     empty because the dimension did not exist yet: the version in
     force on the session's day, or the unknown member when none was
  4. deletes the segments the group has finished with

A first night has months of files to catch up on. It appends and reads
them a week at a time, so the log on disk never holds much more than a
week of events and the segment being written.

    run(warehouse, landing, by)       -> Night
    log_path(warehouse)               where the log is
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

import duckdb

from groundwork.ingest import files
from groundwork.stream import sessions
from groundwork.stream.consumer import Consumer
from groundwork.stream.log import Log, append_files

GROUP = "sessions"
CHUNK = 168                 # hourly files appended at a time: a week
BATCH = 5000                # records the sessioniser reads at a time

REGIONS = """
SELECT a.account_id, r.region_id
FROM marts.dim_account AS a
JOIN marts.dim_region AS r USING (region_key)
WHERE a.is_current AND a.account_id > 0
"""

# The version of the account in force on the session's own day.
FILL_KEYS = """
UPDATE marts.fct_portal_sessions AS s SET account_key = a.account_key
FROM marts.dim_account AS a
WHERE s.account_key IS NULL AND a.account_id = s.account_id
  AND strptime(CAST(s.date_key AS VARCHAR), '%Y%m%d')::DATE
      >= a.valid_from
  AND strptime(CAST(s.date_key AS VARCHAR), '%Y%m%d')::DATE
      < coalesce(a.valid_to, DATE '9999-12-31')
"""

# A session on a day no version of its account was in force (the
# portal saw some companies before the CRM made them customers) points
# at the unknown member, as an early fact does. account_id keeps who.
FILL_UNKNOWN = """
UPDATE marts.fct_portal_sessions SET account_key = (
    SELECT account_key FROM marts.dim_account WHERE account_id = -1)
WHERE account_key IS NULL
"""


@dataclass
class Night:
    files: int          # hourly files appended tonight
    records: int        # events they held
    read: int           # records the group read tonight
    written: int        # session rows written or rewritten
    open: int           # sessions still open: may yet change
    rows: int           # rows in fct_portal_sessions afterwards
    keyless: int        # rows still without an account_key
    early: int          # rows before their account's first version
    lag: int            # records in the log the group has not read
    dropped: int        # old segments deleted


def log_path(warehouse) -> Path:
    return Path(warehouse).parent / "stream" / "portal-log"


def run(warehouse, landing, by: datetime, chunk: int = CHUNK,
        log_dir: Path | None = None) -> Night:
    """One night's sessions: what had arrived by `by`, consumed. What
    it did is the task's detail in audit.task_runs; the stream keeps
    its own place in the log, so it is not a load in the load log."""
    log = Log(log_dir or log_path(warehouse))
    ledger = log.path / "produced.txt"
    done = (set(ledger.read_text().splitlines()) if ledger.exists()
            else set())
    new = [a.path for a in files.discover(landing, "portal", by=by)
           if Path(a.path).as_posix() not in done]
    con = duckdb.connect(str(warehouse))
    try:
        con.execute("SET enable_progress_bar = false")
        night = _consume(con, log, new, chunk)
    finally:
        con.close()
        log.close()
    return night


def _consume(con, log: Log, new: list, chunk: int) -> Night:
    regions = dict(con.execute(REGIONS).fetchall())
    group = Consumer(log, GROUP)
    start, prior = group.committed(), group.restore()
    base = prior.counts["rows"] if prior else 0
    records = dropped = 0
    state = prior
    for i in range(0, len(new), chunk):
        records += append_files(log, new[i:i + chunk])
        # Read whole batches only, so that a catch-up a week at a time
        # makes the same batches, and the same rows, as one long read.
        whole = log.end_offset // BATCH * BATCH
        if whole > Consumer(log, GROUP).committed():
            state = sessions.run(log, con, regions, group=GROUP,
                                 batch=BATCH, until=whole)
        dropped += log.retain(Consumer(log, GROUP).committed())
    state = sessions.run(log, con, regions, group=GROUP, batch=BATCH)
    dropped += log.retain(Consumer(log, GROUP).committed())
    con.execute(FILL_KEYS)
    con.execute(FILL_UNKNOWN)
    rows, keyless, early = con.execute("""
        SELECT count(*), count(*) FILTER (s.account_key IS NULL),
               count(*) FILTER (a.account_id = -1)
        FROM marts.fct_portal_sessions AS s
        LEFT JOIN marts.dim_account AS a USING (account_key)"""
                                         ).fetchone()
    group = Consumer(log, GROUP)
    still = sum(not s.emitted for s in state.open.values())
    return Night(len(new), records, group.committed() - start,
                 state.counts["rows"] - base, still, rows, keyless,
                 early, group.lag, dropped)
