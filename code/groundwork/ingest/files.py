"""
Files other people send: found, fingerprinted, kept, loaded once.

The landing zone is a folder other teams write into and Groundwork
only reads. A file's modification time is when it arrived. This module
is what every file loader is built on:

    discover(root, under, by=...)   what had arrived by a moment
    fingerprint(path)               SHA-256 of the bytes: what a file is
    keep(path, source, sha)         a compressed copy, named by its hash
    ledger(con, source)             every file ever seen, and what
                                    became of it
    load(con, source, table, ...)   the loop: new files only, parsed
                                    from the kept copy, one partition

Every file seen is a row in audit.file_ledger, keyed by source, name
and fingerprint:

    loaded      its rows are in raw
    duplicate   the same bytes were loaded before, under another name;
                nothing is loaded twice
    failed      it could not be read; the next run tries again

A file is new when the ledger has not seen that name with those bytes.
Arrival times are only used to decide what a run can see, never to
decide what is new, because a file can arrive with an old time.

Resends. Each file also gets a logical key, which says what it is
rather than what it is called: a night's accounts export, a supplier's
list for a month. **When two versions of one logical file have both
been loaded, the one that arrived last is in force.** Raw keeps both;
the view audit.current_files names the one in force, and staging reads
through it (Chapter 11). A resend of bytes already loaded is a
duplicate and never becomes the newest version.
"""
from __future__ import annotations

import gzip
import hashlib
import os
import time
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import pyarrow.parquet as pq

from groundwork import loadlog
from groundwork.config import GROUNDWORK, LANDING
from groundwork.storage import write_partition

ARCHIVE = GROUNDWORK / "landed"
"""Every file Groundwork loaded, gzipped, named by its SHA-256."""

QUIET = timedelta(minutes=10)
"""A file younger than this may still be being written: leave it."""

BLOCK = 1 << 20

DDL = """
CREATE SCHEMA IF NOT EXISTS audit;
CREATE TABLE IF NOT EXISTS audit.file_ledger (
    source       VARCHAR NOT NULL,
    name         VARCHAR NOT NULL,    -- path under the landing root
    sha256       VARCHAR NOT NULL,
    size         BIGINT NOT NULL,
    arrived_at   TIMESTAMP NOT NULL,  -- its modification time, UTC
    seen_at      TIMESTAMP NOT NULL,  -- the run that found it
    status       VARCHAR NOT NULL
                 CHECK (status IN ('loaded', 'duplicate', 'failed')),
    table_name   VARCHAR,
    logical_key  VARCHAR,             -- what it is, not its name
    load_date    DATE,
    load_id      VARCHAR,
    rows         BIGINT,
    note         VARCHAR,
    PRIMARY KEY (source, name, sha256)
);
CREATE OR REPLACE VIEW audit.current_files AS
SELECT * FROM audit.file_ledger
WHERE status = 'loaded'
QUALIFY row_number() OVER (
    PARTITION BY source, table_name, logical_key
    ORDER BY arrived_at DESC, name DESC) = 1;
"""
COLUMNS = ("source", "name", "sha256", "size", "arrived_at",
           "seen_at", "status", "table_name", "logical_key",
           "load_date", "load_id", "rows", "note")


@dataclass(frozen=True)
class Arrival:
    """A file in the landing zone, as a folder listing sees it."""
    path: Path
    name: str              # relative to the landing root, with '/'
    size: int
    arrived_at: datetime   # UTC, no zone


@dataclass(frozen=True)
class Seen:
    """One row of audit.file_ledger."""
    source: str
    name: str
    sha256: str
    size: int
    arrived_at: datetime
    seen_at: datetime
    status: str
    table_name: str | None
    logical_key: str | None
    load_date: date | None
    load_id: str | None
    rows: int | None
    note: str | None


@dataclass(frozen=True)
class Parsed:
    """What a loader's reader makes of one file's bytes."""
    rows: list[dict]
    counted: int           # data rows counted another way
    logical_key: str | None
    note: str | None = None


class MissingFile(Exception):
    """A file the schedule says should be there, and is not."""


# ------------------------------------------------------------ finding
def _utc(moment: datetime) -> datetime:
    if moment.tzinfo is not None:
        moment = moment.astimezone(timezone.utc).replace(tzinfo=None)
    return moment


def arrived_at(path) -> datetime:
    """When a file arrived: its modification time, in UTC."""
    t = Path(path).stat().st_mtime
    return datetime.fromtimestamp(t, timezone.utc).replace(tzinfo=None)


def discover(root=LANDING, under: str = "", *,
             by: datetime | None = None,
             quiet: timedelta = QUIET) -> list[Arrival]:
    """
    Every file under root/under that had arrived by `by`, oldest first.

    Hidden files and names ending .tmp or .part are someone's work in
    progress and are skipped; so is a file that arrived less than
    `quiet` before `by`, which the next run will see.
    """
    root = Path(root)
    limit = None if by is None else _utc(by) - quiet
    found = []
    start = root / under
    walk = ([(start.parent, [], [start.name])] if start.is_file()
            else os.walk(start))
    for folder, _, names in walk:
        for n in names:
            if n.startswith(".") or n.endswith((".tmp", ".part")):
                continue
            p = Path(folder) / n
            st = p.stat()
            at = arrived_at(p)
            if limit is not None and at > limit:
                continue
            found.append(Arrival(p, p.relative_to(root).as_posix(),
                                 st.st_size, at))
    return sorted(found, key=lambda a: (a.arrived_at, a.name))


def fingerprint(path) -> str:
    """The SHA-256 of a file's bytes, as 64 hex characters."""
    h = hashlib.sha256()
    with open(path, "rb") as f:
        while block := f.read(BLOCK):
            h.update(block)
    return h.hexdigest()


# ------------------------------------------------------------ keeping
def kept_path(source: str, sha: str, archive=ARCHIVE) -> Path:
    return Path(archive) / source / sha[:2] / f"{sha}.gz"


def keep(path, source: str, sha: str, archive=ARCHIVE) -> Path:
    """
    Copy a landed file into the archive, gzipped, under its hash.

    The copy is checked against `sha` as it is written, so what is
    kept is exactly what was fingerprinted, even if the sender
    overwrites the original a second later. Keeping the same bytes
    twice costs nothing: the name is already there.
    """
    dest = kept_path(source, sha, archive)
    if dest.exists():
        return dest
    dest.parent.mkdir(parents=True, exist_ok=True)
    tmp = dest.with_name(f".{dest.name}.{os.getpid()}.tmp")
    h = hashlib.sha256()
    try:
        with open(path, "rb") as src, \
                gzip.GzipFile(tmp, "wb", mtime=0) as out:
            while block := src.read(BLOCK):
                h.update(block)
                out.write(block)
        if h.hexdigest() != sha:
            raise ValueError(f"{path} changed while it was being kept")
        os.replace(tmp, dest)
    finally:
        tmp.unlink(missing_ok=True)
    return dest


def read_kept(source: str, sha: str, archive=ARCHIVE) -> bytes:
    """The bytes of a kept file, checked against their name."""
    data = gzip.decompress(kept_path(source, sha, archive).read_bytes())
    if hashlib.sha256(data).hexdigest() != sha:
        raise ValueError(f"kept file {sha[:12]} does not match")
    return data


# ------------------------------------------------------------ ledger
def ensure(con) -> None:
    """Create audit.file_ledger and audit.current_files. Safe twice."""
    con.execute(DDL)


def _seen(row) -> Seen:
    return Seen(**dict(zip(COLUMNS, row)))


def ledger(con, source: str | None = None) -> list[Seen]:
    """Every file seen, in the order the runs found them."""
    ensure(con)
    sql = f"SELECT {', '.join(COLUMNS)} FROM audit.file_ledger"
    args = []
    if source is not None:
        sql += " WHERE source = ?"
        args.append(source)
    sql += " ORDER BY seen_at, arrived_at, name"
    return [_seen(r) for r in con.execute(sql, args).fetchall()]


def current(con, source: str) -> list[Seen]:
    """The version in force of each logical file: the last to arrive."""
    ensure(con)
    rows = con.execute(
        f"SELECT {', '.join(COLUMNS)} FROM audit.current_files "
        "WHERE source = ? ORDER BY table_name, logical_key",
        [source]).fetchall()
    return [_seen(r) for r in rows]


def is_new(con, source: str, name: str, sha: str) -> bool:
    """True unless this name with these bytes was loaded or skipped."""
    (n,) = con.execute(
        "SELECT count(*) FROM audit.file_ledger WHERE source = ? AND "
        "name = ? AND sha256 = ? AND status <> 'failed'",
        [source, name, sha]).fetchone()
    return n == 0


def unseen(con, source: str, arrivals: list[Arrival]) -> list[Arrival]:
    """The arrivals whose name and bytes the ledger has not seen."""
    ensure(con)
    return [a for a in arrivals
            if is_new(con, source, a.name, fingerprint(a.path))]


def loaded_as(con, source: str, table: str, sha: str,
              logical_key: str | None) -> str | None:
    """The name these bytes were loaded under before, as the same
    logical file, if they were."""
    row = con.execute(
        "SELECT name FROM audit.file_ledger WHERE source = ? AND "
        "table_name = ? AND sha256 = ? AND logical_key IS NOT DISTINCT "
        "FROM ? AND status = 'loaded' ORDER BY arrived_at LIMIT 1",
        [source, table, sha, logical_key]).fetchone()
    return row[0] if row else None


def record(con, source: str, a: Arrival, sha: str, *, seen_at: datetime,
           status: str, table: str | None = None,
           logical_key: str | None = None, load_date=None,
           load_id: str | None = None, rows: int | None = None,
           note: str | None = None) -> None:
    """Write (or, for a file that failed before, rewrite) its row."""
    con.execute(
        "INSERT OR REPLACE INTO audit.file_ledger VALUES "
        "(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        [source, a.name, sha, a.size, a.arrived_at, _utc(seen_at),
         status, table, logical_key, load_date, load_id, rows, note])


# ------------------------------------------------------------ loading
def lines(data: bytes) -> int:
    """Data rows in a CSV with a header: line breaks, less one."""
    n = data.count(b"\n")
    if data and not data.endswith(b"\n"):
        n += 1                             # a last line with no break
    return max(n - 1, 0)


def load(con, source: str, table: str, arrivals: list[Arrival],
         load_date, *, run_at: datetime, read, folder: str,
         root: Path | None = None, archive=ARCHIVE) -> loadlog.Load:
    """
    Load the new files among `arrivals` into one raw partition.

    `read(data, arrival)` turns a file's bytes into a Parsed. A file
    is new unless the ledger has seen that name with those bytes. It
    is a duplicate if the same bytes were loaded before as the same
    logical file: identical bytes for a different night or month are
    new data, and are loaded. The partition for `load_date` holds
    every file the ledger gives that date, so a rerun that finds one
    more file rewrites it with all of them, read from the kept copies.
    Rows carry `file` and `file_sha256`; `_source_file` is the folder
    the batch came from.
    """
    clock = time.perf_counter()

    def now():
        return run_at + timedelta(seconds=time.perf_counter() - clock)

    def parse(a, sha):
        try:
            return read(read_kept(source, sha, archive), a)
        except Exception as e:
            record(con, source, a, sha, seen_at=run_at, status="failed",
                   table=table, load_date=day, load_id=load_id,
                   note=f"{type(e).__name__}: {e}")
            raise

    ensure(con)
    day = date.fromisoformat(str(load_date))
    load_id = loadlog.start(con, source, table, day, started_at=now())
    counts = {"rows_read": 0, "source_count": 0, "rows_written": 0}
    fresh, dupes, batch = [], [], []
    try:
        for a in arrivals:
            sha = fingerprint(a.path)
            if not is_new(con, source, a.name, sha):
                continue
            keep(a.path, source, sha, archive)
            p = parse(a, sha)
            earlier = loaded_as(con, source, table, sha, p.logical_key)
            if earlier is None:
                fresh.append((a, sha, p))
            else:
                dupes.append((a, sha, p, earlier))
        if fresh:
            before = con.execute(
                "SELECT name, sha256, size, arrived_at "
                "FROM audit.file_ledger WHERE source = ? AND "
                "table_name = ? AND load_date = ? AND "
                "status = 'loaded' ORDER BY arrived_at, name",
                [source, table, day]).fetchall()
            for n, sha, size, at in before:
                a = Arrival(Path(n), n, size, at)
                batch.append((a, sha, parse(a, sha)))
            batch += fresh
        rows = []
        for a, sha, p in batch:
            for r in p.rows:
                r["file"], r["file_sha256"] = a.name, sha
            rows += p.rows
            counts["rows_read"] += len(p.rows)
            counts["source_count"] += p.counted
        if counts["rows_read"] != counts["source_count"]:
            raise ValueError(f"read {counts['rows_read']:,} rows, the "
                             f"files hold {counts['source_count']:,}")
        if fresh:
            path = write_partition(rows, source, table, day,
                                   loaded_at=run_at, load_id=load_id,
                                   source_file=folder, root=root)
            counts["rows_written"] = (
                pq.ParquetFile(path).metadata.num_rows)
            if counts["rows_written"] != counts["rows_read"]:
                raise ValueError(f"wrote {counts['rows_written']:,} "
                                 f"rows, read {counts['rows_read']:,}")
    except Exception as e:
        loadlog.fail(con, load_id, finished_at=now(),
                     error=f"{type(e).__name__}: {e}", **counts)
        raise

    for a, sha, p in fresh:
        record(con, source, a, sha, seen_at=run_at, status="loaded",
               table=table, logical_key=p.logical_key, load_date=day,
               load_id=load_id, rows=len(p.rows), note=p.note)
    for a, sha, p, earlier in dupes:
        record(con, source, a, sha, seen_at=run_at, status="duplicate",
               table=table, logical_key=p.logical_key, load_date=day,
               load_id=load_id, rows=0, note=f"same bytes as {earlier}")
    loadlog.finish(con, load_id, finished_at=now(), **counts,
                   extra={"new_files": len(fresh),
                          "duplicates": len(dupes),
                          "files_in_partition": len(batch)})
    return next(x for x in loadlog.history(con, source, table)
                if x.load_id == load_id)


# ------------------------------------------------------------ command
def cli(argv, *, load, source: str, description: str) -> int:
    """The command line every file loader shares: --at and
    --load-date, one line printed per table loaded."""
    import argparse

    from groundwork.config import NOW
    p = argparse.ArgumentParser(description=description)
    p.add_argument("--at", default=NOW.strftime("%Y-%m-%d %H:%M"),
                   help="the run's moment, UTC (default: NOW)")
    p.add_argument("--load-date", type=date.fromisoformat,
                   help="the logical date (default: the day before)")
    a = p.parse_args(argv)
    run_at = datetime.fromisoformat(a.at)
    day = a.load_date or (run_at.date() - timedelta(days=1))
    result = load(day, run_at=run_at)
    loads = [] if result is None else (
        result if isinstance(result, list) else [result])
    if not loads:
        print(f"{source}: nothing new by {run_at:%Y-%m-%d %H:%M} UTC")
    for x in loads:
        print(f"{x.load_id:<38} {x.rows_written:>7,} rows from "
              f"{x.extra['new_files']:,} new files")
    return 0
