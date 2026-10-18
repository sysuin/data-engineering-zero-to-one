"""
Every source, one logical day, each batch held to its contract.

    python -m groundwork.ingest --day D [--at T] [--only SOURCE ...]

The run for day D starts at 06:00 UTC on D+1, after Relay's export
and Tradeline's nightly job. It runs each source's loader from
Chapters 7 to 9 and holds what it brings to the contract for its raw
table (groundwork/contracts/):

    files       Relay, the suppliers, the catalogue, the ledger and
                the reference files are checked before they land: the
                Chapter 9 reader is wrapped in a contracts.check.Guard,
                so a breaking change fails the file and its load, and
                bad rows are parked in audit.quarantine while the rest
                land. Price lists are read with price_lists.read, which
                knows Pemberton's header from July 2024.
    tradeline   Chapter 7's loader writes as it reads, so its partition
    frontline   (and Chapter 8's) is checked as soon as it is written.
                Their contracts' policy is to fail, not to park: a row
                from a system of record that breaks its contract stops
                the run for that table, and nothing is dropped.

Every check is a row of audit.contract_runs. The run prints one line
per table and exits 1 if any table broke, failed or went missing, so
that nothing downstream runs on a day that did not land whole.
"""
from __future__ import annotations

import argparse
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta
from pathlib import Path

import duckdb
import pyarrow.parquet as pq

from groundwork import contracts, loadlog
from groundwork.config import (LANDING, REFERENCE, TRADELINE_DB,
                               WAREHOUSE)
from groundwork.contracts import check
from groundwork.ingest import (catalogue, files, ledger, price_lists,
                               reference, relay)
from groundwork.storage import raw_path, raw_views

SOURCES = ("tradeline", "frontline", "relay", "suppliers", "catalogue",
           "ledger", "reference")
BAD = ("broken", "failed", "missing")


@dataclass
class Line:
    """What one table's part of the run did."""
    source: str
    table: str
    status: str            # passed, parked, nothing new, or one of BAD
    files: int | None      # files checked; None for a database or API
    rows: int              # rows the batch held
    loaded: int            # rows that landed in raw
    parked: int            # rows put in audit.quarantine
    load_id: str | None = None
    detail: str = ""


def _latest(con, source, table, day) -> loadlog.Load | None:
    loads = [x for x in loadlog.history(con, source, table)
             if x.load_date == day]
    return max(loads, key=lambda x: int(x.load_id.rsplit(".", 1)[1]),
               default=None)


def land(con, source, table, arrivals, day, run_at, read, folder,
         root=None, archive=files.ARCHIVE, *, isolate=True,
         contract=None) -> Line:
    """
    One file table: Chapter 9's files.load, with its reader held to
    the contract by a Guard. With `isolate`, each new file is screened
    first, and one that breaks the contract is recorded failed in the
    file ledger and left out, so one sender's change stops that
    sender's file, not every other sender's; the line still says
    broken. A source whose files are expected (Relay's night) is not
    isolated: its one file failing fails the load.
    """
    c = contract or contracts.load(f"{source}.{table}")
    guard = check.Guard(read, c, con=con)
    files.ensure(con)
    status, detail, keep = None, "", []
    for a in arrivals:
        sha = files.fingerprint(a.path)
        if isolate and files.is_new(con, source, a.name, sha):
            try:
                guard(a.path.read_bytes(), a)
            except check.ContractBroken as e:
                files.record(con, source, a, sha, seen_at=run_at,
                             status="failed", table=table,
                             load_date=day, note=f"ContractBroken: {e}")
                status, detail = "broken", detail or str(e)
                continue
        keep.append(a)
    try:
        files.load(con, source, table, keep, day, run_at=run_at,
                   read=guard, folder=folder, root=root,
                   archive=archive)
    except check.ContractBroken as e:
        status, detail = "broken", str(e)
    except Exception as e:                    # not the contract's doing
        status, detail = "failed", f"{type(e).__name__}: {e}"
    x = _latest(con, source, table, day)
    got = guard.flush(con, load_id=x.load_id, at=run_at, load_date=day)
    if status is None:
        status = ("nothing new" if got["files"] == 0 else
                  "parked" if got["parked"] else "passed")
        detail = (f"{x.extra.get('new_files', 0)} new file(s)"
                  if got["files"] else "")
    loaded = (x.rows_written or 0) if x.status == "succeeded" else 0
    return Line(source, table, status, got["files"], got["rows"],
                loaded, got["parked"], x.load_id, detail)


def _relay(con, day, run_at, landing, root, archive) -> list[Line]:
    """Relay's night, expected by name as Chapter 9's loader expects
    it; every file through the guard."""
    out = []
    folder = f"relay/{day.isoformat()}"
    found = {a.name.rsplit("/", 1)[1]: a
             for a in files.discover(landing, folder, by=run_at)}
    for table in ("accounts", "contracts"):
        a = found.get(f"{table}.csv")
        if a is None:
            load_id = loadlog.start(con, "relay", table, day,
                                    started_at=run_at)
            why = (f"MissingFile: no {folder}/{table}.csv by "
                   f"{run_at:%Y-%m-%d %H:%M} UTC")
            loadlog.fail(con, load_id, finished_at=run_at, error=why)
            out.append(Line("relay", table, "missing", 0, 0, 0, 0,
                            load_id, why))
            continue
        out.append(land(con, "relay", table, [a], day, run_at,
                        relay.read_export(table), folder, root,
                        archive, isolate=False))
    extras = (("field_history", "relay/audit",
               relay.read_csv(relay._month)),
              ("migration_map", "relay/migration_map.csv",
               relay.read_csv(lambda a: "migration_map")))
    for table, under, read in extras:
        arrivals = files.discover(landing, under, by=run_at)
        if arrivals:
            out.append(land(con, "relay", table, arrivals, day,
                            run_at, read, under, root, archive))
    return out


def _typed(con, source, table, x: loadlog.Load, day, run_at,
           root) -> Line:
    """A partition a loader has already written, held to its contract
    straight after: Tradeline's and Frontline's."""
    c = contracts.load(f"{source}.{table}")
    t = pq.read_table(raw_path(source, table, day, root))
    data = t.select([n for n in t.column_names
                     if not n.startswith("_")])
    types = {f.name: str(f.type) for f in data.schema}
    res = check.check(c, data, types=types, con=con)
    parked = check.park(con, res, data, ref=x.load_id, at=run_at,
                        load_id=x.load_id, load_date=day)
    check.record(con, res, load_id=x.load_id, ref=x.load_id, at=run_at)
    detail = res.breaking[0] if res.breaking else "; ".join(res.notes)
    return Line(source, table, res.status, None, t.num_rows,
                0 if res.breaking else t.num_rows - parked, parked,
                x.load_id, detail)


def _tradeline(day, run_at, db, root, warehouse) -> list[Line]:
    from groundwork.ingest import tradeline
    try:
        loads = tradeline.load(day, run_at=run_at, source=db, root=root,
                               warehouse=warehouse)
    except Exception as e:
        return [Line("tradeline", "orders", "failed", None, 0, 0, 0,
                     None, f"{type(e).__name__}: {e}")]
    con = duckdb.connect(str(warehouse))
    try:
        return [_typed(con, "tradeline", x.table_name if
                       x.extra["snapshot"] else f"{x.table_name}_log",
                       x, day, run_at, root) for x in loads]
    finally:
        con.close()


def _frontline(day, run_at, base, root, warehouse) -> list[Line]:
    from groundwork.ingest import frontline
    if base is None:                   # Meridian's desk, served locally
        from meridian.sources import Clock
        from meridian.sources import frontline as desk
        base = desk.serve(Clock(run_at))
    try:
        x = frontline.load(day, run_at=run_at, base=base, root=root,
                           warehouse=warehouse)
    except Exception as e:
        return [Line("frontline", "tickets", "failed", None, 0, 0, 0,
                     None, f"{type(e).__name__}: {e}")]
    con = duckdb.connect(str(warehouse))
    try:
        return [_typed(con, "frontline", t, x, day, run_at, root)
                for t in ("tickets", "ticket_fields", "ticket_tags")]
    finally:
        con.close()


def run(day, *, run_at: datetime | None = None, only=None,
        landing=LANDING, reference_dir=REFERENCE,
        tradeline_db=TRADELINE_DB, frontline_base: str | None = None,
        root: Path | None = None, warehouse: Path = WAREHOUSE,
        archive=files.ARCHIVE) -> list[Line]:
    """Load every source (or `only` these) for logical day `day`."""
    day = date.fromisoformat(str(day))
    run_at = run_at or datetime.combine(day + timedelta(days=1),
                                        time(6))
    want = set(only or SOURCES)
    Path(warehouse).parent.mkdir(parents=True, exist_ok=True)
    lines = []
    if "tradeline" in want:
        lines += _tradeline(day, run_at, tradeline_db, root, warehouse)
    if "frontline" in want:
        lines += _frontline(day, run_at, frontline_base, root,
                            warehouse)
    con = duckdb.connect(str(warehouse))
    try:
        kw = dict(root=root, archive=archive)
        if "relay" in want:
            lines += _relay(con, day, run_at, landing, **kw)
        plain = (("suppliers", "price_lists", price_lists.read),
                 ("catalogue", "products", catalogue.read),
                 ("ledger", "gl_revenue", ledger.read))
        for source, table, read in plain:
            if source in want:
                arrivals = files.discover(landing, source, by=run_at)
                lines.append(land(con, source, table, arrivals, day,
                                  run_at, read, source, **kw))
        if "reference" in want:
            for a in files.discover(reference_dir, "", by=None):
                if a.path.suffix == ".csv":
                    lines.append(land(con, "reference", a.path.stem,
                                      [a], day, run_at, reference.read,
                                      a.name, isolate=False, **kw))
        raw_views(con, root)
    finally:
        con.close()
    return lines


def report(lines, day, run_at) -> str:
    """The run's summary, one line a table."""
    out = [f"ingest {day}  run {run_at:%Y-%m-%d %H:%M} UTC",
           f"{'table':<26}{'files':>6}{'rows':>10}{'loaded':>10}"
           f"{'parked':>7}  status"]
    for x in lines:
        n = "-" if x.files is None else f"{x.files:,}"
        out.append(f"{x.source + '.' + x.table:<26}{n:>6}{x.rows:>10,}"
                   f"{x.loaded:>10,}{x.parked:>7,}  {x.status}")
    for x in lines:
        if x.status in BAD:
            out.append(f"  {x.source}.{x.table}: {x.detail}")
    bad = sum(x.status in BAD for x in lines)
    parked = sum(x.parked for x in lines)
    out.append(f"{len(lines)} tables, {bad} not landed, {parked:,} "
               f"row{'s' * (parked != 1)} parked")
    return "\n".join(out)


def main(argv=None) -> int:
    p = argparse.ArgumentParser(description=__doc__.split("\n")[1])
    p.add_argument("--day", type=date.fromisoformat, required=True,
                   help="the logical date to load")
    p.add_argument("--at", help="the run's moment, UTC (default: 06:00 "
                                "the morning after the day)")
    p.add_argument("--only", nargs="+", choices=SOURCES)
    a = p.parse_args(argv)
    run_at = (datetime.fromisoformat(a.at) if a.at else
              datetime.combine(a.day + timedelta(days=1), time(6)))
    lines = run(a.day, run_at=run_at, only=a.only)
    print(report(lines, a.day, run_at))
    return 1 if any(x.status in BAD for x in lines) else 0


if __name__ == "__main__":
    raise SystemExit(main())
