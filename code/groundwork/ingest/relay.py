"""
Relay, the CRM: each night's export, the field audit and IT's map.

Every night at 23:00 in Newark Relay writes accounts.csv and
contracts.csv into landing/relay/<night>/. The run for a night looks
for exactly those two files in exactly that folder: a night is
expected, not discovered, so a night with no export is a failed load
with a reason, never a quiet success.

Two formats. Before 1 March 2024 the export came from the old CRM:
semicolons, Windows-1252, other column names. From then on it is
Relay's own: commas, UTF-8 with a byte-order mark. Each file's format
is read from its first bytes. The old names are mapped to Relay's, so
one table holds both eras, and every value is kept as the file wrote
it, as text; `source_format` says which era a row came from, and
staging (Chapter 11) reads each era's values its own way.

    raw/relay/accounts/load_date=<night>/       one night's export
    raw/relay/contracts/load_date=<night>/
    raw/relay/field_history/load_date=<run>/    audit files new that run
    raw/relay/migration_map/load_date=<run>/    IT's old-id map

    load(night, run_at=...)       every table, one night
    python -m groundwork.ingest.relay --night D [--at T]
"""
from __future__ import annotations

import argparse
import csv
import io
from datetime import date, datetime, timedelta
from pathlib import Path

import duckdb

from groundwork import loadlog
from groundwork.config import LANDING, TODAY, WAREHOUSE
from groundwork.ingest import files
from groundwork.storage import raw_views

SOURCE = "relay"
BOM = b"\xef\xbb\xbf"
LEGACY = {
    "accounts": {
        "Acct ID": "account_id", "Account Name": "account_name",
        "Region": "region", "Segment": "segment", "Owner": "owner",
        "Customer Since": "customer_since", "Zip": "postcode",
        "Key Acct": "key_account", "Status": "status",
        "Closed Date": "closed_on", "Primary Contact": "contact_name",
        "Contact Email": "contact_email",
        "Contact Phone": "contact_phone"},
    "contracts": {
        "Contract ID": "contract_id", "Acct ID": "account_id",
        "Start": "start_date", "End": "end_date",
        "Term (months)": "term_months", "Legacy Terms": "legacy_terms",
        "Discount %": "discount_pct", "Outcome": "outcome",
        "Notice Date": "notice_date",
        "Cancellation Reason": "cancellation_reason"},
}
COLUMNS = {
    "accounts": ["account_id", "account_name", "region", "segment",
                 "owner", "customer_since", "postcode", "key_account",
                 "status", "closed_on", "contact_name", "contact_email",
                 "contact_phone", "updated_at"],
    "contracts": ["contract_id", "account_id", "start_date", "end_date",
                  "term_months", "legacy_terms", "discount_pct",
                  "outcome", "notice_date", "cancellation_reason",
                  "updated_at"],
}


class UnknownFormat(ValueError):
    """A file that is neither the old CRM's export nor Relay's."""


def sniff(data: bytes) -> tuple[str, str, str]:
    """(format, encoding, delimiter) from a file's first bytes."""
    if data.startswith(BOM):
        return "relay", "utf-8-sig", ","
    head = data[:200].decode("cp1252", errors="replace")
    if head.startswith(("Acct ID;", "Contract ID;")):
        return "legacy", "cp1252", ";"
    raise UnknownFormat(f"unrecognised export: {data[:40]!r}")


def read_export(table: str):
    """A reader for one export table, in either format."""
    def read(data: bytes, arrival: files.Arrival) -> files.Parsed:
        fmt, encoding, delimiter = sniff(data)
        text = data.decode(encoding)
        reader = csv.reader(io.StringIO(text, newline=""),
                            delimiter=delimiter)
        header = next(reader)
        names = ([LEGACY[table][h] for h in header]
                 if fmt == "legacy" else header)
        if set(names) - set(COLUMNS[table]):
            raise UnknownFormat(f"unexpected columns in {arrival.name}")
        rows = []
        for values in reader:
            row = dict.fromkeys(COLUMNS[table])
            row.update(zip(names, values))
            row["source_format"] = fmt
            rows.append(row)
        night = arrival.name.split("/")[-2]
        return files.Parsed(rows, files.lines(data), night, note=fmt)
    return read


def read_csv(key):
    """A reader for a plain UTF-8 CSV kept as text, keyed by `key`."""
    def read(data: bytes, arrival: files.Arrival) -> files.Parsed:
        text = data.decode("utf-8-sig")
        rows = list(csv.DictReader(io.StringIO(text, newline="")))
        return files.Parsed(rows, files.lines(data), key(arrival))
    return read


def _month(a: files.Arrival) -> str:
    return Path(a.name).stem.removeprefix("field_history_")


def load(night, *, run_at: datetime | None = None, landing=LANDING,
         root: Path | None = None, warehouse: Path = WAREHOUSE,
         archive=files.ARCHIVE) -> list[loadlog.Load]:
    """
    Load one night: its export, and whatever audit files are new.

    `run_at` defaults to 06:00 UTC the next morning, when the nightly
    run starts. Raises files.MissingFile, after logging it, when the
    night's export has not arrived.
    """
    night = date.fromisoformat(str(night))
    run_at = run_at or datetime.combine(night + timedelta(days=1),
                                        datetime.min.time()).replace(
                                            hour=6)
    Path(warehouse).parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(warehouse))
    try:
        loads, missing = [], []
        folder = f"relay/{night.isoformat()}"
        found = {a.name.rsplit("/", 1)[1]: a
                 for a in files.discover(landing, folder, by=run_at)}
        for table in ("accounts", "contracts"):
            a = found.get(f"{table}.csv")
            if a is None:
                load_id = loadlog.start(con, SOURCE, table, night,
                                        started_at=run_at)
                when = f"{run_at:%Y-%m-%d %H:%M}"
                loadlog.fail(con, load_id, finished_at=run_at,
                             error=f"MissingFile: no {folder}/"
                                   f"{table}.csv by {when} UTC")
                missing.append(f"{folder}/{table}.csv")
                continue
            loads.append(files.load(
                con, SOURCE, table, [a], night, run_at=run_at,
                read=read_export(table), folder=folder, root=root,
                archive=archive))
        extras = (("field_history", "relay/audit", read_csv(_month)),
                  ("migration_map", "relay/migration_map.csv",
                   read_csv(lambda a: "migration_map")))
        for table, under, read in extras:
            arrivals = files.discover(landing, under, by=run_at)
            if arrivals:
                loads.append(files.load(
                    con, SOURCE, table, arrivals, night, run_at=run_at,
                    read=read, folder=under, root=root,
                    archive=archive))
        raw_views(con, root)
        if missing:
            raise files.MissingFile(", ".join(missing))
        return loads
    finally:
        con.close()


def missing_nights(con, first, last) -> list[date]:
    """Nights from first to last with no accounts export in force."""
    files.ensure(con)
    have = {r[0] for r in con.execute(
        "SELECT logical_key FROM audit.current_files "
        "WHERE source = 'relay' AND table_name = 'accounts'"
    ).fetchall()}
    d, out = date.fromisoformat(str(first)), []
    while d <= date.fromisoformat(str(last)):
        if d.isoformat() not in have:
            out.append(d)
        d += timedelta(days=1)
    return out


def main(argv=None) -> int:
    p = argparse.ArgumentParser(description=__doc__.split("\n")[1])
    p.add_argument("--night", type=date.fromisoformat,
                   default=TODAY - timedelta(days=1),
                   help="the export's night (default: last night)")
    p.add_argument("--to", type=date.fromisoformat,
                   help="load every night from --night to this one")
    p.add_argument("--at", help="the run's moment, UTC (default: 06:00 "
                                "the morning after each night)")
    a = p.parse_args(argv)
    night, status = a.night, 0
    while night <= (a.to or a.night):
        at = datetime.fromisoformat(a.at) if a.at else None
        try:
            loads = load(night, run_at=at)
        except files.MissingFile as e:
            print(f"{night}  MISSING  {e}")
            status = 1
        else:
            for x in loads:
                print(f"{x.load_id:<38} {x.rows_written:>7,} rows, "
                      f"the files hold {x.source_count:>7,}")
        night += timedelta(days=1)
    return status


if __name__ == "__main__":
    raise SystemExit(main())
