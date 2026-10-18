"""
Reference files kept by hand: regions, procurement's supplier master,
and the calendar of price changes.

They live in data/meridian-sources/reference/, not in the landing
zone, and a person edits them when something changes. Their times say
when the file was last saved on this machine, not when anything
arrived, so the run does not filter them by time: it loads any file
whose bytes are new, each into a table named after the file.

    load(load_date, run_at=...)
    python -m groundwork.ingest.reference [--load-date D] [--at T]
"""
from __future__ import annotations

import csv
import io
from datetime import datetime
from pathlib import Path

import duckdb

from groundwork import loadlog
from groundwork.config import REFERENCE, WAREHOUSE
from groundwork.ingest import files
from groundwork.storage import raw_views

SOURCE = "reference"


def read(data: bytes, arrival: files.Arrival) -> files.Parsed:
    text = data.decode("utf-8-sig")
    rows = list(csv.DictReader(io.StringIO(text, newline="")))
    key = Path(arrival.name).stem
    return files.Parsed(rows, files.lines(data), key)


def load(load_date, *, run_at: datetime, reference=REFERENCE,
         root: Path | None = None, warehouse: Path = WAREHOUSE,
         archive=files.ARCHIVE) -> list[loadlog.Load]:
    """Each reference file whose bytes are new, into its own table."""
    Path(warehouse).parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(warehouse))
    try:
        loads = []
        for a in files.discover(reference, "", by=None):
            if a.path.suffix != ".csv":
                continue
            loads.append(files.load(
                con, SOURCE, a.path.stem, [a], load_date,
                run_at=run_at, read=read, folder=a.name, root=root,
                archive=archive))
        raw_views(con, root)
        return loads
    finally:
        con.close()


def main(argv=None) -> int:
    return files.cli(argv, load=load, source=SOURCE,
                     description=__doc__.split("\n")[1])


if __name__ == "__main__":
    raise SystemExit(main())
