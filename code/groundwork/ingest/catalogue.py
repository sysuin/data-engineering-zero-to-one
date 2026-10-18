"""
The catalogue: merchandising's product list, saved over when edited.

There is one file, landing/catalogue/catalogue.csv, and no history: an
edit replaces it. Each version the run finds (new bytes under the same
name) is loaded whole into the day's partition, with the columns and
values as merchandising wrote them. The version that arrived last is
in force; the earlier ones survive only in raw and in the kept copies.

    load(load_date, run_at=...)
    python -m groundwork.ingest.catalogue [--load-date D] [--at T]
"""
from __future__ import annotations

import csv
import io
from datetime import datetime
from pathlib import Path

import duckdb

from groundwork import loadlog
from groundwork.config import LANDING, WAREHOUSE
from groundwork.ingest import files
from groundwork.storage import raw_views

SOURCE, TABLE = "catalogue", "products"


def read(data: bytes, arrival: files.Arrival) -> files.Parsed:
    """Every row, every column as text, names as the file has them."""
    text = data.decode("utf-8-sig")
    rows = list(csv.DictReader(io.StringIO(text, newline="")))
    return files.Parsed(rows, files.lines(data), "catalogue")


def load(load_date, *, run_at: datetime, landing=LANDING,
         root: Path | None = None, warehouse: Path = WAREHOUSE,
         archive=files.ARCHIVE) -> loadlog.Load:
    """Load a new version of the catalogue, if there is one."""
    Path(warehouse).parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(warehouse))
    try:
        arrivals = files.discover(landing, SOURCE, by=run_at)
        x = files.load(con, SOURCE, TABLE, arrivals, load_date,
                       run_at=run_at, read=read, folder=SOURCE,
                       root=root, archive=archive)
        raw_views(con, root)
        return x
    finally:
        con.close()


def main(argv=None) -> int:
    return files.cli(argv, load=load, source=SOURCE,
                     description=__doc__.split("\n")[1])


if __name__ == "__main__":
    raise SystemExit(main())
