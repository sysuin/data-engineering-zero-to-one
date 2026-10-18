"""
The ledger: finance's revenue journal, one file per closed month.

A month's journal lands as landing/ledger/gl_revenue_YYYY-MM.csv on
its fifth working day, and a closed month stays closed. Each file is
loaded once, as finance wrote it, keyed by the period inside it. Were
finance ever to resend a month, the later arrival would be in force
and the ledger would show both.

    load(load_date, run_at=...)
    python -m groundwork.ingest.ledger [--load-date D] [--at T]
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

SOURCE, TABLE = "ledger", "gl_revenue"


def read(data: bytes, arrival: files.Arrival) -> files.Parsed:
    """Every journal line as text; the key is the month it closes."""
    text = data.decode("utf-8-sig")
    rows = list(csv.DictReader(io.StringIO(text, newline="")))
    periods = {r["period"] for r in rows}
    if len(periods) > 1:
        raise ValueError(f"{arrival.name}: periods {sorted(periods)}")
    key = periods.pop() if periods else None
    return files.Parsed(rows, files.lines(data), key)


def load(load_date, *, run_at: datetime, landing=LANDING,
         root: Path | None = None, warehouse: Path = WAREHOUSE,
         archive=files.ARCHIVE) -> loadlog.Load:
    """Every month's journal new by run_at."""
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
