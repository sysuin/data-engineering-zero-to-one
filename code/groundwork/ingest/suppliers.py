"""
Supplier price lists: sixteen senders, four layouts, one raw table.

Procurement saves each supplier's monthly list into
landing/suppliers/<supplier>/, named however the supplier names it.
The run for a day loads every list that has arrived and is new to the
file ledger, whatever its name, into one partition for that day.

Each file's layout is recognised from its own contents, and its
columns are mapped to one shape:

    supplier        the folder it arrived in
    sku             the product code
    description     the supplier's name for it, where there is one
    cost            as the supplier wrote it, as text
    currency        where the supplier says, as it says it
    cost_unit       'dollars' or 'cents', from the layout
    effective_from  as the supplier wrote it, as text: the column, or
                    for a spreadsheet the date in its banner
    layout          which of the four layouts it came in
    file, file_sha256

Nothing is converted: a cost in cents stays in cents, a date written
day first stays day first. The logical key of a list is its supplier
and the month it takes effect, read from the dates inside it, so a
list sent twice for one month is two versions of one file, and the
one that arrived last is in force (see files.py).

    load(load_date, run_at=...)
    python -m groundwork.ingest.suppliers [--load-date D] [--at T]
"""
from __future__ import annotations

import csv
import io
import re
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

import duckdb
import openpyxl

from groundwork import loadlog
from groundwork.config import LANDING, WAREHOUSE
from groundwork.ingest import files
from groundwork.storage import raw_views

SOURCE, TABLE = "suppliers", "price_lists"
FIELDS = ("sku", "description", "cost", "currency", "effective_from")


@dataclass(frozen=True)
class Layout:
    name: str
    marker: str            # a column only this layout has
    columns: dict          # our name -> the file's column
    cost_unit: str
    date_format: str       # how its effective dates are written


LAYOUTS = [
    Layout("standard", "unit_cost",
           {"sku": "sku", "description": "description",
            "cost": "unit_cost", "currency": "currency",
            "effective_from": "effective_from"},
           "dollars", "%Y-%m-%d"),
    Layout("kestrel", "cost_cents",
           {"sku": "sku", "cost": "cost_cents",
            "effective_from": "effective"},
           "cents", "%Y-%m-%d"),
    Layout("pemberton", "Item Code",
           {"sku": "Item Code", "description": "Item",
            "cost": "Cost (USD)", "effective_from": "Valid From"},
           "dollars", "%d/%m/%Y"),
]
SPREADSHEET = Layout("aldridge", "Product Code",
                     {"sku": "Product Code",
                      "description": "Description",
                      "cost": "Trade Price"},
                     "dollars", "%d %B %Y")
BANNER_DATE = re.compile(r"valid from (\d{1,2} [A-Z][a-z]+ \d{4})")


def layout_of(header: list[str]) -> Layout:
    """The layout whose marker column is in the header."""
    for layout in LAYOUTS:
        if layout.marker in header:
            return layout
    raise ValueError(f"no known layout has columns {header}")


def _key(supplier: str, when: str, layout: Layout) -> str:
    month = datetime.strptime(when, layout.date_format)
    return f"{supplier}/{month:%Y-%m}"


def read_csv(data: bytes, arrival: files.Arrival) -> files.Parsed:
    """A CSV list, in whichever of the three CSV layouts it came."""
    supplier = arrival.name.split("/")[1]
    text = data.decode("utf-8-sig")
    reader = csv.DictReader(io.StringIO(text, newline=""))
    layout = layout_of(reader.fieldnames or [])
    rows = []
    for row in reader:
        out = {"supplier": supplier}
        out.update({f: row.get(layout.columns[f])
                    if f in layout.columns else None for f in FIELDS})
        out.update(cost_unit=layout.cost_unit, layout=layout.name)
        rows.append(out)
    key = (_key(supplier, rows[0]["effective_from"], layout)
           if rows else None)
    return files.Parsed(rows, files.lines(data), key, note=layout.name)


def read_spreadsheet(data: bytes,
                     arrival: files.Arrival) -> files.Parsed:
    """
    A spreadsheet list: one sheet, a banner, then a header row.

    The header row is found by looking for it, not assumed to be row
    1; the date the prices take effect is only in the banner, and is
    kept as the banner writes it. More than one sheet, merged cells or
    a banner without a date stop the load: each means the layout has
    changed, and a person should look before anything is loaded.
    """
    supplier = arrival.name.split("/")[1]
    book = openpyxl.load_workbook(io.BytesIO(data), read_only=False)
    if len(book.worksheets) != 1:
        n = len(book.worksheets)
        raise ValueError(f"{arrival.name}: {n} sheets")
    sheet = book.worksheets[0]
    if sheet.merged_cells.ranges:
        raise ValueError(f"{arrival.name}: merged cells")
    cells = [list(r) for r in sheet.iter_rows(values_only=True)]
    at = next(i for i, r in enumerate(cells)
              if SPREADSHEET.marker in r)
    banner = " ".join(str(v) for r in cells[:at] for v in r if v)
    found = BANNER_DATE.search(banner)
    if found is None:
        raise ValueError(f"{arrival.name}: no date in the banner")
    header = cells[at]
    rows = []
    for r in cells[at + 1:]:
        if not any(v is not None for v in r):
            continue
        cell = dict(zip(header, r))
        out = {"supplier": supplier}
        for f in FIELDS:
            v = cell.get(SPREADSHEET.columns.get(f))
            out[f] = None if v is None else str(v)
        out["effective_from"] = found.group(1)
        out.update(cost_unit=SPREADSHEET.cost_unit, layout="aldridge")
        rows.append(out)
    counted = sum(1 for r in cells[at + 1:] if r and r[0] is not None)
    key = _key(supplier, found.group(1), SPREADSHEET)
    return files.Parsed(rows, counted, key, note="aldridge")


def read(data: bytes, arrival: files.Arrival) -> files.Parsed:
    """A price list of any layout: spreadsheets begin 'PK' (a zip)."""
    if data[:2] == b"PK":
        return read_spreadsheet(data, arrival)
    return read_csv(data, arrival)


def load(load_date, *, run_at: datetime, landing=LANDING,
         root: Path | None = None, warehouse: Path = WAREHOUSE,
         archive=files.ARCHIVE) -> loadlog.Load:
    """Every list new by run_at, into load_date's partition."""
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
