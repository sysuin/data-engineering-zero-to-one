"""
Supplier price lists, read with every header procurement has agreed.

Chapter 9's reader (suppliers.py) knows each layout by one marker
column and reads the others by name with row.get, so when Pemberton
renamed "Cost (USD)" to "Unit Cost USD" in July 2024 it read every
cost as missing and said nothing. This reader is the same reader
with one more header: Pemberton's from July 2024, recognised by its
own marker and mapped to the same raw columns, with the same layout
name in raw. A file is still read as one layout or refused, and its
note says which header it came with, so the contract can check the
reader and the file agree.

    read(data, arrival)    as suppliers.read; used by v0.4's ingest
"""
from __future__ import annotations

import csv
import io

from groundwork.ingest import files, suppliers
from groundwork.ingest.suppliers import FIELDS, Layout

PEMBERTON_2024 = Layout(
    "pemberton-2024", "Unit Cost USD",
    {"sku": "Item Code", "description": "Item",
     "cost": "Unit Cost USD", "effective_from": "Valid From"},
    "dollars", "%d/%m/%Y")
LAYOUTS = [PEMBERTON_2024, *suppliers.LAYOUTS]
FAMILY = {"pemberton-2024": "pemberton"}   # same layout, new header


def layout_of(header: list[str]) -> Layout:
    """The layout whose marker column is in the header. Pemberton's
    new header is tried first: it has the old marker too."""
    for layout in LAYOUTS:
        if layout.marker in header:
            return layout
    raise ValueError(f"no known layout has columns {header}")


def read_csv(data: bytes, arrival: files.Arrival) -> files.Parsed:
    """A CSV list, in whichever of the CSV headers it came."""
    supplier = arrival.name.split("/")[1]
    text = data.decode("utf-8-sig")
    reader = csv.DictReader(io.StringIO(text, newline=""))
    layout = layout_of(reader.fieldnames or [])
    family = FAMILY.get(layout.name, layout.name)
    rows = []
    for row in reader:
        out = {"supplier": supplier}
        out.update({f: row[layout.columns[f]]
                    if f in layout.columns else None for f in FIELDS})
        out.update(cost_unit=layout.cost_unit, layout=family)
        rows.append(out)
    key = (suppliers._key(supplier, rows[0]["effective_from"], layout)
           if rows else None)
    return files.Parsed(rows, files.lines(data), key, note=layout.name)


def read(data: bytes, arrival: files.Arrival) -> files.Parsed:
    """A price list of any layout: spreadsheets begin 'PK' (a zip)."""
    if data[:2] == b"PK":
        return suppliers.read_spreadsheet(data, arrival)
    return read_csv(data, arrival)
