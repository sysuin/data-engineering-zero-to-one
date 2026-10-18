# Aldridge's spreadsheet read three ways: as a table, as cells, and
# by a reader that looks for its header and its date.
from collections import Counter

import openpyxl
import pandas as pd

from groundwork.config import LANDING
from groundwork.ingest import suppliers
from groundwork.ingest.files import discover

folder = LANDING / "suppliers" / "aldridge-co"
path = folder / "Aldridge Trade Prices Mar 2024.xlsx"

df = pd.read_excel(path)                 # as if it were a table
print(f"pandas: {df.shape[0]} rows, and these columns:")
for name in df.columns:
    print(f"  {name!r}")
print(f"first row: {df.iloc[0].tolist()}")

print("\nopenpyxl, every cell that holds something:")
sheet = openpyxl.load_workbook(path).worksheets[0]
for row in sheet.iter_rows():
    cells = [f"{c.coordinate}={c.value!r}" for c in row
             if c.value is not None]
    if cells:
        print("  " + "  ".join(cells))

a = next(x for x in discover(LANDING, "suppliers/aldridge-co")
         if x.path == path)
p = suppliers.read(path.read_bytes(), a)
print(f"\nGroundwork's reader: key {p.logical_key}, {p.counted} rows")
for r in p.rows:
    print(f"  {r['sku']}  {r['cost']:>6}  {r['effective_from']}")

# Is every one of the 49 files laid out the same way?
shapes = Counter()
for f in sorted(folder.glob("*.xlsx")):
    book = openpyxl.load_workbook(f)
    ws = book.worksheets[0]
    at = next(r for r in range(1, ws.max_row + 1)
              if ws.cell(r, 1).value == "Product Code")
    banner = str(ws.cell(2, 1).value)
    shapes[(tuple(book.sheetnames), len(ws.merged_cells.ranges), at,
            bool(suppliers.BANNER_DATE.search(banner)))] += 1
print(f"\n{sum(shapes.values())} files; sheets, merged ranges, "
      "header row, date in banner:")
for shape, n in shapes.items():
    print(f"  {n} x {shape}")
