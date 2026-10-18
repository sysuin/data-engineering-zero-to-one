# The catalogue beside procurement's master and Tradeline's own SKUs.
import csv

from groundwork.config import LANDING, REFERENCE
from meridian.sources import tradeline

CATALOGUE = LANDING / "catalogue" / "catalogue.csv"
with open(CATALOGUE, encoding="utf-8") as f:
    catalogue = list(csv.DictReader(f))
with open(REFERENCE / "suppliers.csv", encoding="utf-8") as f:
    master = {row["name"] for row in csv.DictReader(f)}
sold = {s for (s,) in tradeline.connect().execute(
    "SELECT DISTINCT sku FROM order_lines")}

print(f"catalogue: {len(catalogue)} rows; its first two lines:")
with open(CATALOGUE, encoding="utf-8") as f:
    print(f"  {next(f).rstrip()}\n  {next(f).rstrip()}")

names = sorted({r["Supplier"] for r in catalogue})
unmatched = [n for n in names if n not in master]
print(f"\nsupplier names in the catalogue: {len(names)}")
print(f"  not written as procurement writes them: {len(unmatched)}")
for n in unmatched:
    print(f"    {n!r}")

skus = [r["SKU"] for r in catalogue]
missing = [s for s in skus if s not in sold]
print(f"\nSKUs in the catalogue: {len(skus)}; "
      f"Tradeline sells {len(sold)}")
print(f"  not written as Tradeline writes them: {len(missing)}")
for s in missing:
    print(f"    {s!r}")
