# An empty field is not a zero: two ways to read Relay's discounts.
import csv

from groundwork.config import LANDING

path = LANDING / "relay" / "2026-01-04" / "contracts.csv"


def discount(row):
    """The contract's discount in percent, or None if none is set."""
    text = row["discount_pct"].strip()
    if text == "":
        return None
    return int(text)


with open(path, newline="", encoding="utf-8-sig") as f:
    rows = list(csv.DictReader(f))

values = [discount(row) for row in rows]
known = [v for v in values if v is not None]
empty = [row for row, v in zip(rows, values) if v is None]
as_zero = [0 if v is None else v for v in values]

print(f"contracts                 {len(values):>6,}")
print(f"  discount of 0           {known.count(0):>6,}")
print(f"  discount above 0        {sum(v > 0 for v in known):>6,}")
print(f"  field left empty        {len(empty):>6,}")
legacy = sum(row["legacy_terms"] == "true" for row in empty)
print(f"    of which legacy terms {legacy:>6,}")
print(f"mean discount, empty skipped   {sum(known) / len(known):.2f}%")
print(f"mean discount, empty as 0      "
      f"{sum(as_zero) / len(values):.2f}%")
