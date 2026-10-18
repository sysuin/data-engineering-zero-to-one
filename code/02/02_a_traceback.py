# expect-fail
# Relay's contracts, one conversion too trusting, and its traceback.
import csv

from groundwork.config import LANDING

path = LANDING / "relay" / "2026-01-04" / "contracts.csv"


def discount(row):
    return int(row["discount_pct"])


with open(path, newline="", encoding="utf-8-sig") as f:
    rows = list(csv.DictReader(f))

print(f"{len(rows):,} contracts; adding up their discounts")
total = 0
for row in rows:
    total = total + discount(row)
print(f"total discount: {total}")
