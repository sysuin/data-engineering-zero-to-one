# Read Relay's accounts export as rows, keep some, write a new file.
import csv

from groundwork.config import LANDING, SANDBOX

NIGHT = "2026-01-04"                    # the latest export on disk
source = LANDING / "relay" / NIGHT / "accounts.csv"
target = SANDBOX / "ch02" / "key_accounts.csv"

# Relay's files begin with a byte-order mark; utf-8-sig removes it.
with open(source, newline="", encoding="utf-8-sig") as f:
    rows = list(csv.DictReader(f))      # a list of dicts, one per row

print(f"read {len(rows):,} rows from relay/{NIGHT}/{source.name}")
first = rows[0]
for name in ["account_id", "account_name", "region",
             "key_account", "customer_since"]:
    print(f"  {name:15} {first[name]!r}")


def is_key(row):
    """True if Relay marks the account as a key account."""
    return row["key_account"] == "true"


naive = [row for row in rows if row["key_account"]]
print(f"naive filter keeps {len(naive):,}; is_key keeps "
      f"{sum(1 for row in rows if is_key(row))}")

keep = ["account_id", "account_name", "region", "segment", "status"]
key_rows = [{k: row[k] for k in keep} for row in rows if is_key(row)]

target.parent.mkdir(parents=True, exist_ok=True)
with open(target, "w", newline="", encoding="utf-8") as f:
    writer = csv.DictWriter(f, fieldnames=keep)
    writer.writeheader()
    writer.writerows(key_rows)

with open(target, newline="", encoding="utf-8") as f:
    back = list(csv.DictReader(f))
print(f"wrote {len(key_rows)} rows to {target.name}, "
      f"read back {len(back)}")

by_region = {}
for row in back:
    by_region[row["region"]] = by_region.get(row["region"], 0) + 1
for region, n in sorted(by_region.items()):
    print(f"  {region:10} {n:>3}")
