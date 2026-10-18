# Classify every column in raw, staging, intermediate and the marts:
# by its name first, then by scanning what the text columns hold.
import json
from collections import Counter
from pathlib import Path

from groundwork.govern import classify as k

from _govern import base

con = base().connect()
columns = k.classify(con)
k.save(con, columns)

count = Counter((c.layer, c.cls) for c in columns)
print(f"{'layer':<14}{'personal':>9}{'confidential':>14}{'public':>8}"
      f"{'all':>6}")
for layer in k.LAYERS + ("all",):
    row = [sum(n for (lay, cls), n in count.items()
               if cls == want and layer in (lay, "all"))
           for want in (k.PERSONAL, k.CONFIDENTIAL, k.PUBLIC)]
    print(f"{layer:<14}{row[0]:>9}{row[1]:>14}{row[2]:>8}{sum(row):>6}")

by_content = [c for c in columns if c.reason.startswith("holds")]
print("\npersonal by what it holds, not by its name: "
      f"{len(by_content)}")
for c in by_content:
    print(f"  {c.relation + '.' + c.column:<44}{c.hits:>7,} rows")
loose = [c for c in columns if c.reason == k.UNREVIEWED]
print(f"no rule, confidential until reviewed: {len(loose)}")
for c in loose:
    print(f"  {c.relation}.{c.column}")

customer = {}
for c in columns:
    if c.cls == k.PERSONAL and "colleague" not in c.reason:
        customer.setdefault(c.relation, []).append(c.column)
print(f"\nrelations holding customers' personal data: {len(customer)}")
for relation, cols in customer.items():
    print(f"  {relation:<40}{len(cols)} column{'s' * (len(cols) > 1)}")

Path(__file__).with_suffix(".json").write_text(json.dumps({
    "layers": {layer: {cls: count[(layer, cls)] for cls in
                       (k.PERSONAL, k.CONFIDENTIAL, k.PUBLIC)}
               for layer in k.LAYERS},
    "customer": customer}, indent=1) + "\n")
