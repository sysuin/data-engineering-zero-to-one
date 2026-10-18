# Exercise 3: precision and recall of the rule at several thresholds,
# names as written and normalised, scored against the answer key.
import csv
from difflib import SequenceMatcher

import duckdb

from groundwork import identity
from groundwork.config import TRUTH

from _dims import sandbox, tidy

con = duckdb.connect(str(sandbox("s_threshold")))
res = identity.resolve(con)
latest = max(p.b.last_seen for p in res.candidates)
merges = [p for p in res.candidates if p.a.last_seen < latest]
with open(TRUTH / "legacy_map.csv") as f:          # the answer key
    truth = {int(r["legacy_id"]): int(r["account_id"])
             for r in csv.DictReader(f)}

def written(p):
    return SequenceMatcher(None, p.a.name, p.b.name).ratio()

print(f"{'threshold':<10}{'as written':>22}{'normalised':>22}")
print(f"{'':<10}{'precision':>11}{'recall':>11}{'precision':>11}"
      f"{'recall':>11}")
for t in (0.3, 0.5, 0.7, 0.8, 0.9, 0.95):
    cells = []
    for score in (written, lambda p: p.score):
        got = [p for p in res.candidates if score(p) >= t]
        right = sum(truth.get(p.a.account_id) == p.b.account_id
                    for p in got)
        cells += [right / len(got) if got else 1.0, right / len(truth)]
    print(f"{t:<10}" + "".join(f"{x:>11.1%}" for x in cells))
print(f"candidate pairs where one side is gone: {len(merges)}")
tidy("s_threshold")
