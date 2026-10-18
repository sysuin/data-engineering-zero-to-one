# Scoring the 221 candidate pairs by name, before and after normalising,
# then the rule checked against the answer key, which no pipeline reads.
import csv
import json
from difflib import SequenceMatcher
from pathlib import Path

import duckdb

from groundwork import identity
from groundwork.config import TRUTH

from _dims import sandbox, tidy

con = duckdb.connect(str(sandbox("scores")))
res = identity.resolve(con)
pairs = res.candidates

def as_written(a, b):
    return round(SequenceMatcher(None, a, b).ratio(), 3)

bands = [(0, 0.6), (0.6, 0.9), (0.9, 1.0), (1.0, 1.01)]
print(f"{'score':<12}{'as written':>12}{'normalised':>12}")
for lo, hi in bands:
    raw = sum(lo <= as_written(p.a.name, p.b.name) < hi for p in pairs)
    norm = sum(lo <= p.score < hi for p in pairs)
    label = "1.0" if lo == 1.0 else f"{lo} to {hi}"
    print(f"{label:<12}{raw:>12,}{norm:>12,}")
print(f"threshold {identity.THRESHOLD}: matched {len(res.matches)}, "
      f"rejected {len(pairs) - len(res.accepted)}, to review "
      f"{len(res.review)}")

print("\nthree candidate pairs:")
show = [p for p in pairs if p.a.account_id in (90004, 90051)]
show += [min(pairs, key=lambda p: p.score)]
for p in show:
    print(f"  {p.a.name!r:<26} {p.b.name!r:<24} "
          f"{as_written(p.a.name, p.b.name):.2f} {p.score:.2f}")

with open(TRUTH / "legacy_map.csv") as f:      # the answer key
    truth = {int(r["legacy_id"]): int(r["account_id"])
             for r in csv.DictReader(f)}
rules = [
    ("block only", [(p.a.account_id, p.b.account_id) for p in pairs]),
    ("IT's migration map", list(res.it_map.items())),
    ("block + score >= 0.9", list(res.matches.items())),
]
print(f"\nagainst truth/legacy_map.csv ({len(truth)} pairs):")
print(f"{'rule':<24}{'pairs':>7}{'right':>7}{'precision':>11}"
      f"{'recall':>8}")
for name, got in rules:
    right = sum(truth.get(a) == b for a, b in got)
    print(f"{name:<24}{len(got):>7}{right:>7}"
          f"{right / len(got):>11.1%}{right / len(truth):>8.1%}")

def fields(r):
    return {"account_id": r.account_id, "name": r.name,
            "region": r.region, "since": str(r.since),
            "postcode": r.postcode}

pair = next(p for p in pairs if p.a.account_id == 90054)
(sent,) = con.execute("SELECT postcode FROM raw.relay_accounts WHERE "
                      "account_id = '90054'").fetchone()
Path(__file__).with_suffix(".json").write_text(json.dumps({
    "records": {"old": fields(pair.a) | {"as_sent": sent},
                "new": fields(pair.b)},
    "pairs": [[p.a.account_id, p.b.account_id, p.a.name, p.b.name,
               as_written(p.a.name, p.b.name), p.score,
               truth.get(p.a.account_id) == p.b.account_id]
              for p in pairs],
    "threshold": identity.THRESHOLD}, indent=1))
tidy("scores")
