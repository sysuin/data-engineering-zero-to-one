# Exercise 2: three more blocking keys, their cost in pairs and what
# they keep of IT's 187 matches.
import duckdb

from groundwork import identity

from _dims import sandbox, tidy

con = duckdb.connect(str(sandbox("s_blocking")))
records = identity.records(con)
it_map = identity.it_map(con)
keys = [
    ("region + month + postcode", identity.block),
    ("region + postcode", lambda r: (r.region, r.postcode)),
    ("first three of postcode", lambda r: r.postcode[:3]),
    ("normalised name", lambda r: identity.normalise(r.name)),
]
print(f"{'blocking key':<27}{'pairs':>9}{'IT kept':>9}"
      f"{'max block':>11}")
for name, key in keys:
    pairs = identity.candidates(records, key)
    found = {(p.a.account_id, p.b.account_id) for p in pairs}
    sizes = {}
    for r in records:
        sizes[key(r)] = sizes.get(key(r), 0) + 1
    kept = sum((o, n) in found for o, n in it_map.items())
    biggest = max(sizes.values())
    print(f"{name:<27}{len(pairs):>9,}{kept:>9}{biggest:>11,}")
tidy("s_blocking")
