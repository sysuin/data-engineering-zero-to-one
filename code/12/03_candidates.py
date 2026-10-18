# Candidate pairs: which records are worth comparing at all, under five
# blocking keys, and how many of IT's 187 matches each one keeps.
from dataclasses import replace
from math import comb

import duckdb

from groundwork import identity

from _dims import sandbox, tidy

con = duckdb.connect(str(sandbox("candidates")))
records = identity.records(con)
it_map = identity.it_map(con)
latest = max(r.last_seen for r in records)
gone = [r for r in records if r.last_seen < latest]
print(f"records {len(records):,}: in today's export "
      f"{len(records) - len(gone):,}, gone since 1 March 2024 "
      f"{len(gone):,}")
print(f"every pair compared: {comb(len(records), 2):,} pairs")

# The postcode exactly as the old CRM's file wrote it, before staging.
as_sent = dict(con.execute(
    "SELECT CAST(account_id AS INTEGER), postcode "
    "FROM raw.relay_accounts WHERE load_date = DATE '2024-02-29'"
).fetchall())
unpadded = [replace(r, postcode=as_sent[r.account_id])
            if r in gone else r for r in records]

keys = [
    ("postcode", records, lambda r: r.postcode),
    ("region + month joined", records, lambda r: (r.region, r.since)),
    ("region + month + postcode", records, identity.block),
    ("  postcode as sent", unpadded, identity.block),
]
print(f"\n{'blocking key':<27}{'pairs':>9}{'old-new':>9}"
      f"{'new-new':>9}{'IT kept':>9}")
for name, recs, key in keys:
    pairs = identity.candidates(recs, key)
    olds = {(p.a.account_id, p.b.account_id) for p in pairs}
    old_new = sum(p.a.last_seen < latest <= p.b.last_seen
                  for p in pairs)
    kept = sum((o, n) in olds for o, n in it_map.items())
    print(f"{name:<27}{len(pairs):>9,}{old_new:>9,}"
          f"{len(pairs) - old_new:>9,}{kept:>9,}")
tidy("candidates")
