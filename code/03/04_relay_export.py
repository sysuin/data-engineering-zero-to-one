# Relay's nightly export: what is on disk, what a file looks like, and
# how much of it one night changes.
from datetime import datetime, timezone

import duckdb

from groundwork.config import LANDING

relay = LANDING / "relay"
nights = sorted(p.name for p in relay.iterdir() if p.name[:2] == "20")
print(f"{len(nights)} nights on disk: {nights[0]} to {nights[-1]}")

print("file               bytes   arrived (UTC)      first bytes")
for f in sorted((relay / nights[-1]).iterdir()):
    arrived = datetime.fromtimestamp(f.stat().st_mtime, timezone.utc)
    head = f.read_bytes()[:3].hex(" ")
    print(f"{f.name:<14}{f.stat().st_size:>10,}   "
          f"{arrived:%Y-%m-%d %H:%M}   {head}")

con = duckdb.connect()


def export(night: str, table: str) -> str:
    path = relay / night / f"{table}.csv"
    return f"read_csv('{path}', all_varchar = true)"


# The same two files, the night before 1 January and the latest night.
before = "2025-12-31"
print(f"\n{'':<10}{'rows ' + before:>17}{'rows ' + nights[-1]:>17}"
      f"{'new':>7}{'changed':>9}")
for table, key in (("accounts", "account_id"),
                   ("contracts", "contract_id")):
    old, new = export(before, table), export(nights[-1], table)
    (n_old,), (n_new,) = (con.execute(f"SELECT COUNT(*) FROM {x}")
                          .fetchone() for x in (old, new))
    (differ,) = con.execute(f"""SELECT COUNT(*) FROM (
        SELECT * FROM {new} EXCEPT SELECT * FROM {old})""").fetchone()
    (added,) = con.execute(f"""SELECT COUNT(*) FROM {new}
        WHERE {key} NOT IN (SELECT {key} FROM {old})""").fetchone()
    print(f"{table:<10}{n_old:>17,}{n_new:>17,}{added:>7,}"
          f"{differ - added:>9,}")
