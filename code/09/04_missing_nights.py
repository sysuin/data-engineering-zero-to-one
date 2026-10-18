# timeout: 600
# Four years of nights replayed: one export expected each night, and an
# alert for every night the folder was empty at 06:00.
import shutil
from datetime import date, datetime, time, timedelta

from groundwork.config import SANDBOX
from groundwork.ingest.files import discover, fingerprint
from meridian.sources import relay

here = SANDBOX / "ch09" / "nights"
shutil.rmtree(here, ignore_errors=True)
EXPECTED = {"accounts.csv", "contracts.csv"}

first, last = date(2022, 1, 1), date(2026, 1, 4)
alerts, arrived, same_bytes, previous = [], 0, 0, None
night = first
while night <= last:
    relay.export(night, here)            # the night, as it landed
    run_at = datetime.combine(night + timedelta(days=1), time(6))
    found = {a.path.name: a
             for a in discover(here, night.isoformat(), by=run_at)}
    if EXPECTED - set(found):
        alerts.append((night, sorted(EXPECTED - set(found))))
    else:
        arrived += 1
        sha = fingerprint(found["accounts.csv"].path)
        same_bytes += sha == previous
        previous = sha
    shutil.rmtree(here / night.isoformat(), ignore_errors=True)
    night += timedelta(days=1)

expected = (last - first).days + 1
print(f"nights expected {expected:,}, arrived {arrived:,}, "
      f"missing {len(alerts)}")
for night, what in alerts:
    print(f"  ALERT {night} {night:%a}: no {' or '.join(what)} "
          "by 06:00")
print(f"\naccounts.csv unchanged from the night before: "
      f"{same_bytes:,} of {arrived:,} nights")

# The archive's own record of which nights it exported.
record = {date.fromisoformat(n) for n, done, _ in relay.nights()
          if not done}
print(f"the archive's record of nights not exported: {len(record)}; "
      f"same nights: {record == {n for n, _ in alerts}}")
shutil.rmtree(here, ignore_errors=True)
