# Exercise 3: is "closed 96 hours after solved" a rule Frontline keeps?
# The gaps between the two, for every ticket nightly pulls saw both.
# timeout: 600
import json
from collections import Counter
from datetime import datetime, timedelta

from meridian.sources import Clock, frontline

from groundwork.ingest.frontline import Client, pull

clock = Clock("2025-10-01 06:00")
client = Client(frontline.serve(clock))
seen, since = {}, "2025-10-01T00:00:00Z"
while clock.now < datetime(2026, 1, 5, 6, 0, tzinfo=clock.now.tzinfo):
    for body in pull(client, since):
        for t in json.loads(body)["tickets"]:
            if t["status"] in ("solved", "closed"):
                seen.setdefault(t["id"], {}).setdefault(
                    t["status"], t["updated_at"])
    since = (clock.now - timedelta(hours=1)).strftime(
        "%Y-%m-%dT%H:%M:%SZ")
    clock.advance(days=1)
client.close()
both = {k: v for k, v in seen.items() if len(v) == 2}
at = lambda s: datetime.fromisoformat(s.rstrip("Z"))      # noqa: E731
gaps = Counter((at(v["closed"]) - at(v["solved"])) / timedelta(hours=1)
               for v in both.values())
print(f"tickets seen solved and then closed: {len(both):,}")
for hours, n in sorted(gaps.items()):
    print(f"  closed {hours:6.2f} hours after solved: {n:>5,}")
