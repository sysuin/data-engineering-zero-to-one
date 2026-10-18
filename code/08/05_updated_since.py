# Two weeks of nightly pulls; three ways to say where each one starts.
import os
from datetime import datetime, timedelta

import httpx

from meridian.sources import Clock, frontline
from groundwork.config import SANDBOX  # noqa: F401  (loads .env)

AUTH = {"Authorization": f"Bearer {os.environ['FRONTLINE_TOKEN']}"}
FIRST = datetime(2025, 12, 1)          # everything before is loaded
NIGHTS = 14                            # runs at 06:00, 2 to 15 Dec
SKEW = timedelta(minutes=5)            # our clock runs ahead of theirs
Z = "%Y-%m-%dT%H:%M:%SZ"


def changed_since(desk_time, since):
    """Every ticket changed since `since`, by cursor, at desk_time."""
    base = frontline.serve(Clock(desk_time))
    params, got = {"updated_since": since.strftime(Z), "limit": 100}, []
    with httpx.Client(base_url=base, headers=AUTH) as http:
        while True:
            body = http.get("/api/v2/tickets", params=params).json()
            got += [(t["id"], t["updated_at"]) for t in body["tickets"]]
            if not body["meta"]["has_more"]:
                return got
            params["cursor"] = body["meta"]["after_cursor"]


ways = {"since our last run": None,
        "since newest received": timedelta(0),
        "same, less 1 hour": timedelta(hours=1)}
held = {w: {} for w in ways}
rows = {w: 0 for w in ways}
again = {w: 0 for w in ways}
for night in range(NIGHTS):
    run_at = FIRST + timedelta(days=night + 1, hours=6)  # our clock
    desk = run_at - SKEW                                  # theirs
    for way, overlap in ways.items():
        h = held[way]
        if night == 0:
            since = FIRST
        elif overlap is None:
            since = run_at - timedelta(days=1)    # when we last ran
        else:
            newest = max(u for u in h.values())
            since = datetime.strptime(newest, Z) - overlap
        for tid, upd in changed_since(desk, since):
            rows[way] += 1
            again[way] += h.get(tid) == upd
            h[tid] = max(h.get(tid, ""), upd)

truth = dict(changed_since(desk, FIRST))
print(f"{NIGHTS} nights; at the last pull, {len(truth)} tickets "
      f"changed since {FIRST:%d %b}")
print(f"{'watermark':<23}{'rows':>6}{'again':>7}{'missing':>9}"
      f"{'stale':>7}")
for way in ways:
    h = held[way]
    missing = [t for t in truth if t not in h]
    stale = [t for t in truth if t in h and h[t] < truth[t]]
    print(f"{way:<23}{rows[way]:>6}{again[way]:>7}{len(missing):>9}"
          f"{len(stale):>7}")
    for t in (missing + stale)[:3]:
        print(f"    {t} updated {truth[t][5:16]}, held "
              f"{h[t][5:16] if t in h else 'nothing'}")
