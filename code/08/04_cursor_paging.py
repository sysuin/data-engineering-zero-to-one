# The same pull by cursor: nothing skipped, and a check that proves it.
import json
import os
from datetime import datetime, timedelta
from pathlib import Path

import httpx

from meridian.sources import Clock, frontline
from groundwork.config import SANDBOX  # noqa: F401  (loads .env)

AUTH = {"Authorization": f"Bearer {os.environ['FRONTLINE_TOKEN']}"}
SINCE = "2025-11-25T00:00:00Z"
START = datetime(2025, 12, 2, 20, 0)
TICK = timedelta(minutes=3)


def total(http):
    """The API's own count, from a one-ticket numbered page."""
    body = http.get("/api/v2/tickets", params={
        "updated_since": SINCE, "page": 1, "per_page": 1}).json()
    return body["meta"]["total"]


def by_cursor(http, limit=10):
    """Hand back the cursor until has_more is false."""
    params = {"updated_since": SINCE, "limit": limit}
    while True:
        body = http.get("/api/v2/tickets", params=params).json()
        yield body["tickets"]
        if not body["meta"]["has_more"]:
            return
        params["cursor"] = body["meta"]["after_cursor"]


moving = Clock(START)
base = frontline.serve(moving, tick=TICK)
with httpx.Client(base_url=base, headers=AUTH) as http:
    before = total(http)
    pages = list(by_cursor(http))
    last = moving.now - TICK          # the desk's time at the last page
    after = total(http)

got = [t for page in pages for t in page]
latest = {}
for t in got:                      # keep each ticket's newest copy
    if t["id"] not in latest or t["updated_at"] > \
            latest[t["id"]]["updated_at"]:
        latest[t["id"]] = t
print(f"pages {len(pages)}, rows received {len(got)}, "
      f"distinct ids {len(latest)}")
print(f"API total before {before}, after {after}: "
      f"{before} <= {len(latest)} <= {after} is "
      f"{before <= len(latest) <= after}")
for tid in ["FT-020860", "FT-021162"]:
    seen = [t["updated_at"][5:16] for t in got if t["id"] == tid]
    print(f"  {tid} received {len(seen)}x: {', '.join(seen)}; "
          f"kept {latest[tid]['updated_at'][5:16]}")
for tid in ["FT-020648", "FT-021086"]:
    print(f"  {tid} received: {tid in latest}")

still = Clock(last)
with httpx.Client(base_url=frontline.serve(still), headers=AUTH) as h:
    truth = {t["id"]: t["updated_at"] for page in by_cursor(h, 100)
             for t in page}
same = sum(latest[i]["updated_at"] == u for i, u in truth.items()
           if i in latest)
print(f"standing desk at {last:%H:%M}: {len(truth)}; the pull "
      f"holds {same} as they stood")
Path(__file__).with_suffix(".json").write_text(json.dumps(
    {"pages": len(pages), "rows": len(got), "distinct": len(latest),
     "before": before, "after": after}, indent=1) + "\n")
