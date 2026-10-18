# Exercise 1: numbered and cursor pulls, ticket by ticket, three ticks.
import os
from datetime import datetime, timedelta

import httpx

from meridian.sources import Clock, frontline
from groundwork.config import SANDBOX  # noqa: F401  (loads .env)

AUTH = {"Authorization": f"Bearer {os.environ['FRONTLINE_TOKEN']}"}
SINCE = "2025-11-25T00:00:00Z"
START = datetime(2025, 12, 2, 20, 0)


def numbered(http, n=10):
    page = 1
    while True:
        got = http.get("/api/v2/tickets", params={
            "updated_since": SINCE, "page": page, "per_page": n}).json()
        yield got["tickets"]
        if len(got["tickets"]) < n:
            return
        page += 1


def cursor(http, n=10):
    params = {"updated_since": SINCE, "limit": n}
    while True:
        got = http.get("/api/v2/tickets", params=params).json()
        yield got["tickets"]
        if not got["meta"]["has_more"]:
            return
        params["cursor"] = got["meta"]["after_cursor"]


def compare(way, tick):
    """Pull while the desk moves; compare with it standing still."""
    clock = Clock(START)
    with httpx.Client(base_url=frontline.serve(clock, tick=tick),
                      headers=AUTH) as http:
        held = {}
        for page in way(http):
            for t in page:
                held[t["id"]] = max(held.get(t["id"], ""),
                                    t["updated_at"])
    last = clock.now - tick
    with httpx.Client(base_url=frontline.serve(Clock(last)),
                      headers=AUTH) as http:
        truth = {t["id"]: t["updated_at"] for page in cursor(http, 100)
                 for t in page}
    missing = sorted(set(truth) - set(held))
    old = sorted(i for i in truth if i in held and held[i] < truth[i])
    extra = sorted(set(held) - set(truth))
    return len(truth), missing, old, extra


print(f"{'pull':<9}{'tick':>6}{'desk':>6}  missing / old / extra")
for minutes in (1, 2, 3):
    for name, way in (("numbered", numbered), ("cursor", cursor)):
        tick = timedelta(minutes=minutes)
        n, missing, old, extra = compare(way, tick)
        shown = f"{len(missing)} / {len(old)} / {len(extra)}"
        print(f"{name:<9}{minutes:>4} m{n:>6}  {shown}  "
              f"{' '.join(missing)}".rstrip())
