# Numbered pages while tickets change: the count agrees; the ids do not.
import json
import os
from datetime import datetime, timedelta
from pathlib import Path

import httpx

from meridian.sources import Clock, frontline
from groundwork.config import SANDBOX  # noqa: F401  (loads .env)

AUTH = {"Authorization": f"Bearer {os.environ['FRONTLINE_TOKEN']}"}
SINCE = "2025-11-25T00:00:00Z"        # a week of changed tickets
START = datetime(2025, 12, 2, 20, 0)  # Tuesday evening, UTC
TICK = timedelta(minutes=3)           # the desk moves on per request


def by_page(base, per_page=10):
    """Ask for page 1, 2, 3 ... until a page comes back short."""
    pages, n = [], 1
    with httpx.Client(base_url=base, headers=AUTH) as http:
        while True:
            body = http.get("/api/v2/tickets", params={
                "updated_since": SINCE, "page": n,
                "per_page": per_page}).json()
            pages.append(body)
            if len(body["tickets"]) < per_page:
                return pages
            n += 1


moving = Clock(START)
pages = by_page(frontline.serve(moving, tick=TICK))
last = moving.now - TICK              # the desk's time at the last page
got = [(t["id"], t["updated_at"], i + 1)
       for i, p in enumerate(pages) for t in p["tickets"]]
ids = [g[0] for g in got]
print(f"pages {len(pages)}, {START:%H:%M} to {last:%H:%M} on the desk")
print(f"rows received {len(got):>4}")
print(f"meta.total    {pages[-1]['meta']['total']:>4}   (last page)")
print(f"distinct ids  {len(set(ids)):>4}")

# The same question asked of a desk that stands still at `last`.
still = by_page(frontline.serve(Clock(last)))
truth = [t for p in still for t in p["tickets"]]
order = [t["id"] for t in truth]
missing = sorted(set(order) - set(ids))
twice = sorted({i for i in ids if ids.count(i) > 1})
print(f"standing desk {len(truth):>4}   (every ticket, once)")
print("received twice:")
for tid in twice:
    seen = [(u, pg) for i, u, pg in got if i == tid]
    print("  " + tid + "".join(f"  p{pg} {u[5:16]}" for u, pg in seen))
print("never received:")
for tid in missing:
    k = order.index(tid)
    t = truth[k]
    print(f"  {tid}  updated {t['updated_at'][5:16]}, "
          f"#{k + 1} of {len(truth)}, after {order[k - 1]}")

# What the pull saw around the first skipped ticket, for the figure.
k = min(order.index(tid) for tid in missing)
pg = next(pg for i, u, pg in got if i == order[k - 1])
Path(__file__).with_suffix(".json").write_text(json.dumps({
    "since": SINCE, "start": f"{START:%Y-%m-%d %H:%M}",
    "tick_minutes": TICK.seconds // 60, "per_page": 10,
    "rows": len(got), "total": pages[-1]["meta"]["total"],
    "distinct": len(set(ids)), "truth": len(truth),
    "twice": {tid: [(u, pg) for i, u, pg in got if i == tid]
              for tid in twice},
    "missing": missing, "skipped": order[k], "page": pg,
    "fetched": [[i, u] for i, u, p in got if p in (pg, pg + 1)],
    "standing": [[t["id"], t["updated_at"]]
                 for t in truth[k - 6:k + 6]],
}, indent=1) + "\n", encoding="utf-8")
