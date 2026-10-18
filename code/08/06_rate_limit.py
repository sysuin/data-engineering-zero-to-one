# A desk that allows three requests in four: crash, or wait as told.
import os
import time

import httpx

from meridian.sources import Clock, frontline
from groundwork.config import SANDBOX  # noqa: F401  (loads .env)

AUTH = {"Authorization": f"Bearer {os.environ['FRONTLINE_TOKEN']}"}
FIRST = {"updated_since": "2025-12-01T00:00:00Z", "limit": 25}


def pull(http, polite):
    """Follow the cursor; if polite, honour Retry-After on a 429."""
    params, tickets, waited = dict(FIRST), 0, 0
    while True:
        r = http.get("/api/v2/tickets", params=params)
        if polite and r.status_code == 429:
            wait = int(r.headers["Retry-After"])
            time.sleep(wait)                   # a real second
            waited += wait
            continue
        r.raise_for_status()
        body = r.json()
        tickets += len(body["tickets"])
        if not body["meta"]["has_more"]:
            return tickets, waited
        params["cursor"] = body["meta"]["after_cursor"]


for polite in (False, True):
    base = frontline.serve(Clock("2025-12-08 06:00"), rate_limit=4)
    print("honours Retry-After:" if polite else "raises on any error:")
    try:
        with httpx.Client(base_url=base, headers=AUTH) as http:
            tickets, waited = pull(http, polite)
        print(f"  {tickets} tickets, {waited} s spent waiting")
    except httpx.HTTPStatusError as e:
        print(f"  {type(e).__name__}: "
              f"{str(e).split(' for url')[0]}")
    codes = [str(s) for _, s in frontline.requests_made()]
    print("  the server answered:")
    for i in range(0, len(codes), 12):
        print("    " + " ".join(codes[i:i + 12]))
