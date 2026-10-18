# Six requests, six answers: what each status code asks of the caller.
import os

import httpx

from meridian.sources import Clock, frontline
from groundwork.config import SANDBOX  # noqa: F401  (loads .env)

# A desk that refuses every 5th request and fails every 6th.
base = frontline.serve(Clock("2025-12-02 09:00"), rate_limit=5,
                       fail_every=6)
good = {"Authorization": f"Bearer {os.environ['FRONTLINE_TOKEN']}"}
page = {"updated_since": "2025-12-01", "limit": 1}
asks = [
    ("a page of tickets", "/api/v2/tickets", page, good),
    ("page number 0", "/api/v2/tickets", {"page": 0}, good),
    ("no token", "/api/v2/tickets", page, {}),
    ("a ticket that isn't", "/api/v2/tickets/FT-999999", {}, good),
    ("the 5th request", "/api/v2/tickets", page, good),
    ("the 6th request", "/api/v2/tickets", page, good),
]


def verdict(r):
    """What a loader should do with this answer."""
    if r.status_code == 200:
        return "use it"
    if r.status_code == 429:
        return f"wait {r.headers['Retry-After']} s, ask again"
    if r.status_code >= 500:
        return "wait a while, ask again"
    return "stop: a retry won't help"


print(f"{'asked for':<20}{'status':>6}  {'error':<14}what to do")
with httpx.Client(base_url=base) as http:
    for what, path, params, headers in asks:
        r = http.get(path, params=params, headers=headers)
        error = r.json().get("error", "-") if r.status_code != 200 \
            else "-"
        print(f"{what:<20}{r.status_code:>6}  {error:<14}{verdict(r)}")
