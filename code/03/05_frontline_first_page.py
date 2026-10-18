# Frontline's API: a health check, a request without the token, and the
# first page of tickets, one ticket long.
import json
import os
from pathlib import Path

import httpx

from groundwork.config import NOW
from meridian.sources import Clock, frontline

base = frontline.serve(Clock(NOW))    # the desk, as it stood at NOW
url = f"{base}/api/v2/tickets"

r = httpx.get(f"{base}/api/v2/health")
print("health", r.status_code, r.json())

r = httpx.get(url, params={"limit": 1})
print("no token", r.status_code, r.json()["error"])

token = os.environ["FRONTLINE_TOKEN"]   # from .env, which git ignores
r = httpx.get(url, params={"limit": 1},
              headers={"Authorization": f"Bearer {token}"})
print("with token", r.status_code, r.headers["content-type"])
print(json.dumps(r.json(), indent=1, ensure_ascii=False))

# Saved for the chapter's figure.
Path(__file__).with_suffix(".json").write_text(json.dumps(
    {"status": r.status_code, "content_type": r.headers["content-type"],
     "url": "/api/v2/tickets?limit=1", "body": r.json()},
    indent=1, ensure_ascii=False) + "\n", encoding="utf-8")
