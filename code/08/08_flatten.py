# One ticket, flattened: a ticket row, field rows and tag rows.
import json
from pathlib import Path

import httpx

from meridian.sources import Clock, frontline
from groundwork.config import NOW
from groundwork.ingest.frontline import flatten, token

base = frontline.serve(Clock(NOW))
r = httpx.get(f"{base}/api/v2/tickets/FT-021198",
              headers={"Authorization": f"Bearer {token()}"})
ticket = r.json()["ticket"]
nested = [f"{k} ({type(v).__name__})" for k, v in ticket.items()
          if isinstance(v, (dict, list))]
print(f"received: {len(ticket)} keys, {len(nested)} of them nested:")
print("  " + ", ".join(nested))

row, fields, tags = flatten(ticket, page=1)
print(f"tickets: 1 row, {len(row)} columns")
for k, v in row.items():
    if k != "payload":
        shown = repr(v)
        if len(shown) > 38:
            shown = shown[:34] + "...'"
        print(f"  {k:<21}{shown}")
print(f"  {'payload':<21}{len(row['payload'])} characters of JSON")
print(f"ticket_fields: {len(fields)} rows, keyed by ticket_id and "
      "updated_at")
for f in fields:
    print(f"  {f['ticket_id']}  {f['field_id']}  {f['name']:<9}"
          f"{f['value']}")
print(f"ticket_tags: {len(tags)} rows")
for t in tags:
    print(f"  {t['ticket_id']}  {t['position']}  {t['tag']}")
print("payload read back equals what was sent:",
      json.loads(row["payload"]) == ticket)

Path(__file__).with_suffix(".json").write_text(json.dumps(
    {"ticket": ticket, "row": {k: v for k, v in row.items()
                               if k != "payload"},
     "payload_chars": len(row["payload"]), "fields": fields,
     "tags": tags}, indent=1, ensure_ascii=False) + "\n",
    encoding="utf-8")
