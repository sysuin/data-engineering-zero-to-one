# One Frontline ticket as JSON: dicts inside dicts, and lists of dicts.
import json
import os
import pprint
from pathlib import Path
from urllib.request import Request, urlopen

from groundwork.config import NOW, SANDBOX
from meridian.sources import Clock, frontline

base = frontline.serve(Clock(NOW))      # the desk's API, run locally
token = os.environ["FRONTLINE_TOKEN"]   # from .env, never in code
request = Request(f"{base}/api/v2/tickets/FT-020866",
                  headers={"Authorization": f"Bearer {token}"})
with urlopen(request) as response:
    body = response.read().decode("utf-8")
print(f"received {len(body)} characters of text")


def walk(value, path="ticket"):
    """Print every leaf under value with its path and type."""
    if isinstance(value, dict):
        for key, inner in value.items():
            walk(inner, f"{path}.{key}")
    elif isinstance(value, list):
        for i, inner in enumerate(value):
            walk(inner, f"{path}[{i}]")
    else:
        shown = repr(value) if len(repr(value)) <= 24 else "..."
        print(f"  {path:30} {type(value).__name__:8} {shown}")


ticket = json.loads(body)["ticket"]     # text in, dicts and lists out
walk({k: ticket[k] for k in ["id", "created_at", "status",
                             "requester", "custom_fields", "tags"]})

fields = {f["name"]: f["value"] for f in ticket["custom_fields"]}
row = {"ticket_id": ticket["id"],
       "account_id": ticket["requester"]["account_id"],
       "status": ticket["status"],
       "desk": fields["desk"],
       "category": fields["category"]}
print("ticket.get('closed_at') ->", ticket.get("closed_at"))

out = SANDBOX / "ch02" / "tickets.jsonl"
with open(out, "w", encoding="utf-8") as f:
    f.write(json.dumps(row) + "\n")      # one JSON object per line
lines = out.read_text(encoding="utf-8").splitlines()
print(f"wrote {len(lines)} line to {out.name}; read back:")
pprint.pprint(json.loads(lines[0]), width=66, sort_dicts=False)

print("three texts json.loads refuses:")
bad = [("cut", body[:120]), ("quotes", "{'id': 'FT-020866'}"),
       ("empty", "")]
for label, text in bad:
    try:
        json.loads(text)
    except json.JSONDecodeError as e:
        print(f"  {label:6} {e.msg} (char {e.pos})")

# The ticket as received, for the chapter's annotated figure.
Path(__file__).with_suffix(".json").write_text(body, encoding="utf-8")
