# One hour of portal events: what JSON says about types, and what not.
import gzip
import json

import duckdb

from groundwork.config import LANDING

path = LANDING / "portal" / "2025" / "07" / "17" / "19.jsonl.gz"
with gzip.open(path, "rt", encoding="utf-8") as f:
    events = [json.loads(line) for line in f]
print(f"{path.name}: {len(events):,} events, one JSON object a line")

first = events[0]
for key, value in first.items():
    print(f"  {key:<15} {type(value).__name__:<5} {value!r:.44}")

# DuckDB reads the same file and guesses a type for every key.
con = duckdb.connect()
types = con.execute(f"""SELECT column_name, column_type FROM
    (DESCRIBE SELECT * FROM read_json('{path}'))""").fetchall()
print("\nDuckDB's guesses:")
for name, kind in types:
    if name in ("account_id", "occurred_at"):
        print(f"  {name:<12} {kind}")
    if name == "properties":            # a long type: a field a line
        inner = kind.removeprefix("STRUCT(").removesuffix(")")
        print(f"  {name:<12} STRUCT, the union of every event's keys:")
        for field in inner.split(", "):
            print(f"    {field}")
shapes = {(e["event_type"], tuple(e["properties"])) for e in events}
print(f"event types this hour, each with its own keys: {len(shapes)}")
