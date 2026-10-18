# One portal event as it was sent, and the day its shape changed.
import gzip
import json

import duckdb

from groundwork.config import LANDING

PORTAL = LANDING / "portal"


def first_cart(hour: str) -> dict:
    """The first add_to_cart in one hourly file."""
    with gzip.open(PORTAL / f"{hour}.jsonl.gz", "rt") as f:
        return next(e for e in map(json.loads, f)
                    if e["event_type"] == "add_to_cart")


event = first_cart("2025/09/16/14")
print("From portal/2025/09/16/14.jsonl.gz:")
for key, value in event.items():
    print(f"  {key:<15} {json.dumps(value)}")

later = first_cart("2025/12/09/14")
print("\nAn add_to_cart from portal/2025/12/09/14.jsonl.gz:")
print(f"  {'schema_version':<15} {later['schema_version']}")
for key, value in later["properties"].items():
    print(f"  properties.{key:<18} {json.dumps(value)}")

con = duckdb.connect()
con.execute(f"""
    CREATE TABLE events AS
    SELECT event_type, schema_version AS v, device, properties,
           left(occurred_at, 7) AS month
    FROM read_json('{PORTAL}/*/*/*/*.jsonl.gz',
                   columns = {{event_type: 'VARCHAR',
                              schema_version: 'INTEGER',
                              device: 'VARCHAR', properties: 'JSON',
                              occurred_at: 'VARCHAR'}})""")

print("\nEvents by schema version, by month on the device's clock")
print(f"  {'month':<8} {'desktop v1':>11} {'desktop v2':>11}"
      f" {'mobile v1':>10} {'mobile v2':>10}")
for row in con.execute("""
        PIVOT (SELECT month, device || ' v' || v AS kind FROM events
               WHERE month BETWEEN '2025-07' AND '2025-12')
        ON kind IN ('desktop v1', 'desktop v2', 'mobile v1',
                    'mobile v2')
        USING count(*) ORDER BY month""").fetchall():
    print(f"  {row[0]:<8} {row[1]:>11,} {row[2]:>11,} {row[3]:>10,}"
          f" {row[4]:>10,}")

print("\nProduct views, counted by properties.sku (the old name)")
for month, views, named in con.execute("""
        SELECT month, count(*), count(properties->>'sku')
        FROM events WHERE event_type = 'product_view'
          AND month BETWEEN '2025-07' AND '2025-12'
        GROUP BY month ORDER BY month""").fetchall():
    print(f"  {month}  {views:>7,} views, {named:>7,} with a sku"
          f"  ({named / views:.0%})")
