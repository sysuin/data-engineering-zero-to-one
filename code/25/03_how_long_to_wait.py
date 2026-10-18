# How long a window must wait for device data: the portal as a proxy.
import duckdb

from groundwork.config import LANDING

WAITS = [("10 seconds", 10), ("1 minute", 60), ("1 hour", 3600),
         ("6 hours", 6 * 3600), ("1 day", 86_400),
         ("3 days", 3 * 86_400), ("7 days", 7 * 86_400)]

con = duckdb.connect()
con.execute(f"""
    CREATE TABLE delay AS
    SELECT device,
           epoch(received_at::TIMESTAMP - occurred_at::TIMESTAMP)
               AS seconds
    FROM read_json('{LANDING}/portal/*/*/*/*.jsonl.gz',
         format = 'newline_delimited',
         columns = {{'device': 'VARCHAR', 'occurred_at': 'VARCHAR',
                    'received_at': 'VARCHAR'}})
""")
totals = dict(con.execute("""SELECT device, COUNT(*) FROM delay
    GROUP BY device ORDER BY device""").fetchall())
print(f"events: {sum(totals.values()):,}   " + "   ".join(
    f"{d}: {n:,}" for d, n in totals.items()))
ahead = dict(con.execute("""SELECT device, COUNT(*) FROM delay
    WHERE seconds < 0 GROUP BY device ORDER BY device""").fetchall())
print("received before they happened: " + "   ".join(
    f"{d}: {ahead.get(d, 0):,}" for d in totals))

print(f"\n{'close a window after':22}"
      f"{'still to come, per million':>28}")
print(f"{'':22}{'all':>10}{'desktop':>10}{'mobile':>10}")
for label, wait in WAITS:
    late = dict(con.execute("""SELECT device, COUNT(*) FROM delay
        WHERE seconds > ? GROUP BY device ORDER BY device""",
        [wait]).fetchall())
    per_m = [1e6 * late.get(d, 0) / totals[d] for d in totals]
    everyone = 1e6 * sum(late.values()) / sum(totals.values())
    print(f"{label:22}{everyone:>10,.0f}" + "".join(
        f"{x:>10,.0f}" for x in per_m))
