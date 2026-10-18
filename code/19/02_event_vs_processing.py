# When each event happened, against when the portal received it.
import json
from pathlib import Path

import duckdb

from groundwork.config import LANDING

con = duckdb.connect()
con.execute(f"""
    CREATE TABLE e AS
    SELECT event_id, device, filename AS file,
           strptime(occurred_at, '%Y-%m-%dT%H:%M:%S.%gZ') AS event_t,
           strptime(received_at, '%Y-%m-%dT%H:%M:%S.%gZ') AS proc_t
    FROM read_json('{LANDING}/portal/*/*/*/*.jsonl.gz', filename = 1,
                   columns = {{event_id: 'VARCHAR', device: 'VARCHAR',
                              occurred_at: 'VARCHAR',
                              received_at: 'VARCHAR'}})""")
con.execute("""ALTER TABLE e ADD COLUMN lag DOUBLE;
               UPDATE e SET lag = epoch(proc_t) - epoch(event_t)""")


def span(seconds: float) -> str:
    """A delay in the unit that suits it."""
    if abs(seconds) < 60:
        return f"{seconds:.1f} s"
    if abs(seconds) < 3600:
        return f"{seconds / 60:.1f} m"
    return f"{seconds / 3600:.1f} h"


print("Delay, received_at - occurred_at, every event as delivered")
print(f"  {'device':<8} {'events':>8} {'median':>7} {'99th':>7}"
      f" {'longest':>8} {'> 1 hour':>9} {'future':>7}")
for d, n, med, p99, most, late, future in con.execute("""
        SELECT device, count(*), median(lag), quantile_cont(lag, 0.99),
               max(lag), count(*) FILTER (WHERE lag > 3600),
               count(*) FILTER (WHERE lag < 0)
        FROM e GROUP BY device ORDER BY device""").fetchall():
    print(f"  {d:<8} {n:>8,} {span(med):>7} {span(p99):>7}"
          f" {span(most):>8} {late:>9,} {future:>7,}")

(wrong, total) = con.execute("""
    SELECT count(*) FILTER (WHERE strftime(event_t, '%Y/%m/%d/%H')
                            <> regexp_extract(file,
                                 '(\\d{4}/\\d\\d/\\d\\d/\\d\\d)', 1)),
           count(*) FROM e""").fetchone()
print(f"\nEvents filed under an hour they did not happen in: "
      f"{wrong:,} of {total:,}")

hour = "2025-12-09 16:00:00"
in_file, happened, both = con.execute(f"""
    SELECT count(*) FILTER (WHERE fh), count(*) FILTER (WHERE eh),
           count(*) FILTER (WHERE fh AND eh)
    FROM (SELECT date_trunc('hour', proc_t) = '{hour}' AS fh,
                 date_trunc('hour', event_t) = '{hour}' AS eh
          FROM e)""").fetchone()
print(f"The hour from 16:00 UTC on 9 December 2025:\n"
      f"  {in_file} events in its file; {happened} happened in it\n"
      f"  {in_file - both} in the file happened earlier; "
      f"{happened - both} that happened in it came later")

# For the figure: three days of event time, every event more than five
# minutes off the diagonal and one in 40 of the rest.
points = con.execute("""
    SELECT epoch(event_t), epoch(proc_t), device FROM e
    WHERE event_t >= '2025-12-08' AND event_t < '2025-12-11'
      AND (abs(lag) > 300 OR hash(event_id) % 40 = 0)
    ORDER BY event_t, event_id""").fetchall()
Path(__file__).with_suffix(".json").write_text(json.dumps(
    {"start": "2025-12-08", "points": points}))
print(f"\nSaved {len(points):,} points from 8-10 December for a figure")
