# Exercise 19.1: the portal's second copies, found with one query.
import duckdb

from groundwork.config import LANDING

con = duckdb.connect()
twins, straddle, shortest, longest = con.execute(f"""
    WITH e AS (
      SELECT event_id, filename AS file,
             strptime(received_at, '%Y-%m-%dT%H:%M:%S.%gZ') AS t
      FROM read_json('{LANDING}/portal/*/*/*/*.jsonl.gz', filename = 1,
                     columns = {{event_id: 'VARCHAR',
                                received_at: 'VARCHAR'}})),
    pairs AS (
      SELECT event_id, count(*) - 1 AS extra,
             count(DISTINCT file) > 1 AS two_files,
             epoch(max(t)) - epoch(min(t)) AS gap
      FROM e GROUP BY event_id HAVING count(*) > 1)
    SELECT sum(extra), count(*) FILTER (WHERE two_files),
           min(gap), max(gap) FROM pairs""").fetchone()
print(f"second copies: {twins:,}; in a different file: {straddle}")
print(f"seconds between copies: {shortest:.1f} to {longest:.1f}")
