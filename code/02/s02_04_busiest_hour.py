# Exercise 2.4: the busiest hourly portal file of Q3 2025, found twice.
import gzip

import duckdb

from groundwork.config import LANDING

PORTAL = LANDING / "portal"


def counts():
    """(path, number of lines) for each file, one file at a time."""
    for month in ["2025/07", "2025/08", "2025/09"]:
        for path in sorted((PORTAL / month).glob("*/*.jsonl.gz")):
            with gzip.open(path, "rt", encoding="utf-8") as f:
                yield path, sum(1 for _ in f)


path, n = max(counts(), key=lambda pc: (pc[1], str(pc[0])))
print(f"Python: {path.relative_to(PORTAL)}  {n} events")

glob = PORTAL / "2025" / "0[789]" / "*" / "*.jsonl.gz"
name, m = duckdb.sql(f"""
    SELECT filename, count(*) AS n
    FROM read_json('{glob}', filename = true)
    GROUP BY filename
    ORDER BY n DESC, filename DESC LIMIT 1""").fetchone()
print(f"DuckDB: {name.split('portal/')[-1]}  {m} events")
