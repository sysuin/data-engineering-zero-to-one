# Exercise 2.1: Relay's accounts read by csv and by DuckDB, compared.
import csv
from collections import Counter

import duckdb

from groundwork.config import LANDING

path = LANDING / "relay" / "2026-01-04" / "accounts.csv"
with open(path, newline="", encoding="utf-8-sig") as f:
    rows = list(csv.DictReader(f))

con = duckdb.connect()
table = f"read_csv('{path}')"
(n,) = con.execute(f"SELECT count(*) FROM {table}").fetchone()
ids = {i for (i,) in con.execute(f"SELECT account_id FROM {table}")
       .fetchall()}
segments = dict(con.execute(
    f"SELECT segment, count(*) FROM {table} GROUP BY segment")
    .fetchall())

print(f"rows           csv {len(rows):>6,}   duckdb {n:>6,}")
print("same ids:     ", {int(r["account_id"]) for r in rows} == ids)
by_segment = Counter(r["segment"] for r in rows)
print("same segments:", by_segment == segments)
kinds = {name: kind for name, kind, *_ in
         con.execute(f"DESCRIBE SELECT * FROM {table}").fetchall()}
text = type(rows[0]["customer_since"]).__name__
print(f"customer_since: csv {text}, "
      f"duckdb {kinds['customer_since']}")
