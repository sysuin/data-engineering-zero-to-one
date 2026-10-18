# The whole staging layer built over a copy of the warehouse, and every
# model's rows reconciled with raw and with a second source.
import json
from datetime import datetime

import duckdb

from groundwork.transform import runner
from meridian.sources import tradeline

from _pack import sandbox, tidy

warehouse, folder = sandbox("staging", pack=False)
results = runner.run(warehouse, folder=folder,
                     run_at=datetime(2026, 1, 5, 6, 30))
con = duckdb.connect(str(warehouse))
tl = tradeline.connect()
(orders,), (lines,) = (
    tl.execute(f"SELECT count(*) FROM {t}").fetchone()
    for t in ("orders", "order_lines"))
desk, extra = con.execute("""SELECT source_count, extra
    FROM audit.load_log WHERE source = 'frontline'
    AND status = 'succeeded'
    ORDER BY load_id DESC LIMIT 1""").fetchone()
in_force = {(s, t): n for s, t, n in con.execute("""
    SELECT source, table_name, sum(rows) FROM audit.current_files
    GROUP BY ALL""").fetchall()}
second = {                           # file sources: the file ledger's
    ("tradeline", "orders"): orders,             # Tradeline itself
    ("tradeline", "order_lines"): lines,
    ("frontline", "tickets"): desk,              # the desk's total
    ("frontline", "ticket_fields"): 2 * desk,    # two per ticket
    ("frontline", "ticket_tags"): json.loads(extra)["ticket_tags"],
} | in_force

print(f"{'staging model':<25}{'raw':>11}{'staged':>11}{'second':>11}")
agree = 0
for r in results:
    if r.materialised == "seed":
        continue
    short = r.model.removeprefix("stg_")
    source, table = short.split("__")
    (n_raw,) = con.execute(
        f"SELECT count(*) FROM raw.{source}_{table}").fetchone()
    n_second = second[source, table]
    mark = "=" if n_second == r.rows else "!"
    agree += mark == "="
    print(f"{short:<25}{n_raw:>11,}{r.rows:>11,}{n_second:>11,} {mark}")
print(f"{agree} of {len(results) - 1} models agree with their second "
      "source")
types = con.execute("""
    SELECT split_part(data_type, '(', 1), count(*)
    FROM information_schema.columns WHERE table_schema = 'staging'
    GROUP BY 1 ORDER BY 2 DESC, 1""").fetchall()
print("staging's columns by type:")
for i in range(0, len(types), 4):
    print("  " + ", ".join(f"{t} {n}" for t, n in types[i:i + 4]))
con.close()
tidy("staging")
