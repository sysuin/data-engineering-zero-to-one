# features.renewal_snapshot: one row per contract as it stood on the
# morning of its 90-day mark, checked, and reconciled with Book 2.
# timeout: 300
import json
import textwrap
from pathlib import Path

import duckdb

from groundwork.config import BOOK2_WAREHOUSE, TODAY
from groundwork.serve import features

from _serve import tidy, warehouse

con = duckdb.connect(str(warehouse()))
n = features.build(con)
first, last, labelled, open_ = con.execute("""
    SELECT min(as_of), max(as_of), count(not_renewed),
           count(*) - count(not_renewed)
    FROM features.renewal_snapshot""").fetchone()
print(f"rows {n:,}: marks from {first} to {last}")
print(f"  labelled (ended before {TODAY}) {labelled:,}; "
      f"not yet ended {open_:,}")
print(f"check: {features.check(con) or 'every promise kept'}")

# Book 2's builder, as SQL over Book 2's warehouse: same contracts,
# same windows, its own way of resolving the old CRM's ids.
con.execute(f"ATTACH '{BOOK2_WAREHOUSE}' AS b2 (TYPE sqlite, "
            "READ_ONLY)")
con.execute((Path(__file__).parent / "book2_features.sql").read_text())
total, *differ = con.execute("""
    SELECT count(*),
      count(*) FILTER (b.account_id <> g.account_id),
      count(*) FILTER (b.since IS DISTINCT FROM g.days_since_order),
      count(*) FILTER (b.n90 <> g.orders_90d
                       OR b.p90 <> g.orders_prev_90d),
      count(*) FILTER (abs(b.spend - g.spend_365) >= 0.005),
      count(*) FILTER (b.t90 <> g.tickets_90d),
      count(*) FILTER (b.tenure IS DISTINCT FROM g.tenure_days),
      count(*) FILTER (b.segment IS DISTINCT FROM g.segment),
      count(*) FILTER (b.not_renewed <> g.not_renewed)
    FROM book2 AS b JOIN features.renewal_snapshot AS g
    USING (contract_id)""").fetchone()
(ours,) = con.execute("""
    SELECT count(*) FROM features.renewal_snapshot
    WHERE end_date <= DATE '2025-12-31'""").fetchone()
print(f"Book 2's rows {total:,}, ours ending by 2025 {ours:,}; "
      "rows that\ndiffer in")
names = ("account", "days since order", "order counts", "spend",
         "tickets", "tenure", "segment", "label")
said = ", ".join(f"{k} {v}" for k, v in zip(names, differ))
print(textwrap.fill(said, 68, initial_indent="  ",
                    subsequent_indent="  "))
(gap,) = con.execute("""SELECT count(*) FROM features.renewal_snapshot
                        WHERE segment IS NULL""").fetchone()
print(f"rows with no account version the day before the mark: {gap}")

row = con.execute("""SELECT * FROM features.renewal_snapshot
                     WHERE contract_id = 7332""").fetchone()
cols = [c[0] for c in con.description]
print("contract 7332:")
for k, v in zip(cols, row):
    if k not in ("contract_id", "legacy_terms", "discount_pct"):
        print(f"  {k:<18}{v}")
orders = con.execute("""
    SELECT o.order_date FROM marts.fct_orders AS o
    JOIN marts.dim_account AS a USING (account_key)
    WHERE a.account_id = ? AND o.order_date BETWEEN ? AND ?
    ORDER BY 1""", [row[1], row[2].replace(year=row[2].year - 1),
                    row[3]]).fetchall()
Path(__file__).with_suffix(".json").write_text(json.dumps(
    {"row": dict(zip(cols, map(str, row))),
     "orders": [str(d) for (d,) in orders]}))
con.close()
tidy("warehouse.duckdb")
