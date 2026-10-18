# Exercise 3: an order under an old CRM id in the tiny Meridian. The id
# must have been exported on an earlier night for identity to know it.
# timeout: 900
import shutil
from datetime import date, datetime, time, timedelta

import duckdb
from _sandbox import HERE

from groundwork.testing import Sources, example


def fact(s: Sources) -> tuple:
    con = duckdb.connect(str(s.warehouse), read_only=True)
    row = con.execute("""
        SELECT f.order_id, a.account_id, i.matched_by
        FROM marts.fct_order_lines AS f
        JOIN marts.dim_account AS a USING (account_key)
        LEFT JOIN intermediate.int_account_identity AS i
            ON i.crm_id = 90010
        WHERE f.order_id = 6""").fetchone()
    con.close()
    return row


for earlier in (False, True):
    root = HERE / "old-id"
    shutil.rmtree(root, ignore_errors=True)
    s = example(root)
    s.old_id(90010, 10)                        # IT's map: 90010 is 10
    s.order(6, 90010, "2025-12-05 15:00:00")   # placed under the old id
    s.write()
    if earlier:            # the night before, the CRM still sent 90010
        night = date(2026, 1, 3)
        old = Sources(root, night=night)
        old.accounts = {**s.accounts,
                        90010: dict(s.accounts[10], account_id=90010)}
        old._csv(root / "landing" / "relay" / night.isoformat() /
                 "accounts.csv", list(s.accounts[10]),
                 old.accounts.values(), bom=True,
                 arrived=datetime.combine(night + timedelta(days=1),
                                          time(4)))
        header = list(next(iter(s.contracts.values())))
        old._csv(root / "landing" / "relay" / night.isoformat() /
                 "contracts.csv", header,
                 s.contracts.values(), bom=True,
                 arrived=datetime.combine(night + timedelta(days=1),
                                          time(4)))
        old.ingest(s.warehouse)
    s.ingest()
    s.transform()
    order, account, matched = fact(s)
    print(f"exported earlier: {earlier!s:<5}  order {order} -> "
          f"account {account}, identity says {matched}")
shutil.rmtree(HERE / "old-id")
