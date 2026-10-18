# timeout: 600
# Evolving the price lists' contract for Pemberton's new header, what
# else a change could break and for whom, and repairing the history
# Chapter 9 loaded with empty costs.
import shutil
import sys
from dataclasses import replace
from pathlib import Path

import duckdb

from groundwork import contracts
from groundwork.config import LANDING, NOW, SANDBOX
from groundwork.contracts import check
from groundwork.ingest import files, price_lists, suppliers
from groundwork.ingest.__main__ import land
from groundwork.storage import raw_views

sys.path.insert(0, str(Path(__file__).parent))
from _variants import first_contract  # noqa: E402

v1, v2 = first_contract(), contracts.load("suppliers.price_lists")
print(f"version {v1.version} to {v2.version}:")
for who, what in contracts.compatible(v1, v2):
    print(f"  breaks {who:<10}{what}")

cols = dict(v2.columns)
cols["pack_size"] = contracts.Column("pack_size", nullable=True)
cols["currency"] = replace(cols["currency"], allowed=("USD", "CAD"))
cols["description"] = replace(cols["description"], nullable=False)
print("three other changes someone might ask for:")
for who, what in contracts.compatible(v2, replace(v2, columns=cols)):
    print(f"  breaks {who:<10}{what}")

july = "pemberton-mills/PEMBERTON-PRICES-202407.csv"
a = files.Arrival(LANDING / "suppliers" / july, f"suppliers/{july}",
                  0, None)
print("\nPemberton's July 2024 list under version 2:")
for name, read in (("Chapter 9's reader", suppliers.read),
                   ("price_lists.read", price_lists.read)):
    try:
        p = check.Guard(read, v2)(a.path.read_bytes(), a)
        print(f"  {name:<19} passed, {len(p.rows)} rows, cost "
              f"{p.rows[0]['cost']}")
    except check.ContractBroken as e:
        print(f"  {name:<19} broken\n    {str(e).split(': ', 1)[1]}")

# Chapter 9 loaded Pemberton's 48 lists on 4 Jan; mend them.
here = SANDBOX / "ch10" / "evolve"
shutil.rmtree(here, ignore_errors=True)
kw = dict(root=here / "raw", archive=here / "landed")
con = duckdb.connect(str(here.parent / "evolve.duckdb"))
day = NOW.date().replace(day=4)
pem = files.discover(LANDING, "suppliers/pemberton-mills", by=NOW)
files.load(con, "suppliers", "price_lists", pem, day, run_at=NOW,
           read=suppliers.read, folder="suppliers", **kw)
raw_views(con, kw["root"])


def audit(label):
    found = check.audit(con, "suppliers.price_lists",
                        archive=kw["archive"])
    broken = [ref for ref, _, r in found if r.breaking]
    (empty,) = con.execute("SELECT count(*) FROM "
                           "raw.suppliers_price_lists WHERE cost IS "
                           "NULL").fetchone()
    print(f"  {label:<9}{len(found)} lists, {len(broken)} broken, "
          f"{empty} empty costs in raw")
    return broken


print("\nrepairing what Chapter 9 loaded:")
broken = audit("audit")
n = check.reopen(con, "suppliers", "price_lists", broken,
                 "reopened: Pemberton's header from July 2024")
print(f"  reopened {n} lists")
x = land(con, "suppliers", "price_lists", pem, day, NOW,
         price_lists.read, "suppliers", **kw)
raw_views(con, kw["root"])
print(f"  reloaded {x.load_id}: {x.loaded} rows, {x.status}")
audit("audit")
con.close()
shutil.rmtree(here)
(here.parent / "evolve.duckdb").unlink()
