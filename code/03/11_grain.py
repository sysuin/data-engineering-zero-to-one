# The grain of every source, proved: rows against distinct keys.
import os

import httpx

from groundwork.config import (LANDING, NOW, REFERENCE, SANDBOX,
                               TRADELINE_DB)
from groundwork.profile import connect
from meridian.sources import Clock, frontline

con = connect()
con.execute(f"ATTACH '{TRADELINE_DB}' AS tradeline "
            "(TYPE sqlite, READ_ONLY)")
relay = LANDING / "relay" / "2026-01-04"
sup = LANDING / "suppliers"


def csv(path, **kw) -> str:
    opts = "".join(f", {k} = {v!r}" for k, v in kw.items())
    return f"read_csv('{path}', all_varchar = true{opts})"


# (source, its key, where to read it)
GRAINS = [
    ("tradeline.orders", "id", "tradeline.orders"),
    ("tradeline.order_lines", "order_id, line_no",
     "tradeline.order_lines"),
    ("relay.accounts", "account_id",
     csv(relay / "accounts.csv")),
    ("relay.contracts", "contract_id",
     csv(relay / "contracts.csv")),
    ("frontline.tickets", "id",
     f"read_json('{SANDBOX / 'ch03' / 'tickets.jsonl'}')"),
    ("suppliers, 13 alike", "filename, sku",
     csv(sup / "*/*_????-??.csv", filename=True)),
    ("suppliers, kestrel", "filename, sku",
     csv(sup / "kestrel-supply/*.csv", filename=True)),
    ("suppliers, pemberton", 'filename, "Item Code"',
     csv(sup / "pemberton-mills/*.csv", filename=True,
         union_by_name=True)),
    ("catalogue", "SKU",
     csv(LANDING / "catalogue" / "catalogue.csv")),
    ("ledger", "period, region, description",
     csv(LANDING / "ledger" / "*.csv", quote='"')),
    ("reference.regions", "region_id",
     csv(REFERENCE / "regions.csv")),
    ("reference.suppliers", "supplier_id",
     csv(REFERENCE / "suppliers.csv")),
]
print(f"{'source':<21}{'rows':>10}{'distinct':>10}  key")
for name, key, source in GRAINS:
    rows, distinct = con.execute(f"""
        SELECT COUNT(*), COUNT(DISTINCT ({key})) FROM {source}
    """).fetchone()
    mark = "" if rows == distinct else "   NOT UNIQUE"
    print(f"{name:<21}{rows:>10,}{distinct:>10,}  "
          f"{key.replace(', ', ',')}{mark}")

# The API's own count, asked a second way: numbered pages carry a total.
base = frontline.serve(Clock(NOW))
auth = {"Authorization": f"Bearer {os.environ['FRONTLINE_TOKEN']}"}
meta = httpx.get(f"{base}/api/v2/tickets", headers=auth,
                 params={"page": 1, "per_page": 1}).json()["meta"]
print(f"\nFrontline says it holds {meta['total']:,} tickets")
