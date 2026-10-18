# Every source Meridian has, profiled the same way: sizes and notes.
import json
import os

import httpx

from groundwork.config import (LANDING, NOW, REFERENCE, SANDBOX,
                               TRADELINE_DB)
from groundwork.profile import connect, profile
from meridian.sources import Clock, frontline

con = connect()
con.execute(f"ATTACH '{TRADELINE_DB}' AS tradeline "
            "(TYPE sqlite, READ_ONLY)")

# Frontline has no file to read: page through the API into one, kept
# in the sandbox for the next listing.
work = SANDBOX / "ch03"
work.mkdir(parents=True, exist_ok=True)
tickets = work / "tickets.jsonl"
base = frontline.serve(Clock(NOW))
auth = {"Authorization": f"Bearer {os.environ['FRONTLINE_TOKEN']}"}
params = {"limit": 100}
with httpx.Client(base_url=base, headers=auth) as http, \
        open(tickets, "w", encoding="utf-8") as out:
    while True:
        page = http.get("/api/v2/tickets", params=params).json()
        for t in page["tickets"]:
            out.write(json.dumps(t, ensure_ascii=False) + "\n")
        if not page["meta"]["has_more"]:
            break
        params["cursor"] = page["meta"]["after_cursor"]

relay = LANDING / "relay" / "2026-01-04"
sup = LANDING / "suppliers"
sources = {
    "tradeline.orders": con.table("tradeline.orders"),
    "tradeline.order_lines": con.table("tradeline.order_lines"),
    "relay.accounts": con.read_csv(str(relay / "accounts.csv")),
    "relay.contracts": con.read_csv(str(relay / "contracts.csv")),
    "frontline.tickets": con.read_json(str(tickets)),
    "suppliers, 13 alike": con.read_csv(str(sup / "*/*_2025-12.csv")),
    "suppliers, kestrel": con.read_csv(
        str(sup / "kestrel-supply" / "kestrel_20251201.csv")),
    "suppliers, pemberton": con.read_csv(
        str(sup / "pemberton-mills" / "PEMBERTON-PRICES-202512.csv")),
    "catalogue": con.read_csv(str(LANDING / "catalogue" /
                                  "catalogue.csv")),
    "ledger": con.read_csv(str(LANDING / "ledger" / "gl_revenue_*.csv"),
                          quotechar='"'),
    "reference.regions": con.read_csv(str(REFERENCE / "regions.csv")),
    "reference.suppliers": con.read_csv(str(REFERENCE /
                                            "suppliers.csv")),
}
for name, rel in sources.items():
    p = profile(rel, name)
    print(f"{name:<24}{p.rows:>12,} rows{len(p.columns):>4} columns")
    for note in p.notes():
        print(f"  {note}")
