# What each of Meridian's systems holds this morning, asked of each.
import csv
import os

import httpx

from groundwork.config import LANDING, NOW
from meridian.sources import Clock, frontline, landing, tradeline


def csv_rows(path, **fmt):
    with open(path, newline="", encoding="utf-8") as f:
        return sum(1 for _ in csv.reader(f, **fmt)) - 1   # less header


held = []

# Tradeline: the orders application's own database, read-only.
con = tradeline.connect()
(orders,) = con.execute("SELECT COUNT(*) FROM orders").fetchone()
(lines,) = con.execute("SELECT COUNT(*) FROM order_lines").fetchone()
held.append(("Tradeline", "a database",
             f"{orders:,} orders, {lines:,} lines"))

# Relay: the latest nightly export, two CSV files in a dated folder.
night = sorted((LANDING / "relay").glob("20*"))[-1]
held.append(("Relay", "files, every night",
             f"{csv_rows(night / 'accounts.csv'):,} accounts, "
             f"{csv_rows(night / 'contracts.csv'):,} contracts"))

# Frontline: an API. Ask for one ticket and read the total it reports.
base = frontline.serve(Clock(NOW))
page = httpx.get(f"{base}/api/v2/tickets",
                 params={"page": 1, "per_page": 1},
                 headers={"Authorization":
                          f"Bearer {os.environ['FRONTLINE_TOKEN']}"})
held.append(("Frontline", "an API",
             f"{page.json()['meta']['total']:,} tickets"))

# The files other teams drop in the landing zone.
lists = landing.arrived(by=NOW, under="suppliers")
held.append(("Suppliers", "files, by email",
             f"{len(lists)} price lists from "
             f"{len({p.parent.name for p in lists})} suppliers"))
held.append(("Catalogue", "one file, overwritten",
             f"{csv_rows(LANDING / 'catalogue' / 'catalogue.csv'):,}"
             " products"))
hours = landing.arrived(by=NOW, under="portal")
held.append(("Portal", "files, one an hour",
             f"{len(hours):,} hourly files"))
months = sorted(p.stem[-7:] for p in (LANDING / "ledger").glob("*.csv"))
held.append(("Ledger", "files, one a month",
             f"{len(months)} months, {months[0]} to {months[-1]}"))

print(f"{'system':<10} {'arrives as':<23} holds")
for system, how, what in held:
    print(f"{system:<10} {how:<23} {what}")
