#!/usr/bin/env python3
"""
Prove Meridian's sources are what the book says they are.

Two kinds of check:

1.  **The sources rebuild Book 2's warehouse.** A reference pipeline, written plainly and
    kept here as the answer key, reads every source the way the book teaches and rebuilds
    each of Book 2's tables. Every row and value must match, except where the book says
    it cannot (history from before the first export, and two versions that lasted no time).
    It also rebuilds Book 1's quarterly revenue from the key accounts, and ties each
    ledger month to the orders.

2.  **Every planted problem is present**, at the size the manifest records, so no chapter's
    lesson can quietly fail to happen.

Standard library only.

    python3 verify_sources.py [--sources ../../data/meridian-sources] [--ml ../../data/meridian-ml]
"""
from __future__ import annotations

import argparse
import csv
import glob
import gzip
import json
import os
import re
import sqlite3
import sys
import zipfile
from collections import Counter, defaultdict
from datetime import date, datetime, timedelta, timezone
from xml.etree import ElementTree
from zoneinfo import ZoneInfo

UTC = timezone.utc
TZ = {1: "America/New_York", 2: "America/New_York", 3: "America/New_York",
      4: "America/Los_Angeles", 5: "America/Chicago"}
REGION_ID = {"Northeast": 1, "Southeast": 2, "Midwest": 3, "West": 4, "Southwest": 5}
REGION_CODE = {"NE": 1, "SE": 2, "MW": 3, "WE": 4, "SW": 5}

failures = 0


def check(ok: bool, what: str, detail: str = "") -> None:
    global failures
    print(f"  {'ok  ' if ok else 'FAIL'}  {what}" + (f"  ({detail})" if detail else ""))
    failures += 0 if ok else 1


def utc(s: str) -> datetime:
    return datetime.fromisoformat(s.replace("T", " ").rstrip("Z")).replace(tzinfo=UTC)


def local_date(moment: datetime, region: int) -> date:
    return moment.astimezone(ZoneInfo(TZ[region])).date()


def read_csv(path: str, delimiter: str = ",", encoding: str = "utf-8") -> list[dict]:
    with open(path, newline="", encoding=encoding) as f:
        return list(csv.DictReader(f, delimiter=delimiter))


def read_xlsx(path: str) -> list[list]:
    ns = {"m": "http://schemas.openxmlformats.org/spreadsheetml/2006/main"}
    with zipfile.ZipFile(path) as z:
        root = ElementTree.fromstring(z.read("xl/worksheets/sheet1.xml"))
    rows = []
    for r in root.iter(f"{{{ns['m']}}}row"):
        row = []
        for c in r:
            t = c.find("m:is/m:t", ns)
            v = c.find("m:v", ns)
            row.append(t.text if t is not None else float(v.text))
        rows.append(row)
    return rows


# --------------------------------------------------------------------------- the reference pipeline

def load_accounts(src: str, relay_store: str) -> tuple[dict, dict, dict]:
    """
    Accounts as the CRM last exported them, plus the legacy records from the last export
    before the migration. Returns (accounts, region_of, legacy_ids).
    """
    sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))
    from meridian.sources import relay
    tmp = os.path.join(src, "..", "sandbox", "verify-relay")
    last = relay.export("2026-01-04", tmp, store=relay_store)
    legacy = relay.export("2024-02-29", tmp, store=relay_store)
    accounts, region_of = {}, {}
    for r in read_csv(last[0], encoding="utf-8-sig"):
        a = int(r["account_id"])
        accounts[a] = (a, r["account_name"], REGION_ID[r["region"]], r["segment"], r["owner"],
                       r["customer_since"], r["postcode"], int(r["key_account"] == "true"),
                       "Meridian CRM", r["closed_on"] or None)
        region_of[a] = REGION_ID[r["region"]]
    legacy_ids = {}
    for r in read_csv(legacy[0], delimiter=";", encoding="cp1252"):
        a = int(r["Acct ID"])
        if a in accounts:
            continue
        m, d, y = r["Customer Since"].split("/")
        accounts[a] = (a, r["Account Name"], REGION_ID[r["Region"]], r["Segment"], r["Owner"],
                       f"{y}-{m}-{d}", r["Zip"].zfill(5), 0, "Legacy CRM", None)
        region_of[a] = REGION_ID[r["Region"]]
        legacy_ids[a] = r
    return accounts, region_of, legacy_ids


def load_costs(src: str) -> dict[str, list[tuple[date, float]]]:
    """Every price list, in each layout, the latest arrival winning for each month."""
    by_key: dict[tuple, tuple] = {}
    for path in sorted(glob.glob(os.path.join(src, "landing", "suppliers", "*", "*"))):
        name = os.path.basename(path)
        arrived = os.path.getmtime(path)
        items = []
        if name.startswith("PEMBERTON"):
            for r in read_csv(path):
                cost = r.get("Unit Cost USD") or r.get("Cost (USD)")
                d, m, y = r["Valid From"].split("/")
                items.append((r["Item Code"], date(int(y), int(m), int(d)), float(cost)))
        elif name.startswith("kestrel"):
            for r in read_csv(path):
                items.append((r["sku"], date.fromisoformat(r["effective"]), int(r["cost_cents"]) / 100))
        elif name.endswith(".xlsx"):
            rows = read_xlsx(path)
            banner = re.search(r"from (\d+) (\w+) (\d{4})", rows[1][0])
            eff = datetime.strptime(" ".join(banner.groups()), "%d %B %Y").date()
            for sku, _, price in rows[4:]:
                items.append((sku, eff, float(price)))
        else:
            for r in read_csv(path):
                items.append((r["sku"], date.fromisoformat(r["effective_from"]), float(r["unit_cost"])))
        for sku, eff, cost in items:
            key = (sku, eff)
            if key not in by_key or by_key[key][0] < arrived:
                by_key[key] = (arrived, cost)
    costs: dict[str, list] = defaultdict(list)
    for (sku, eff), (_, cost) in sorted(by_key.items()):
        costs[sku].append((eff, cost))
    return costs


def cost_on(costs: dict, sku: str, d: date) -> float:
    best = None
    for eff, c in costs[sku]:
        if eff <= d:
            best = c
    return best


def rebuild(src: str) -> dict:
    """Book 2's tables, from the sources."""
    tl = sqlite3.connect(f"file:{os.path.join(src, 'tradeline', 'tradeline.db')}?mode=ro", uri=True)
    relay_store = os.path.join(src, "relay", "_store.db")
    accounts, region_of, legacy = load_accounts(src, relay_store)
    costs = load_costs(src)
    channel = dict(tl.execute("SELECT code, label FROM channels"))
    orders, ids = [], {}
    for oid, num, acct, placed, ch in tl.execute(
            "SELECT id, order_number, account_id, placed_at, channel FROM orders "
            "WHERE status = 'placed'"):
        d = local_date(utc(placed), region_of[acct])
        orders.append((num, acct, d.isoformat(), channel[ch]))
        ids[oid] = (num, d)
    lines = []
    for oid, line_no, sku, qty, pc, disc in tl.execute(
            "SELECT order_id, line_no, sku, qty, unit_price_cents, discount_pct FROM order_lines"):
        if oid in ids:
            num, d = ids[oid]
            lines.append((num, line_no, sku, qty, pc / 100, cost_on(costs, sku, d), disc))
    tl.close()

    # Contracts: the last export.
    contracts = []
    last = os.path.join(src, "..", "sandbox", "verify-relay", "2026-01-04", "contracts.csv")
    for r in read_csv(last, encoding="utf-8-sig"):
        contracts.append((int(r["contract_id"]), int(r["account_id"]), r["start_date"],
                          r["end_date"], int(r["term_months"]), int(r["legacy_terms"] == "true"),
                          int(r["discount_pct"]) if r["discount_pct"] else None,
                          r["outcome"] or None, r["notice_date"] or None,
                          r["cancellation_reason"] or None))

    # Account history: today's state, walked back through the audit to the day each
    # account first appeared (its creation, or the first export anyone kept).
    audit = []
    for path in sorted(glob.glob(os.path.join(src, "landing", "relay", "audit", "*.csv"))):
        audit += [r for r in read_csv(path) if r["object"] == "account"]
    created = {int(r["record_id"]): utc(r["changed_at"]) for r in audit if r["field"] == "created"}
    changes = defaultdict(list)
    for r in audit:
        if r["field"] in ("segment", "owner"):
            changes[int(r["record_id"])].append((utc(r["changed_at"]), r["field"],
                                                  r["old_value"], r["new_value"]))
    history = []
    for a, row in accounts.items():
        if row[8] == "Legacy CRM":
            continue
        region = row[2]
        state = {"segment": row[3], "owner": row[4]}
        steps = []
        for moment, field, old, new in sorted(changes[a], reverse=True):
            steps.append((local_date(moment, region), dict(state)))
            state[field] = old
        first = local_date(created[a], region) if a in created else date(2022, 1, 1)
        versions = {first: dict(state)}
        for d, st in reversed(steps):
            versions[d] = st          # two changes in a day: the day ends in the later one
        days = sorted(versions)
        for i, d in enumerate(days):
            vt = (days[i + 1] - timedelta(days=1)).isoformat() if i + 1 < len(days) else None
            history.append((a, d.isoformat(), vt, versions[d]["segment"], versions[d]["owner"]))

    # Tickets: the desk's records as they stand today.
    fl = sqlite3.connect(f"file:{os.path.join(src, 'frontline', '_store.db')}?mode=ro", uri=True)
    tickets = []
    chan = {"email": "Email", "web": "Web form", "voice": "Phone"}
    for r in fl.execute(
            "WITH cur AS (SELECT v.*, ROW_NUMBER() OVER (PARTITION BY ticket_id ORDER BY "
            "updated_at DESC) rn FROM ticket_versions v) SELECT t.ticket_id, "
            "t.requester_account_id, t.created_at, t.channel, t.sku, t.language, t.description, "
            "c.desk, c.category, c.priority FROM tickets t JOIN cur c USING (ticket_id) "
            "WHERE rn = 1"):
        tid, acct, created_at, ch, sku, lang, body, desk, cat, pri = r
        opened = utc(created_at).astimezone(ZoneInfo(TZ[region_of[acct]]))
        tickets.append((tid, acct, opened.strftime("%Y-%m-%d %H:%M:%S"), chan[ch], sku, lang, body,
                        desk.replace("_", " ").capitalize(), cat.capitalize(), pri.capitalize()))
    fl.close()

    # Products: the catalogue, cleaned, with procurement's supplier ids and the first cost.
    suppliers = {r["name"]: int(r["supplier_id"])
                 for r in read_csv(os.path.join(src, "reference", "suppliers.csv"))}
    alias = {"Pemberton": "Pemberton Mills", "Aldridge and Co": "Aldridge & Co",
             "Kestrel": "Kestrel Supply"}
    products = []
    for r in read_csv(os.path.join(src, "landing", "catalogue", "catalogue.csv")):
        sku = r["SKU"].strip().upper()
        sname = r["Supplier"].strip()
        launched = (datetime.strptime(r["Launched"], "%d-%b-%Y").date().isoformat()
                    if r["Launched"] else None)
        products.append((sku, r["Product Name"], r["Category"], suppliers[alias.get(sname, sname)],
                         costs[sku][0][1], float(r["List Price"].replace("$", "").replace(",", "")),
                         launched))
    return {"orders": orders, "lines": lines, "accounts": list(accounts.values()),
            "history": history, "contracts": contracts, "tickets": tickets,
            "products": products, "region_of": region_of}


def compare(name: str, ours: list, theirs: list, key=lambda r: r) -> tuple[set, set]:
    a, b = set(map(tuple, ours)), set(map(tuple, theirs))
    only_ours, only_theirs = a - b, b - a
    check(not only_ours and not only_theirs, f"{name}: {len(b):,} rows rebuilt exactly",
          "" if not (only_ours or only_theirs) else
          f"{len(only_ours)} only in the rebuild, {len(only_theirs)} only in Book 2")
    return only_ours, only_theirs


def rounded(rows: list, cols: tuple) -> list:
    return [tuple(round(v, 2) if i in cols and v is not None else v for i, v in enumerate(r))
            for r in rows]


# --------------------------------------------------------------------------- main

def main() -> int:
    here = os.path.dirname(os.path.abspath(__file__))
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--sources", default=os.path.join(here, "..", "..", "data", "meridian-sources"))
    ap.add_argument("--ml", default=os.path.join(here, "..", "..", "data", "meridian-ml"))
    ap.add_argument("--book1", default=os.path.join(here, "..", "..", "data", "meridian"))
    args = ap.parse_args()
    src = os.path.abspath(args.sources)
    manifest = json.load(open(os.path.join(src, "manifest.json")))
    b2 = sqlite3.connect(f"file:{os.path.join(args.ml, 'warehouse', 'meridian_ml.db')}?mode=ro",
                         uri=True)
    q = lambda sql: b2.execute(sql).fetchall()                          # noqa: E731

    print("The sources rebuild Book 2's warehouse")
    r = rebuild(src)
    compare("orders", r["orders"], q("SELECT order_id, account_id, order_date, channel FROM orders"))
    compare("order_lines", rounded(r["lines"], (4, 5)),
            rounded(q("SELECT order_id, line_no, sku, qty, unit_price, unit_cost, discount_pct "
                      "FROM order_lines"), (4, 5)))
    compare("accounts", r["accounts"], q("SELECT * FROM accounts"))
    compare("contracts", r["contracts"], q("SELECT * FROM contracts"))
    compare("tickets", r["tickets"], q("SELECT * FROM tickets"))
    compare("products", rounded(r["products"], (4, 5)), rounded(q("SELECT * FROM products"), (4, 5)))

    ours = set(r["history"])
    theirs = q("SELECT * FROM account_history ORDER BY account_id, valid_from, rowid")
    expected, notes = book2_history_as_a_crm_saw_it(theirs, q("SELECT account_id, segment, "
                                                              "account_manager FROM accounts"))
    check(ours == expected, f"account_history: {len(theirs):,} rows; every difference explained",
          ", ".join(f"{n} {what}" for what, n in notes.items()))
    b2_nonlegacy = q("SELECT account_id FROM accounts WHERE crm_source <> 'Legacy CRM'")
    check(len({x[0] for x in ours}) == len(b2_nonlegacy),
          "every account has a history", f"{len(b2_nonlegacy):,} accounts")

    print("The ledger ties to the orders")
    by = Counter()
    for (num, line_no, sku, qty, price, cost, disc) in r["lines"]:
        pass
    order_region = {o[0]: (r["region_of"][o[1]], o[2][:7]) for o in r["orders"]}
    for (num, line_no, sku, qty, price, cost, disc) in r["lines"]:
        reg, period = order_region[num]
        by[(period, reg)] += round(qty * price * 100)
    gl = Counter()
    journals = Counter()
    for path in sorted(glob.glob(os.path.join(src, "landing", "ledger", "*.csv"))):
        for row in read_csv(path):
            k = (row["period"], REGION_CODE[row["region"]])
            amount = round(float(row["amount"]) * 100)
            (gl if row["description"] == "Sales journal" else journals)[k] += amount
    months = sorted({k[0] for k in gl})
    check(all(gl[k] == by[k] for k in gl), f"every region's sales journal equals its orders, "
          f"{months[0]} to {months[-1]}", f"{len(gl)} region-months, to the cent")
    check(len(journals) == len(manifest["ledger_journals"]),
          "finance's own journals are the only other lines", f"{len(journals)}")

    print("Book 1's quarters, from the key accounts")
    book1 = json.load(open(os.path.join(args.book1, "manifest.json")))["quarterly_totals"]
    key = {a[0] for a in r["accounts"] if a[7]}
    rev, cost, orders = Counter(), Counter(), defaultdict(set)
    odate = {o[0]: o[2] for o in r["orders"] if o[1] in key}
    for num, line_no, sku, qty, price, c, disc in r["lines"]:
        if num in odate:
            d = date.fromisoformat(odate[num])
            qk = f"{d.year} Q{(d.month - 1) // 3 + 1}"
            rev[qk] += round(qty * price * 100)
            cost[qk] += round(qty * c * 100)
            orders[qk].add(num)
    ok = all(round(rev[k] / 100, 2) == v["revenue"] and round(cost[k] / 100, 2) == v["cost"]
             and len(orders[k]) == v["orders"] for k, v in book1.items())
    check(ok, "revenue, cost and orders for every quarter Book 1 printed", f"{len(book1)} quarters")

    print("The planted problems")
    tl = sqlite3.connect(f"file:{os.path.join(src, 'tradeline', 'tradeline.db')}?mode=ro", uri=True)
    hist = sqlite3.connect(f"file:{os.path.join(src, 'tradeline', '_history.db')}?mode=ro", uri=True)
    (n_cancel,) = tl.execute("SELECT COUNT(*) FROM orders WHERE status = 'cancelled'").fetchone()
    check(n_cancel == manifest["counts"]["cancelled_orders"], "cancelled orders stay in Tradeline",
          f"{n_cancel:,}")
    (n_amended,) = tl.execute("SELECT COUNT(*) FROM orders WHERE updated_at <> created_at "
                              "AND status = 'placed'").fetchone()
    check(n_amended == manifest["counts"]["amended_orders"],
          "amended orders keep created_at and move updated_at", f"{n_amended:,}")
    (n_deleted,) = hist.execute("SELECT COUNT(DISTINCT id) FROM orders_v WHERE op = 'D'").fetchone()
    check(n_deleted == manifest["counts"]["test_orders_deleted"],
          "test orders were inserted and hard-deleted", f"{n_deleted}")
    racing = hist.execute("SELECT COUNT(*) FROM amend_job_runs WHERE committed_at > "
                          "substr(started_at, 1, 11) || '02:05:00'").fetchone()[0]
    (runs,) = hist.execute("SELECT COUNT(*) FROM amend_job_runs").fetchone()
    check(racing > runs // 2, "the amendment job commits after an 02:05 extract on most nights",
          f"{racing} of {runs} nights")
    shifted = sum(1 for o in r["orders"] if False)
    for (placed, acct) in tl.execute("SELECT placed_at, account_id FROM orders "
                                     "WHERE status='placed'"):
        if placed[:10] != local_date(utc(placed), r["region_of"][acct]).isoformat():
            shifted += 1
    check(shifted > 1000, "some orders' UTC date is not their local date", f"{shifted:,}")

    nights = sqlite3.connect(os.path.join(src, "relay", "_store.db")).execute(
        "SELECT COUNT(*) FROM nights WHERE exported = 0").fetchone()[0]
    check(nights == len(manifest["missing_nights"]), "nights with no CRM export", f"{nights}")
    legacy_file = glob.glob(os.path.join(src, "..", "sandbox", "verify-relay", "2024-02-29",
                                         "accounts.csv"))[0]
    raw = open(legacy_file, "rb").read()
    check(b";" in raw.split(b"\r\n")[0] and b"\xeb" in raw,
          "the old CRM wrote semicolons in Windows-1252")
    short_zips = sum(1 for row in read_csv(legacy_file, ";", "cp1252") if len(row["Zip"]) < 5)
    check(short_zips > 100, "the old CRM lost postcodes' leading zeros", f"{short_zips:,} rows")
    mp = read_csv(os.path.join(src, "landing", "relay", "migration_map.csv"))
    check(len(mp) == manifest["counts"]["migration_map_rows"] < manifest["counts"]["legacy_ids"],
          "IT's migration map misses some legacy ids",
          f"{len(mp)} of {manifest['counts']['legacy_ids']}")

    pem = sorted(glob.glob(os.path.join(src, "landing", "suppliers", "pemberton-mills", "*")))
    heads = {open(p).readline().strip() for p in pem}
    check(len(heads) == 2, "Pemberton renamed its cost column", f"{len(pem)} files")
    kest = read_csv(sorted(glob.glob(os.path.join(src, "landing", "suppliers", "kestrel-supply",
                                                  "*")))[0])
    check("cost_cents" in kest[0], "Kestrel sends cents")
    ald = glob.glob(os.path.join(src, "landing", "suppliers", "aldridge-co", "*corrected*"))
    check(len(ald) == 1, "Aldridge resent a corrected list", os.path.basename(ald[0]) if ald else "")
    brackley = glob.glob(os.path.join(src, "landing", "suppliers", "brackley-plastics", "*"))
    check(not any("2023-08" in b for b in brackley) and len(brackley) == 47,
          "Brackley Plastics' August 2023 list never arrived", f"{len(brackley)} of 48 months")
    voss = glob.glob(os.path.join(src, "landing", "suppliers", "voss-industrial", "*2025-01*"))[0]
    arrived = datetime.fromtimestamp(os.path.getmtime(voss), UTC)
    check(arrived.date() > date(2025, 1, 1), "Voss's January 2025 list arrived late",
          f"{arrived:%Y-%m-%d}")
    cat = read_csv(os.path.join(src, "landing", "catalogue", "catalogue.csv"))
    names = {row["Supplier"] for row in cat}
    check(len(names - set(read_csv_names(src))) == 4, "the catalogue spells four suppliers its own way")
    check(sum(1 for row in cat if row["SKU"] != row["SKU"].strip().upper()) == 3,
          "three catalogue SKUs are written differently")

    events, ids, skew, late, bots, versions = 0, Counter(), 0, 0, 0, Counter()
    for path in sorted(glob.glob(os.path.join(src, "landing", "portal", "*", "*", "*", "*.gz"))):
        with gzip.open(path, "rt", encoding="utf-8") as f:
            for line in f:
                e = json.loads(line)
                events += 1
                ids[e["event_id"]] += 1
                gap = (utc(e["received_at"]) - utc(e["occurred_at"])).total_seconds()
                skew += gap < -60
                late += gap > 3600
                bots += "Bot" in e["user_agent"]
                versions[e["schema_version"]] += 1
    dupes = sum(c - 1 for c in ids.values() if c > 1)
    check(events == manifest["counts"]["portal_events"], "portal events", f"{events:,}")
    check(dupes == manifest["counts"]["portal_duplicates"], "duplicated deliveries", f"{dupes:,}")
    check(late > 1000 and skew > 100, "late arrivals and clocks that run ahead",
          f"{late:,} over an hour late, {skew:,} from the future")
    check(bots == manifest["counts"]["portal_bot_events"], "the bot's events", f"{bots:,}")
    check(len(versions) == 2, "two event schemas", f"v1 {versions[1]:,}, v2 {versions[2]:,}")

    fl = sqlite3.connect(os.path.join(src, "frontline", "_store.db"))
    (pii,) = fl.execute("SELECT COUNT(*) FROM comments WHERE body LIKE '%555-01%' "
                        "OR body LIKE '%.example%'").fetchone()
    check(pii > 100, "ticket comments carry phone numbers and emails", f"{pii}")

    print()
    print("All checks passed." if not failures else f"{failures} check(s) FAILED.")
    return 1 if failures else 0


def book2_history_as_a_crm_saw_it(rows: list, current: list) -> tuple[set, dict]:
    """
    Book 2's history, with the four things no CRM could have recorded taken out:
    a start before the first export anyone kept (1 January 2022), a version that lasted
    no time, a "change" to the value already held, and a segment change Book 2's history
    dropped but its accounts table kept.
    """
    notes = Counter()
    by = defaultdict(list)
    for a, vf, vt, seg, mgr in rows:
        if vt is not None and vt < vf:
            notes["lasted no time"] += 1
            continue
        if vf < "2022-01-01":
            notes["began before the first export"] += 1
            vf = "2022-01-01"
        if by[a] and (by[a][-1][3], by[a][-1][4]) == (seg, mgr):
            notes["changed nothing"] += 1
            continue
        if by[a] and by[a][-1][1] == vf:       # clamped onto a change of the same day
            by[a][-1] = [a, vf, None, seg, mgr]
            continue
        by[a].append([a, vf, None, seg, mgr])
    now = {a: (seg, mgr) for a, seg, mgr in current}
    for a, vs in by.items():
        last = vs[-1]
        if (last[3], last[4]) != now[a]:
            notes["lost a segment change"] += 1
            vf = date.fromisoformat(last[1])
            m = date(vf.year + (vf.month == 12), vf.month % 12 + 1, 1)
            vs.append([a, m.isoformat(), None, *now[a]])
        for i in range(len(vs) - 1):
            vs[i][2] = (date.fromisoformat(vs[i + 1][1]) - timedelta(days=1)).isoformat()
    return {tuple(v) for vs in by.values() for v in vs}, dict(notes)


def read_csv_names(src: str) -> list[str]:
    return [r["name"] for r in read_csv(os.path.join(src, "reference", "suppliers.csv"))]


if __name__ == "__main__":
    sys.exit(main())
