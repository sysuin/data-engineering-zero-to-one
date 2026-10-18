#!/usr/bin/env python3
"""
Generate Meridian for machine learning: the company from Book 1, scaled to about six
thousand accounts, with labels that come from a written-down process.

Book 1's warehouse has 58 customers. That is enough to answer questions about and far
too few to learn from, so this file adds a long tail of smaller accounts and everything a
prediction needs: contracts and their renewals, account history over time, support
tickets tied to accounts, and the price changes that move the ground under a model.

Four rules govern this file:

1.  **Book 1 is not touched.** This script reads Book 1's warehouse (generating it first
    if it is missing) and writes to a different directory. The 58 key accounts keep their
    orders exactly as Book 1 generated them.

2.  **Deterministic.** Standard library only, seeded throughout, so two readers get
    byte-identical files and the book can print exact numbers.

3.  **The label comes from a known process.** Ninety days before each contract ends, the
    account makes up its mind. The chance that it leaves is `renewal_logit()` below: a
    formula over things the warehouse records, plus logistic noise. Because the formula is
    known, the best score any model could reach is known too, and verify_ml.py prints it.

4.  **The traps are deliberate.** Each is a mistake real projects make, and each is listed
    in the manifest with the chapter that catches it. Do not "fix" one here.

    python3 generate_ml.py [--book1 ../../data/meridian] [--out ../../data/meridian-ml]
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import math
import os
import random
import sqlite3
import subprocess
import sys
from bisect import bisect_left
from datetime import date, datetime, timedelta

SEED_ML = 20260925

HISTORY_START = date(2022, 1, 1)    # the long tail's history starts a year early, so every
                                    # 2023 renewal has a full year behind it
KEY_START = date(2023, 1, 1)        # Book 1's key accounts begin here
END = date(2025, 12, 31)            # the last day anything is recorded

N_TAIL = 6_000
N_DUPLICATES = 212                  # trap: split across two IDs by the CRM migration
MIGRATION = date(2024, 3, 1)

SB_PRICE_CHANGE = date(2023, 7, 1)  # small-business list prices rise 6%
VOSS_PASS_THROUGH = date(2025, 2, 1)  # Voss lines rise 8% to long-tail accounts
VOSS_COST_RISE = date(2025, 1, 1)   # Book 1's planted cost rise, unchanged
SANITATION_LAUNCH = date(2024, 4, 1)
DEFECT_SKU = "MRD-CLE-001"          # the Pemberton Mills cloth
DEFECT_START, DEFECT_END = date(2024, 10, 7), date(2024, 12, 20)
HALLOWAY_END = date(2024, 6, 30)    # Book 1: Halloway's last order is 2024-06-30

DECIDE_BEFORE = 90                  # days before a contract ends that the account decides
NOTICE_BEFORE = 60                  # the renewal conversation: notice is given here

B0 = -2.44                          # set by hand so about 7% of 2023-2025 renewals fail

REGIONS = [(1, "Northeast", "Newark"), (2, "Southeast", "Atlanta"), (3, "Midwest", "Columbus"),
           (4, "West", "Sacramento"), (5, "Southwest", "Dallas")]
POSTCODE_FIRST = {1: "01", 2: "3", 3: "4", 4: "9", 5: "7"}
DESK = {1: "North desk", 3: "North desk", 2: "South desk", 4: "South desk", 5: "South desk"}

SEGMENT_MIX = [("Small business", 55), ("Mid-market", 30), ("Public sector", 10),
               ("Enterprise", 5)]
SEGMENT_RATE = {"Small business": 1.1, "Mid-market": 2.0, "Public sector": 1.6,
                "Enterprise": 3.2}          # median orders a month

MANAGERS = {
    1: ["Ines Varga", "Tom Ashby", "Rhea Lindqvist"],
    2: ["Marcus Hale", "Priti Nair", "Owen Castell"],
    3: ["Dara Whitlock", "Colm Reyes", "Hana Osei"],
    4: ["Luis Ferrand", "Maya Tolland", "Kit Ansel"],
    5: ["Jonah Pike", "Sara Quell", "Bea Montrose"],
}
RETENTION_DESK = "Retention desk"

NAME_HEADS = ["Ash", "Brad", "Carl", "Dun", "Elm", "Fair", "Glen", "Hart", "Ivy", "Kings",
              "Lang", "Marsh", "New", "Oak", "Pen", "Red", "Stan", "Thorn", "Wal", "York",
              "Bex", "Chil", "Dray", "Eas", "Fox", "Grey", "Hol", "Kel", "Lyn", "Mill",
              "Nor", "Ot", "Pres", "Rock", "Sel", "Tal", "Upp", "Ware", "West", "Wick"]
NAME_TAILS = ["ford", "ley", "ton", "wood", "field", "bury", "combe", "dale", "ham",
              "hurst", "more", "stead", "worth", "brook", "gate", "holm", "wick", "by",
              "well", "den", "mere", "cliffe", "ridge", "stone", "haven", "port", "vale",
              "croft", "land", "shaw"]
NAME_SUFFIXES = ["Cleaning", "Facilities", "Services", "Property Care", "Dental", "Schools",
                 "Hotels", "Logistics", "Foods", "Veterinary", "Clinic", "Motors",
                 "Fitness", "Builders", "Print", "Laundry", "Dairy", "Storage", "Kitchens",
                 "Engineering", "Nursery", "Cafe", "Salon", "Offices", "Estates"]

CANCELLATION_REASONS = ["Moved to a competitor on price", "Consolidated suppliers",
                        "Service and delivery problems", "Product quality", "Budget cuts",
                        "Business closed"]


# --------------------------------------------------------------------------- the truth

def renewal_logit(x: dict) -> float:
    """
    The log-odds that an account does not renew, decided DECIDE_BEFORE
    days before its contract ends, from what the warehouse recorded up
    to that day. Appendix G prints this function; Chapters 13 and 24
    depend on its dated terms.
    """
    d = x["decided_on"]
    after_pricing = d >= SB_PRICE_CHANGE
    z = B0
    z += 1.10 * math.log1p(x["recency_days"] / 30)
    z -= 0.70 * x["order_trend"]
    z += 0.30 * min(x["complaints_90"], 5)
    z -= 0.25 * math.log1p(x["spend_365"] / 1000)
    z -= 0.35 * math.sqrt(min(x["tenure_years"], 9))
    if x["legacy_terms"]:
        z += 0.55
    else:
        z -= (0.07 if after_pricing else 0.03) * x["discount_pct"]
    z += {"Small business": 0.75 if after_pricing else 0.25,
          "Mid-market": 0.0, "Public sector": -0.5,
          "Enterprise": -0.9}[x["segment"]]
    in_window = date(2024, 10, 1) <= d <= date(2025, 3, 31)
    if x["pemberton_complaint"] and in_window:
        z += 1.30
    if d >= VOSS_PASS_THROUGH:
        z += 2.40 * x["voss_share"]
    return z


def sigmoid(z: float) -> float:
    return 1 / (1 + math.exp(-z))


def auc(scores, labels) -> float:
    """Rank-based area under the ROC curve, ties averaged. No NumPy, on purpose."""
    pairs = sorted(zip(scores, labels))
    ranks, i = [0.0] * len(pairs), 0
    while i < len(pairs):
        j = i
        while j < len(pairs) and pairs[j][0] == pairs[i][0]:
            j += 1
        avg = (i + 1 + j) / 2
        for k in range(i, j):
            ranks[k] = avg
        i = j
    pos = sum(1 for _, y in pairs if y)
    neg = len(pairs) - pos
    rsum = sum(r for r, (_, y) in zip(ranks, pairs) if y)
    return (rsum - pos * (pos + 1) / 2) / (pos * neg)


# --------------------------------------------------------------------------- helpers

def poisson(rng: random.Random, lam: float) -> int:
    if lam <= 0:
        return 0
    if lam > 30:
        return max(0, int(round(rng.gauss(lam, math.sqrt(lam)))))
    floor, k, p = math.exp(-lam), 0, 1.0
    while True:
        p *= rng.random()
        if p <= floor:
            return k
        k += 1


def month_starts(start: date, end: date):
    d = date(start.year, start.month, 1)
    while d <= end:
        yield d
        d = date(d.year + d.month // 12, d.month % 12 + 1, 1)


def month_end(d: date) -> date:
    return date(d.year + d.month // 12, d.month % 12 + 1, 1) - timedelta(days=1)


def add_months(d: date, n: int) -> date:
    m = d.month - 1 + n
    y = d.year + m // 12
    m = m % 12 + 1
    last = month_end(date(y, m, 1)).day
    return date(y, m, min(d.day, last))


def seasonal(category_peak: int, d: date) -> float:
    return 1.35 if (d.month - 1) // 3 + 1 == category_peak else 1.0


# --------------------------------------------------------------------------- ticket text
#
# Tickets are written the way customers write: short, unpunctuated, misspelt, sometimes
# not in English. Each has a true intent, from which its true category and urgency follow.
# The desk that handles it assigns the category and priority the book's models learn
# from — mostly right, and in one place systematically different between the two desks.

INTENTS = {
    # intent: (true category, true urgency, templates)
    "late": ("Delivery", "Normal", [
        "order {oid} still hasnt arrived", "we were promised {day} for order {oid}, nothing yet",
        "delivery for {po} is {n} days late", "where is order {oid}",
        "order {oid} was due {day}, any update?", "still waiting on {oid}, second time this month"]),
    "missing": ("Delivery", "High", [
        "{n} boxes missing from order {oid}", "only half of order {oid} turned up",
        "order {oid} delivered but {sku} not in it", "short delivery on {oid}, missing {n} cases"]),
    "damaged": ("Delivery", "Normal", [
        "{sku} arrived crushed, outer box soaked through", "pallet damaged in transit, {n} cases of {sku} unusable",
        "driver dropped the delivery, {sku} split open", "{sku} came in a torn pack, half of it spilled"]),
    "defect": ("Quality", "High", [
        "{sku} falling apart after one use", "whole batch of {sku} is faulty",
        "{sku} not the same as last time, much thinner", "{sku} doesnt work, tried 3 of them",
        "the {sku} we got leaks from the seal"]),
    "cloth": ("Quality", "High", [
        "cloths falling apart after one wash - {sku}", "{sku} tearing straight out the pack, whole batch is bad",
        "{sku} shedding fibres everywhere, staff refusing to use them", "new batch of {sku} disintegrates when wet"]),
    "safety": ("Quality", "Urgent", [
        "{sku} gave off fumes when mixed, two staff felt ill", "{sku} ladder rung snapped, someone nearly fell",
        "label on {sku} is missing the hazard warning", "{sku} caught fire in the storeroom",
        "glove {sku} split and chemical got on a cleaners hand"]),
    "double": ("Billing", "Normal", [
        "invoice {inv} charged twice", "we have been billed twice for order {oid}",
        "card taken twice for {inv}", "duplicate charge on {inv} please refund"]),
    "credit": ("Billing", "Low", [
        "credit note for {inv} still not showing", "where is the credit for the return on {oid}",
        "{inv} should have the discount applied"]),
    "tax": ("Billing", "Low", [
        "we are tax exempt, please remove sales tax from {inv}", "why is there tax on {inv}, cert on file",
        "PO number missing on invoice {inv}, accounts wont pay without it"]),
    "return": ("Returns", "Normal", [
        "need to return {sku}, ordered wrong size", "RMA for order {oid} please",
        "collection booked {n} days ago, nobody came", "how do we send back {sku}",
        "return label for {oid} doesnt scan"]),
    "account": ("Account", "Low", [
        "can we add a second delivery address", "please remove {person} from our account",
        "send me our current price list", "update billing contact to {person}",
        "can we get statements monthly instead of weekly"]),
    "leaving": ("Account", "Normal", [
        "please send a final statement, we will not be renewing",
        "we are moving to another supplier when our contract ends",
        "close our account at the end of the contract please",
        "cancel auto renewal on our agreement"]),
    "stock": ("Stock", "Normal", [
        "is {sku} back in stock", "{sku} showing out of stock again", "when will {sku} be available",
        "can we get {sku} in the bigger pack size"]),
    "stock_urgent": ("Stock", "Urgent", [
        "we need {sku} today or the line stops", "completely out of {sku}, site cant open without it",
        "urgent - no {sku} left and inspection tomorrow", "{sku} out of stock and we are down to the last box, need same day"]),
}
URGENT_MODIFIERS = ["site is closed until this arrives", "production line is down",
                    "we open to the public tomorrow", "inspection this week"]
GREETINGS = ["", "", "", "Hi, ", "Hello, ", "hi team ", "Morning, "]
SIGNOFFS = ["", "", "", " thanks", " thx", " regards", " cheers", " pls advise", " asap!!"]
DAYS = ["monday", "tuesday", "wednesday", "thursday", "friday", "last week", "yesterday"]
PEOPLE = ["Jo", "Sam", "Alex", "Chris", "Pat", "Robin", "Lee", "Morgan"]
NON_ENGLISH = {
    "late": [("fr", "La commande {oid} n'est toujours pas arrivée."),
             ("es", "El pedido {oid} todavía no ha llegado."),
             ("de", "Bestellung {oid} ist immer noch nicht da.")],
    "defect": [("es", "El producto {sku} llegó defectuoso, necesitamos un reemplazo."),
               ("fr", "Le produit {sku} est défectueux, tout le lot."),
               ("de", "Der Artikel {sku} ist fehlerhaft.")],
    "double": [("de", "Die Rechnung {inv} wurde doppelt berechnet."),
               ("fr", "La facture {inv} a été facturée deux fois.")],
    "stock_urgent": [("es", "Necesitamos {sku} hoy, no nos queda nada."),
                     ("fr", "Plus de {sku} en stock, c'est urgent.")],
}
PRIORITIES = ["Low", "Normal", "High", "Urgent"]


def misspell(rng: random.Random, text: str) -> str:
    words = text.split(" ")
    for i, w in enumerate(words):
        if len(w) > 4 and w.isalpha() and rng.random() < 0.035:
            j = rng.randrange(len(w) - 1)
            words[i] = w[:j] + w[j + 1] + w[j] + w[j + 2:] if rng.random() < 0.6 else w[:j] + w[j + 1:]
    return " ".join(words)


def write_ticket(rng: random.Random, intent: str, sku: str, oid: int) -> tuple[str, str, bool]:
    """Returns (body, language, urgent_modifier_used)."""
    fill = {"oid": oid, "sku": sku, "po": f"PO-{rng.randint(1000, 9999)}",
            "inv": f"INV-{rng.randint(10000, 99999)}", "n": rng.randint(2, 9),
            "day": rng.choice(DAYS), "person": rng.choice(PEOPLE)}
    if intent in NON_ENGLISH and rng.random() < 0.05:
        lang, tmpl = rng.choice(NON_ENGLISH[intent])
        return tmpl.format(**fill), lang, False
    body = rng.choice(INTENTS[intent][2]).format(**fill)
    modified = False
    if intent in ("late", "missing", "stock") and rng.random() < 0.12:
        body += ", " + rng.choice(URGENT_MODIFIERS)
        modified = True
    body = rng.choice(GREETINGS) + body + rng.choice(SIGNOFFS)
    body = misspell(rng, body)
    if rng.random() < 0.35:
        body = body.lower()
    elif rng.random() < 0.05:
        body = body.upper()
    return body.strip(), "en", modified


def desk_labels(rng: random.Random, intent: str, desk: str, urgency: str) -> tuple[str, str]:
    """
    The category and priority the handling desk recorded. Mostly right. One convention
    differs by desk (trap: two teams label goods damaged in transit differently).
    """
    category = INTENTS[intent][0]
    if intent == "damaged" and desk == "North desk":
        category = "Quality"
    elif rng.random() < 0.05:
        category = rng.choice(["Delivery", "Quality", "Billing", "Returns", "Account", "Stock"])
    i = PRIORITIES.index(urgency)
    r = rng.random()
    if r < 0.10 and i > 0:
        i -= 1
    elif r < 0.18 and i < 3:
        i += 1
    return category, PRIORITIES[i]


# --------------------------------------------------------------------------- Book 1 input

def load_book1(book1: str):
    db = os.path.join(book1, "warehouse", "meridian.db")
    if not os.path.exists(db):
        here = os.path.dirname(os.path.abspath(__file__))
        print("  Book 1's warehouse is missing; generating it first")
        subprocess.run([sys.executable, os.path.join(here, "generate.py"), "--out", book1],
                       check=True)
    con = sqlite3.connect(db)
    q = lambda sql: con.execute(sql).fetchall()      # noqa: E731
    suppliers = q("SELECT supplier_id, name, country, tier FROM suppliers ORDER BY supplier_id")
    products = [dict(zip(("sku", "name", "category", "supplier_id", "unit_cost", "list_price"), r))
                for r in q("SELECT sku, name, category, supplier_id, unit_cost, list_price "
                           "FROM products ORDER BY sku")]
    customers = q("SELECT customer_id, name, region_id, segment, since FROM customers "
                  "ORDER BY customer_id")
    orders = q("SELECT order_id, customer_id, order_date, channel FROM orders ORDER BY order_id")
    lines = q("SELECT order_id, line_no, sku, qty, unit_price, unit_cost, discount_pct "
              "FROM order_lines ORDER BY order_id, line_no")
    con.close()
    with open(db, "rb") as f:
        digest = hashlib.sha256(f.read()).hexdigest()
    return suppliers, products, customers, orders, lines, digest


# --------------------------------------------------------------------------- accounts

class Account:
    __slots__ = ("id", "name", "region", "segment", "since", "postcode", "key", "rate",
                 "prefs", "engagement", "manager", "history", "contract", "orders",
                 "tickets", "closed_on", "legacy_id", "legacy_name", "pemberton_complaint")

    def __init__(self, **kw):
        for k in self.__slots__:
            setattr(self, k, kw.get(k))
        self.orders = []            # (date, revenue, voss_revenue) — for the truth's features
        self.tickets = []           # (date, desk_category, sku)
        self.history = []           # [valid_from, segment, manager]
        self.pemberton_complaint = None


def make_names(rng: random.Random, n: int, taken: set) -> list[str]:
    names = []
    while len(names) < n:
        base = rng.choice(NAME_HEADS) + rng.choice(NAME_TAILS)
        name = f"{base} {rng.choice(NAME_SUFFIXES)}"
        if name not in taken:
            taken.add(name)
            names.append(name)
    return names


def legacy_variant(rng: random.Random, name: str) -> str:
    """How the old CRM wrote the same company: the kind of difference entity resolution meets."""
    style = rng.randrange(4)
    if style == 0:
        return name.upper()
    if style == 1:
        return name + " Inc"
    if style == 2:
        return name.replace(" ", "  ", 1)
    return name + " (old)"


def build_tail(rng: random.Random, taken: set, first_id: int, products: list, voss_id: int):
    names = make_names(rng, N_TAIL, taken)
    segs, seg_w = zip(*SEGMENT_MIX)
    accounts = []
    for k, name in enumerate(names):
        region = rng.choices([1, 2, 3, 4, 5], weights=[24, 20, 16, 14, 26])[0]
        segment = rng.choices(segs, weights=seg_w)[0]
        if rng.random() < 0.6:
            since = date(rng.randint(2012, 2021), rng.randint(1, 12), 1)
        else:
            since = date(rng.randint(2022, 2025), rng.randint(1, 12), 1)
            if since > date(2025, 9, 1):
                since = date(2025, rng.randint(1, 9), 1)
        # Supplier and category preferences: why one account buys a lot of Voss and
        # another almost none, which matters after 2025's price rise.
        prefs = []
        voss_lean = rng.gammavariate(0.7, 1.0)
        for p in products:
            w = rng.gammavariate(1.2, 1.0)
            if p["supplier_id"] == voss_id:
                w *= 3.0 * voss_lean
            prefs.append(w)
        accounts.append(Account(
            id=first_id + k, name=name, region=region, segment=segment, since=since,
            postcode=POSTCODE_FIRST[region] + f"{rng.randint(0, 10 ** (5 - len(POSTCODE_FIRST[region])) - 1):0{5 - len(POSTCODE_FIRST[region])}d}",
            key=False, rate=SEGMENT_RATE[segment] * rng.lognormvariate(0, 0.45),
            prefs=prefs, engagement=rng.gauss(0, 0.3),
            manager=rng.choice(MANAGERS[region])))
    return accounts


def first_contract(rng: random.Random, a: Account, start: date) -> dict:
    """The contract in force on `start` (or on the day the account arrives)."""
    term = 24 if rng.random() < 0.15 else 12
    legacy = a.since < date(2019, 1, 1) and rng.random() < 0.7
    begin = a.since
    while add_months(begin, term) <= start:
        begin = add_months(begin, term)
    return {"start": begin, "end": add_months(begin, term) - timedelta(days=1), "term": term,
            "legacy": legacy, "discount": None if legacy else
            rng.choices([0, 3, 5, 8, 10, 12, 15], weights=[20, 15, 20, 18, 14, 8, 5])[0],
            "decided": None, "leaves": None, "p": None, "x": None}


# --------------------------------------------------------------------------- features

def features_at(a: Account, d: date, contract: dict) -> dict:
    """What the warehouse knew about account `a` on day `d`: the inputs to the truth."""
    dates = [o[0] for o in a.orders]
    i = bisect_left(dates, d)
    before = a.orders[:i]
    last = before[-1][0] if before else None
    recency = min((d - last).days, 365) if last else 365
    lo90, lo180, lo365 = d - timedelta(days=90), d - timedelta(days=180), d - timedelta(days=365)
    n90 = sum(1 for o in before if o[0] >= lo90)
    n_prev = sum(1 for o in before if lo180 <= o[0] < lo90)
    spend = sum(o[1] for o in before if o[0] >= lo365)
    voss = sum(o[2] for o in before if o[0] >= lo365)
    complaints = sum(1 for t in a.tickets if lo90 <= t[0] < d and t[1] in ("Quality", "Delivery"))
    pem = a.pemberton_complaint is not None and a.pemberton_complaint < d
    return {
        "decided_on": d, "recency_days": recency,
        "order_trend": max(-2.0, min(2.0, math.log((n90 + 1) / (n_prev + 1)))),
        "complaints_90": complaints, "spend_365": spend,
        "voss_share": voss / spend if spend else 0.0,
        "tenure_years": (d - a.since).days / 365.25,
        "legacy_terms": contract["legacy"], "discount_pct": contract["discount"] or 0,
        "segment": segment_on(a, d), "pemberton_complaint": pem,
    }


def segment_on(a: Account, d: date) -> str:
    seg = a.history[0][1]
    for valid_from, s, _ in a.history:
        if valid_from <= d:
            seg = s
    return seg


# --------------------------------------------------------------------------- simulation

def simulate(rng, accounts, products, voss_id, cat_peak):
    """
    Month by month: orders, tickets, engagement drifting, contracts coming up, decisions
    made ninety days out, and the accounts that leave winding down before they go.
    """
    contracts, orders, lines, tickets = [], [], [], []
    oid = 1_000_000                      # long-tail order ids never collide with Book 1's
    tid = 0

    for a in accounts:
        a.history.append([max(a.since, HISTORY_START) if not a.key else KEY_START,
                          a.segment, a.manager])
        a.contract = first_contract(rng, a, max(a.since, HISTORY_START))

    for m in month_starts(HISTORY_START, END):
        m_end = month_end(m)
        span = (m_end - m).days + 1
        available = [i for i, p in enumerate(products)
                     if p["category"] != "Sanitation" or m >= SANITATION_LAUNCH]
        for a in accounts:
            if a.key or a.since > m_end or (a.closed_on and a.closed_on < m):
                continue
            c = a.contract

            # 1. Candidate orders for the month, before anyone's decision is known.
            start_day = max(m, a.since)
            lam = a.rate * math.exp(a.engagement) * ((m_end - start_day).days + 1) / span
            cand = sorted(start_day + timedelta(days=rng.randrange((m_end - start_day).days + 1))
                          for _ in range(poisson(rng, lam)))
            cand = _make_orders(rng, a, cand, products, available, voss_id, cat_peak)

            # 2. Tickets, before any decision this month, so that a decision sees every
            #    ticket dated before its day.
            n_orders = len(cand)
            leaving = c["leaves"] and c["decided"] and m_end >= c["end"] - timedelta(days=NOTICE_BEFORE)
            lam_t = 0.035 + 0.03 * n_orders + (0.35 if leaving else 0)
            recent_defect = any(o[0] >= m - timedelta(days=90) and o[3] for o in cand) or \
                any(o[0] >= m - timedelta(days=90) and o[4] for o in a.orders[-12:])
            in_defect = DEFECT_START <= m_end and m <= DEFECT_END
            for _ in range(poisson(rng, lam_t)):
                intent = rng.choices(
                    ["late", "missing", "damaged", "defect", "safety", "double", "credit", "tax",
                     "return", "account", "stock", "stock_urgent", "leaving"],
                    weights=[16, 7, 7, 10, 1.2, 6, 5, 4, 9, 8, 9, 2.5, 30 if leaving else 0])[0]
                tid = _ticket(rng, a, m, m_end, intent, products, tickets, tid, oid)
            if in_defect and recent_defect:
                for _ in range(poisson(rng, 0.45)):
                    tid = _ticket(rng, a, max(m, DEFECT_START), min(m_end, DEFECT_END), "cloth",
                                  products, tickets, tid, oid, sku=DEFECT_SKU)

            # 3. A decision due this month sees only the orders recorded before its day.
            #    Those are committed first; they cannot be affected by what is decided.
            decide = c["end"] - timedelta(days=DECIDE_BEFORE)
            if c["decided"] is None and decide <= m_end:
                day = max(decide, m)
                oid = _commit(a, [o for o in cand if o[0] < day], orders, lines, oid)
                cand = [o for o in cand if o[0] >= day]
                x = features_at(a, day, c)
                p = sigmoid(renewal_logit(x))
                c.update(decided=day, p=p, x=x, leaves=rng.random() < p)

            # 4. An account that has decided to go winds down: orders thin out from a
            #    fortnight after the decision, and stop when the contract ends.
            kept = []
            for o in cand:
                d = o[0]
                if c["leaves"] and c["decided"] is not None:
                    fade_from = c["decided"] + timedelta(days=15)
                    if d > c["end"]:
                        continue
                    if d >= fade_from:
                        frac = (d - fade_from).days / max(1, (c["end"] - fade_from).days)
                        if rng.random() > 1 - 0.7 * frac:
                            continue
                kept.append(o)
            oid = _commit(a, kept, orders, lines, oid)

            # 5. The contract ends this month: renew, or close the account.
            if c["end"] <= m_end:
                contracts.append((a, c))
                if c["leaves"]:
                    a.closed_on = c["end"]
                else:
                    legacy = c["legacy"] and rng.random() < 0.7
                    disc = None if legacy else (
                        c["discount"] if c["discount"] is not None and rng.random() < 0.7 else
                        rng.choices([0, 3, 5, 8, 10, 12, 15], weights=[20, 15, 20, 18, 14, 8, 5])[0])
                    start = c["end"] + timedelta(days=1)
                    a.contract = {"start": start, "end": add_months(start, c["term"]) - timedelta(days=1),
                                  "term": c["term"], "legacy": legacy, "discount": disc,
                                  "decided": None, "leaves": None, "p": None, "x": None}

            # 6. Engagement drifts; segments occasionally change; managers move.
            a.engagement = 0.88 * a.engagement + rng.gauss(0, 0.30)
            if a.segment == "Small business" and rng.random() < 0.0025:
                a.segment = "Mid-market"
                a.rate *= 1.4
                a.history.append([m_end + timedelta(days=1), a.segment, a.manager])
            elif rng.random() < 0.012:
                a.manager = rng.choice([x for x in MANAGERS[a.region] if x != a.manager])
                a.history.append([m_end + timedelta(days=1), a.segment, a.manager])

    # Contracts still running at the end of the record: decided or not, their outcome is
    # not in the warehouse yet.
    for a in accounts:
        if not a.key and not (a.closed_on and a.closed_on <= END):
            contracts.append((a, a.contract))
    return contracts, orders, lines, tickets


def _make_orders(rng, a, days, products, available, voss_id, cat_peak):
    """Each order once, with its lines: (day, revenue, voss_revenue, has_defect_sku, lines, channel)."""
    made = []
    for d in days:
        revenue = voss_rev = 0.0
        defect = False
        specs = []
        picks = rng.choices(available, weights=[a.prefs[i] for i in available],
                            k=rng.randint(1, 4))
        channel = rng.choices(["Web", "Field sales", "Telesales", "Partner"],
                              weights=[0.55, 0.12, 0.2, 0.13])[0]
        for i in picks:
            p = products[i]
            qty = max(1, int(rng.lognormvariate(1.1, 0.7) * seasonal(cat_peak[p["category"]], d)))
            disc = a.contract["discount"] if a.contract["discount"] is not None else rng.choice([0, 0, 5])
            price = p["list_price"]
            if a.segment == "Small business" and d >= SB_PRICE_CHANGE:
                price *= 1.06
            if p["supplier_id"] == voss_id and d >= VOSS_PASS_THROUGH:
                price *= 1.08
            price = round(price * (1 - disc / 100), 2)
            cost = (round(p["unit_cost"] * 1.12, 2)
                    if p["supplier_id"] == voss_id and d >= VOSS_COST_RISE else p["unit_cost"])
            revenue += qty * price
            if p["supplier_id"] == voss_id:
                voss_rev += qty * price
            defect = defect or p["sku"] == DEFECT_SKU
            specs.append((p["sku"], qty, price, cost, disc))
        made.append((d, revenue, voss_rev, defect, specs, channel))
    return made


def _commit(a, made, orders, lines, oid):
    """Write orders to the warehouse lists and to the account's own record."""
    for d, revenue, voss_rev, defect, specs, channel in made:
        oid += 1
        orders.append((oid, a.legacy_id if (a.legacy_id and d < MIGRATION) else a.id,
                       d.isoformat(), channel))
        for line_no, (sku, qty, price, cost, disc) in enumerate(specs, start=1):
            lines.append((oid, line_no, sku, qty, price, cost, disc))
        a.orders.append((d, revenue, voss_rev, False, defect))
    return oid


def _ticket(rng, a, lo, hi, intent, products, tickets, tid, oid, sku=None):
    tid += 1
    day = lo + timedelta(days=rng.randrange((hi - lo).days + 1))
    if a.closed_on and day > a.closed_on:
        day = a.closed_on
    sku = sku or rng.choice(products)["sku"]
    body, lang, modified = write_ticket(rng, intent, sku, rng.randint(max(1, oid - 5000), max(2, oid)))
    true_cat, urgency = INTENTS[intent][0], INTENTS[intent][1]
    if modified:
        urgency = "Urgent" if urgency == "High" else "High"
    desk = DESK[a.region]
    cat, pri = desk_labels(rng, intent, desk, urgency)
    opened = datetime(day.year, day.month, day.day, rng.randint(7, 18), rng.randrange(60))
    account_id = a.legacy_id if (a.legacy_id and day < MIGRATION) else a.id
    mentions_sku = "{sku}" in " ".join(INTENTS[intent][2]) or intent in ("cloth",)
    tickets.append({
        "ticket_id": f"FT-{tid:06d}", "account_id": account_id,
        "opened_at": opened.isoformat(sep=" "), "channel": rng.choices(
            ["Email", "Web form", "Phone"], weights=[55, 30, 15])[0],
        "sku": sku if mentions_sku and sku in body else None, "language": lang, "body": body,
        "desk": desk, "category": cat, "priority": pri,
        "true_intent": intent, "true_category": true_cat, "true_urgency": urgency,
    })
    stored_sku = sku if mentions_sku and sku in body else None
    a.tickets.append((day, cat, stored_sku))
    # A complaint about the defective cloth, as the warehouse can see it: the SKU named in a
    # Quality or Delivery ticket during the defect window.
    if (stored_sku == DEFECT_SKU and cat in ("Quality", "Delivery")
            and DEFECT_START <= day <= DEFECT_END and a.pemberton_complaint is None):
        a.pemberton_complaint = day
    return tid


# --------------------------------------------------------------------------- key accounts

def key_accounts(rng, customers, orders_b1, lines_b1, products, voss_id, tickets, tid):
    """
    Book 1's 58 customers, their orders unchanged. They renew every year except Halloway
    Group, whose contract ends on 30 June 2024 — and whose complaints in the months before
    are the only warning a model could have had.
    """
    by_sku = {p["sku"]: p for p in products}
    rev = {}
    for oid, line_no, sku, qty, price, cost, disc in lines_b1:
        r = rev.setdefault(oid, [0.0, 0.0, False])
        r[0] += qty * price
        if by_sku[sku]["supplier_id"] == voss_id:
            r[1] += qty * price
        r[2] = r[2] or sku == DEFECT_SKU
    per_customer = {}
    for oid, cid, odate, _ in orders_b1:
        per_customer.setdefault(cid, []).append(
            (date.fromisoformat(odate), rev[oid][0], rev[oid][1], False, rev[oid][2]))

    accounts, contracts = [], []
    for cid, name, region, segment, since in customers:
        a = Account(id=cid, name=name, region=region, segment=segment,
                    since=date.fromisoformat(since), key=True,
                    postcode=POSTCODE_FIRST[region] + "0" * (5 - len(POSTCODE_FIRST[region])),
                    manager=MANAGERS[region][cid % 3], rate=0, prefs=None, engagement=0)
        a.postcode = POSTCODE_FIRST[region] + f"{(cid * 7919) % 10 ** (5 - len(POSTCODE_FIRST[region])):0{5 - len(POSTCODE_FIRST[region])}d}"
        a.orders = sorted(per_customer.get(cid, []))
        a.history.append([KEY_START, segment, a.manager])
        halloway = name.startswith("Halloway")
        # Tickets: a steady trickle, and for Halloway a run of complaints in early 2024.
        for m in month_starts(KEY_START, HALLOWAY_END if halloway else END):
            n = sum(1 for o in a.orders if m <= o[0] <= month_end(m))
            for _ in range(poisson(rng, 0.002 * n)):
                intent = rng.choice(["late", "missing", "double", "return", "account", "stock"])
                tid = _ticket(rng, a, m, month_end(m), intent, products, tickets, tid, 60000)
        if halloway:
            for day, intent in [(date(2024, 1, 9), "double"), (date(2024, 1, 23), "late"),
                                (date(2024, 2, 6), "missing"), (date(2024, 2, 20), "late"),
                                (date(2024, 3, 4), "damaged"), (date(2024, 3, 5), "double"),
                                (date(2024, 3, 19), "missing"), (date(2024, 3, 28), "late"),
                                (date(2024, 4, 15), "leaving"), (date(2024, 5, 2), "leaving")]:
                tid = _ticket(rng, a, day, day, intent, products, tickets, tid, 60000)
        # Contracts: annual, on the anniversary of `since`, covering 2023 to 2025.
        start = date(2022, int(since[5:7]), 1)
        while start <= END:
            c = {"start": start, "end": add_months(start, 12) - timedelta(days=1), "term": 12,
                 "legacy": False, "discount": 10 if segment == "Enterprise" else 5,
                 "decided": None, "leaves": False, "p": None, "x": None}
            decide = c["end"] - timedelta(days=DECIDE_BEFORE)
            if c["end"] >= KEY_START and decide <= END:
                x = features_at(a, max(decide, KEY_START), c)
                c.update(decided=max(decide, KEY_START), x=x, p=sigmoid(renewal_logit(x)))
            if halloway and c["start"] <= HALLOWAY_END <= c["end"]:
                c["end"] = HALLOWAY_END            # an early, negotiated exit
                c["leaves"] = True
                a.closed_on = HALLOWAY_END
            if c["end"] >= KEY_START:
                contracts.append((a, c))
            if c["leaves"]:
                break
            start = c["end"] + timedelta(days=1)
        accounts.append(a)
    return accounts, contracts, tid


# --------------------------------------------------------------------------- writing

SCHEMA = """
CREATE TABLE regions   (region_id INTEGER PRIMARY KEY, name TEXT, depot TEXT);
CREATE TABLE suppliers (supplier_id INTEGER PRIMARY KEY, name TEXT, country TEXT, tier TEXT);
CREATE TABLE products  (sku TEXT PRIMARY KEY, name TEXT, category TEXT, supplier_id INTEGER,
                        unit_cost REAL, list_price REAL, launched_on TEXT);
-- The CRM as it stands on the last day of the record. Current values only: for what an
-- account looked like on an earlier date, use account_history.
CREATE TABLE accounts  (account_id INTEGER PRIMARY KEY, name TEXT, region_id INTEGER,
                        segment TEXT, account_manager TEXT, since TEXT, postcode TEXT,
                        is_key_account INTEGER, crm_source TEXT, closed_on TEXT);
CREATE TABLE account_history (account_id INTEGER, valid_from TEXT, valid_to TEXT,
                        segment TEXT, account_manager TEXT);
CREATE TABLE contracts (contract_id INTEGER PRIMARY KEY, account_id INTEGER, start_date TEXT,
                        end_date TEXT, term_months INTEGER, legacy_terms INTEGER,
                        discount_pct INTEGER, outcome TEXT, notice_date TEXT,
                        cancellation_reason TEXT);
CREATE TABLE orders    (order_id INTEGER PRIMARY KEY, account_id INTEGER, order_date TEXT,
                        channel TEXT);
CREATE TABLE order_lines (order_id INTEGER, line_no INTEGER, sku TEXT, qty INTEGER,
                        unit_price REAL, unit_cost REAL, discount_pct INTEGER,
                        PRIMARY KEY (order_id, line_no));
CREATE TABLE tickets   (ticket_id TEXT PRIMARY KEY, account_id INTEGER, opened_at TEXT,
                        channel TEXT, sku TEXT, language TEXT, body TEXT, desk TEXT,
                        category TEXT, priority TEXT);
CREATE TABLE price_events (effective_on TEXT, scope TEXT, change_pct REAL, note TEXT);

CREATE INDEX idx_orders_acct  ON orders(account_id, order_date);
CREATE INDEX idx_orders_date  ON orders(order_date);
CREATE INDEX idx_tickets_acct ON tickets(account_id, opened_at);
CREATE INDEX idx_contracts_end ON contracts(end_date);
CREATE INDEX idx_history_acct ON account_history(account_id, valid_from);

CREATE VIEW v_sales AS
SELECT o.order_id, o.order_date, o.channel, a.account_id, a.name AS account, a.segment,
       r.name AS region, p.sku, p.category, s.name AS supplier, l.qty, l.unit_price,
       l.unit_cost, l.discount_pct,
       ROUND(l.qty * l.unit_price, 2) AS revenue,
       ROUND(l.qty * (l.unit_price - l.unit_cost), 2) AS gross_profit
FROM order_lines l
JOIN orders    o ON o.order_id = l.order_id
JOIN accounts  a ON a.account_id = o.account_id
JOIN regions   r ON r.region_id = a.region_id
JOIN products  p ON p.sku = l.sku
JOIN suppliers s ON s.supplier_id = p.supplier_id;
"""


def write_all(out, suppliers, products, accounts, contracts, orders, lines, tickets, rng):
    wdir = os.path.join(out, "warehouse")
    tdir = os.path.join(out, "truth")
    os.makedirs(wdir, exist_ok=True)
    os.makedirs(tdir, exist_ok=True)
    path = os.path.join(wdir, "meridian_ml.db")
    if os.path.exists(path):
        os.remove(path)
    con = sqlite3.connect(path)
    con.executescript(SCHEMA)
    con.executemany("INSERT INTO regions VALUES (?,?,?)", REGIONS)
    con.executemany("INSERT INTO suppliers VALUES (?,?,?,?)", suppliers)
    con.executemany("INSERT INTO products VALUES (?,?,?,?,?,?,?)",
                    [(p["sku"], p["name"], p["category"], p["supplier_id"], p["unit_cost"],
                      p["list_price"], SANITATION_LAUNCH.isoformat() if p["category"] == "Sanitation"
                      else None) for p in products])

    acct_rows, hist_rows = [], []
    for a in sorted(accounts, key=lambda a: a.id):
        acct_rows.append((a.id, a.name, a.region, a.segment, a.manager, a.since.isoformat(),
                          a.postcode, int(a.key), "Meridian CRM",
                          a.closed_on.isoformat() if a.closed_on and a.closed_on <= END else None))
        if a.legacy_id:
            acct_rows.append((a.legacy_id, a.legacy_name, a.region, a.history[0][1],
                              a.history[0][2], a.since.isoformat(), a.postcode, 0,
                              "Legacy CRM", None))
        for i, (vf, seg, mgr) in enumerate(a.history):
            vt = a.history[i + 1][0] - timedelta(days=1) if i + 1 < len(a.history) else None
            hist_rows.append((a.id, vf.isoformat(), vt.isoformat() if vt else None, seg, mgr))
    con.executemany("INSERT INTO accounts VALUES (?,?,?,?,?,?,?,?,?,?)", acct_rows)
    con.executemany("INSERT INTO account_history VALUES (?,?,?,?,?)", hist_rows)

    contract_rows, truth_rows = [], []
    for n, (a, c) in enumerate(sorted(contracts, key=lambda ac: (ac[1]["end"], ac[0].id)), 1):
        ended = c["end"] <= END
        outcome = ("not_renewed" if c["leaves"] else "renewed") if ended else None
        notice = (c["end"] - timedelta(days=NOTICE_BEFORE)).isoformat() \
            if ended and c["leaves"] else None
        reason = None
        if ended and c["leaves"]:
            x = c["x"] or {}
            w = [4 + 30 * x.get("voss_share", 0) * (c["end"] >= VOSS_PASS_THROUGH), 3,
                 2 + x.get("complaints_90", 0), 1 + 4 * bool(x.get("pemberton_complaint")), 3, 2]
            reason = rng.choices(CANCELLATION_REASONS, weights=w)[0]
        contract_rows.append((n, a.id, c["start"].isoformat(), c["end"].isoformat(), c["term"],
                              int(c["legacy"]), c["discount"], outcome, notice, reason))
        if c["p"] is not None:
            x = c["x"]
            truth_rows.append((n, a.id, c["decided"].isoformat(), c["end"].isoformat(),
                               round(c["p"], 6), outcome or "", int(a.key),
                               x["recency_days"], round(x["order_trend"], 6), x["complaints_90"],
                               round(x["spend_365"], 2), round(x["voss_share"], 6),
                               round(x["tenure_years"], 4), int(x["legacy_terms"]),
                               x["discount_pct"], x["segment"], int(x["pemberton_complaint"])))
    con.executemany("INSERT INTO contracts VALUES (?,?,?,?,?,?,?,?,?,?)", contract_rows)
    con.executemany("INSERT INTO orders VALUES (?,?,?,?)", orders)
    con.executemany("INSERT INTO order_lines VALUES (?,?,?,?,?,?,?)", lines)
    con.executemany("INSERT INTO tickets VALUES (?,?,?,?,?,?,?,?,?,?)",
                    [(t["ticket_id"], t["account_id"], t["opened_at"], t["channel"], t["sku"],
                      t["language"], t["body"], t["desk"], t["category"], t["priority"])
                     for t in sorted(tickets, key=lambda t: (t["opened_at"], t["ticket_id"]))])
    con.executemany("INSERT INTO price_events VALUES (?,?,?,?)", [
        (SB_PRICE_CHANGE.isoformat(), "Small business, all lines", 6.0, "List price review"),
        (VOSS_COST_RISE.isoformat(), "Voss Industrial, unit cost", 12.0, "Supplier cost increase"),
        (VOSS_PASS_THROUGH.isoformat(), "Voss Industrial lines, non-key accounts", 8.0,
         "Partial pass-through of the Voss cost increase"),
    ])
    con.commit()
    con.close()

    with open(os.path.join(tdir, "renewals.csv"), "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["contract_id", "account_id", "decided_on", "end_date", "p_leave",
                    "outcome", "key_account", "recency_days", "order_trend", "complaints_90",
                    "spend_365", "voss_share", "tenure_years", "legacy_terms", "discount_pct",
                    "segment", "pemberton_complaint"])
        w.writerows(truth_rows)
    with open(os.path.join(tdir, "tickets.csv"), "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["ticket_id", "true_intent", "true_category", "true_urgency"])
        w.writerows(sorted((t["ticket_id"], t["true_intent"], t["true_category"],
                            t["true_urgency"]) for t in tickets))
    return path, truth_rows


# --------------------------------------------------------------------------- entry point

def main():
    here = os.path.dirname(os.path.abspath(__file__))
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--book1", default=os.path.join(here, "..", "..", "data", "meridian"))
    ap.add_argument("--out", default=os.path.join(here, "..", "..", "data", "meridian-ml"))
    args = ap.parse_args()
    out = os.path.abspath(args.out)

    print("Meridian for machine learning — generating")
    suppliers, products, customers, orders_b1, lines_b1, b1_digest = load_book1(
        os.path.abspath(args.book1))
    voss_id = next(s[0] for s in suppliers if s[1] == "Voss Industrial")
    cat_peak = {"Cleaning": 1, "Safety": 3, "Packaging": 4, "Facilities": 2, "Sanitation": 1}

    rng = random.Random(SEED_ML)
    tickets, tid = [], 0
    keys, key_contracts, tid = key_accounts(rng, customers, orders_b1, lines_b1, products,
                                            voss_id, tickets, tid)
    taken = {a.name for a in keys}
    tail = build_tail(rng, taken, 59, products, voss_id)

    # Trap: the CRM migration split some accounts across two IDs.
    for k, a in enumerate(rng.sample([a for a in tail if a.since < date(2023, 6, 1)],
                                     N_DUPLICATES)):
        a.legacy_id = 90_001 + k
        a.legacy_name = legacy_variant(rng, a.name)

    print(f"  accounts    {len(keys)} key accounts from Book 1, {len(tail):,} long-tail, "
          f"{N_DUPLICATES} of them also under a legacy CRM id")
    t_contracts, orders, lines, _ = simulate_wrapped(rng, tail, products, voss_id, cat_peak,
                                                     tickets, tid)
    # Trap: accounts that are leaving are handed to the retention desk after notice, and
    # the CRM keeps only the current manager.
    for a, c in t_contracts:
        if c["leaves"] and c["end"] <= END and rng.random() < 0.75:
            notice = c["end"] - timedelta(days=NOTICE_BEFORE)
            seg = segment_on(a, c["end"])
            a.history = [h for h in a.history if h[0] < notice] + [[notice, seg, RETENTION_DESK]]
            a.manager = RETENTION_DESK
        elif not c["leaves"] and c["decided"] and c["end"] <= END and rng.random() < 0.02:
            a.history.append([c["end"] - timedelta(days=NOTICE_BEFORE), segment_on(a, c["end"]),
                              RETENTION_DESK])
            a.history.append([c["end"] + timedelta(days=1), segment_on(a, c["end"]),
                              rng.choice(MANAGERS[a.region])])
            a.history.sort(key=lambda h: h[0])
            a.manager = a.history[-1][2]
    all_orders = [(o[0], o[1], o[2], o[3]) for o in orders_b1] + orders
    all_lines = list(lines_b1) + lines
    print(f"  sales       {len(all_orders):,} orders, {len(all_lines):,} order lines "
          f"({len(orders):,} and {len(lines):,} from the long tail)")

    path, truth = write_all(out, suppliers, products, keys + tail, key_contracts + t_contracts,
                            all_orders, all_lines, tickets, rng)
    print(f"  warehouse   {os.path.relpath(path, out)}  ({os.path.getsize(path) / 1e6:.1f} MB)")
    print(f"  tickets     {len(tickets):,}")

    # The numbers the book quotes about the dataset itself, and the ceiling on any model.
    scored = [(float(r[4]), r[5] == "not_renewed") for r in truth
              if r[5] and not r[6] and r[3] >= "2023-01-01"]
    rate = sum(y for _, y in scored) / len(scored)
    ceiling = auc([p for p, _ in scored], [y for _, y in scored])
    print(f"  renewals    {len(scored):,} decided in 2023-2025, {rate:.1%} not renewed")
    print(f"  ceiling     AUC {ceiling:.3f} for a model that knew the true process")

    manifest = {
        "name": "Meridian Supply Co., scaled for machine learning",
        "seed": SEED_ML, "generated_by": "code/meridian/generate_ml.py",
        "synthetic": True, "contains_personal_data": False,
        "book1_warehouse_sha256": b1_digest,
        "history": {"long_tail_from": HISTORY_START.isoformat(),
                    "key_accounts_from": KEY_START.isoformat(), "to": END.isoformat()},
        "counts": {"key_accounts": len(keys), "long_tail_accounts": len(tail),
                   "legacy_duplicate_ids": N_DUPLICATES,
                   "contracts": len(key_contracts) + len(t_contracts),
                   "orders": len(all_orders), "order_lines": len(all_lines),
                   "tickets": len(tickets)},
        "renewals": {"decided_2023_2025_long_tail": len(scored),
                     "not_renewed_rate": round(rate, 4), "ceiling_auc": round(ceiling, 4),
                     "decided_days_before_end": DECIDE_BEFORE,
                     "notice_days_before_end": NOTICE_BEFORE},
        "events": {
            "small_business_price_rise": SB_PRICE_CHANGE.isoformat(),
            "sanitation_launch": SANITATION_LAUNCH.isoformat(),
            "crm_migration": MIGRATION.isoformat(),
            "halloway_contract_ends": HALLOWAY_END.isoformat(),
            "pemberton_defect": [DEFECT_START.isoformat(), DEFECT_END.isoformat()],
            "voss_cost_rise": VOSS_COST_RISE.isoformat(),
            "voss_price_pass_through": VOSS_PASS_THROUGH.isoformat(),
        },
        "traps": {
            "cancellation_reason": "contracts.cancellation_reason is filled only for accounts "
                                   "that left (Chapter 13)",
            "retention_desk": "accounts.account_manager holds today's manager; leavers were "
                              "handed to the Retention desk after notice (Chapter 13)",
            "crm_duplicates": f"{N_DUPLICATES} accounts ordered under a legacy id before "
                              f"{MIGRATION.isoformat()} (Chapter 4)",
            "legacy_discount_missing": "contracts.discount_pct is NULL on legacy terms, and "
                                       "legacy terms raise the chance of leaving (Chapter 5)",
            "pricing_change_2023": "the effect of segment and discount changes on "
                                   f"{SB_PRICE_CHANGE.isoformat()} (Chapter 8)",
            "imbalance": "most renewals renew (Chapter 14)",
            "desk_conventions": "North desk files goods damaged in transit as Quality; South "
                                "desk as Delivery (Chapter 20)",
        },
    }
    with open(os.path.join(out, "manifest.json"), "w") as f:
        json.dump(manifest, f, indent=2)
    print(f"  manifest    manifest.json\n  Output: {out}")


def simulate_wrapped(rng, tail, products, voss_id, cat_peak, tickets, tid):
    """simulate() with the ticket list and id counter shared with the key accounts."""
    contracts, orders, lines, t = simulate(rng, tail, products, voss_id, cat_peak)
    # simulate() numbers its own tickets from 1; renumber after the key accounts' tickets.
    for k, row in enumerate(t, start=tid + 1):
        row["ticket_id"] = f"FT-{k:06d}"
    tickets.extend(t)
    return contracts, orders, lines, tid + len(t)


if __name__ == "__main__":
    main()
