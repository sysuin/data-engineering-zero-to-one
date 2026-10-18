#!/usr/bin/env python3
"""
Generate Meridian's operational sources: the systems Books 1 and 2's warehouses came from.

Book 2's warehouse (`meridian_ml.db`) is tidy. Real companies do not keep their data
tidy; they keep it in the systems that run the business, each of which has its own
habits. This file runs the warehouse backwards: every order, account, contract and
ticket Book 2 used is emitted the way an operational system would have held it, with
the mess such systems add, and a pipeline that handles the mess correctly rebuilds
Book 2's warehouse exactly. `verify_sources.py` proves that it does.

Five rules govern this file:

1.  **Books 1 and 2 are not touched.** It reads their warehouses (generating them first
    if they are missing) and writes to a different directory.

2.  **Deterministic.** Standard library only, seeded throughout, so two readers get
    byte-identical files and the book can print exact numbers.

3.  **Every source keeps time.** Each row version, file and event records when it came
    into being, so the simulators in `meridian/sources/` can show any source as it stood
    at any moment. A pipeline that "runs on 14 March 2025" sees what it would have seen.

4.  **The mess is deliberate.** Each problem is one real pipelines meet, listed in the
    manifest with the chapter that catches it. Do not "fix" one here.

5.  **Nothing is invented that the warehouse contradicts.** Extra rows (cancelled and
    test orders, contacts, comments, clicks) are things Book 2's warehouse rightly left
    out; every row it kept is here, recoverable to the cent.

    python3 generate_sources.py [--ml ../../data/meridian-ml] [--out ../../data/meridian-sources]

The systems, and where each lands in `--out`:

    tradeline/tradeline.db     the orders application's database, as it stands today
    tradeline/_history.db      every row version it ever held (for the simulator)
    relay/_store.db            the CRM's record versions and field history (for the simulator)
    frontline/_store.db        the support desk's tickets and their versions (served by an API)
    landing/relay/             nightly CRM exports (the last fourteen), the audit, the migration map
    landing/suppliers/         sixteen suppliers' monthly price lists
    landing/catalogue/         merchandising's product sheet
    landing/portal/            the customer portal's clickstream, hourly, from July 2025
    landing/ledger/            finance's monthly revenue journal
    reference/                 regions, procurement's supplier master, the price calendar
    truth/                     answer keys, read only by verification and named listings
    manifest.json              counts, dates, and the planted problems
"""

from __future__ import annotations

import argparse
import csv
import gzip
import hashlib
import io
import json
import os
import random
import shutil
import sqlite3
import subprocess
import sys
import zipfile
from datetime import date, datetime, time, timedelta, timezone
from zoneinfo import ZoneInfo

SEED_SOURCES = 20261001

UTC = timezone.utc
TODAY = date(2026, 1, 5)                            # the book's "today", a Monday
NOW = datetime(2026, 1, 5, 6, 0, tzinfo=UTC)        # every source is generated up to here
EPOCH = datetime(2021, 1, 1, tzinfo=UTC)            # "before anyone kept records"

EXPORTS_FROM = date(2022, 1, 1)     # the first nightly CRM export anyone kept
KEY_IMPORT = date(2023, 1, 1)       # key accounts moved into the CRM from their own desk
MIGRATION = date(2024, 3, 1)        # the old CRM was replaced by Relay overnight
PORTAL_FROM = date(2025, 7, 1)      # the new customer portal, and its event log
PORTAL_V2 = datetime(2025, 10, 1, tzinfo=UTC)        # web events change shape
PORTAL_APP_V2 = datetime(2025, 11, 10, tzinfo=UTC)   # the mobile app catches up
VOSS_RISE = date(2025, 1, 1)
END = date(2025, 12, 31)            # the last day of trading on record

TZ = {1: "America/New_York", 2: "America/New_York", 3: "America/New_York",
      4: "America/Los_Angeles", 5: "America/Chicago"}
REGION_CODE = {1: "NE", 2: "SE", 3: "MW", 4: "WE", 5: "SW"}
AREA_CODE = {1: "973", 2: "404", 3: "614", 4: "916", 5: "214"}
CHANNEL_CODE = {"Web": "WEB", "Field sales": "FLD", "Telesales": "TEL", "Partner": "PTR"}
TICKET_CHANNEL = {"Email": "email", "Web form": "web", "Phone": "voice"}

# Nights with no CRM export: a failed job, a holiday, and the night after the migration.
MISSING_NIGHTS = [date(2022, 8, 14), date(2023, 2, 5), date(2023, 11, 26), date(2024, 3, 2),
                  date(2024, 7, 4), date(2024, 12, 25), date(2025, 5, 18), date(2025, 9, 7)]
KEEP_NIGHTS = 14                    # the export folder keeps a fortnight; older nights are replayed

AMEND_RATE = 0.025                  # orders changed after they were placed
CANCEL_RATE = 0.012                 # extra orders placed and then cancelled
N_TEST_ORDERS = 38                  # created by staff to test the app, deleted next day
TEST_ACCOUNT = 2                    # Ardenwood Retail: a real key account
MIGRATION_MAP_KEEPS = 187           # of the 212 legacy ids, the rows IT's map remembers

AGENTS = ["Priya N", "Tom H", "Ada K", "Luis M", "Mei W", "Sam O", "Noor B", "Eli R"]

FIRST_NAMES = ["James", "Maria", "Robert", "Linda", "Michael", "Sarah", "David", "Karen",
               "Daniel", "Emily", "Chris", "Laura", "Kevin", "Megan", "Brian", "Rachel",
               "Jason", "Nicole", "Ryan", "Amanda", "Zoë", "José", "Siobhán", "Renée",
               "André", "Chloé", "Ramón", "Björn", "Inés", "Noël", "Mateo", "Aisha"]
LAST_NAMES = ["Smith", "Johnson", "Brennan", "Garcia", "Miller", "Davis", "Wilson",
              "Anderson", "Thomas", "Moore", "Martin", "Clark", "Lewis", "Walker", "Hall",
              "Young", "King", "Wright", "Scott", "Green", "Álvarez", "Núñez", "O'Brien",
              "Ó Briain", "Müller", "Lefèvre", "Søndergaard", "Nakamura", "Okafor", "Patel"]

# Four ways a supplier writes a price list.
LAYOUT = {"Pemberton Mills": "pemberton", "Kestrel Supply": "kestrel",
          "Aldridge & Co": "aldridge"}          # everyone else sends the default CSV
PEMBERTON_RENAME = date(2024, 7, 1)             # "Cost (USD)" becomes "Unit Cost USD"
ALDRIDGE_RESEND = date(2024, 3, 1)              # sent with a typo, then corrected
BRACKLEY_MISSING = date(2023, 8, 1)             # a list that never arrived
VOSS_LATE_ARRIVAL = datetime(2025, 1, 6, 15, 12, tzinfo=UTC)

# Merchandising's names for some suppliers, which procurement spells differently.
CATALOGUE_SUPPLIER = {"Pemberton Mills": "Pemberton", "Aldridge & Co": "Aldridge and Co",
                      "Kestrel Supply": "Kestrel", "Voss Industrial": "Voss Industrial "}

LEDGER_JOURNALS = [   # (period, region, amount, description): finance's own entries
    ("2024-09", 3, -18_500_00, "Manual adjustment: Halloway settlement credit"),
    ("2025-11", 5, -12_400_00, "Manual adjustment: Q3 volume rebate, key accounts"),
]


# --------------------------------------------------------------------------- time

def local_dt(d: date, t: time, region: int) -> datetime:
    """A wall-clock moment at the region's depot, as an aware datetime."""
    return datetime.combine(d, t, tzinfo=ZoneInfo(TZ[region]))


def to_utc(dt: datetime) -> datetime:
    return dt.astimezone(UTC)


def ts(dt: datetime) -> str:
    """The way Tradeline and the stores write a moment: UTC, to the second, no zone."""
    return dt.astimezone(UTC).strftime("%Y-%m-%d %H:%M:%S")


def iso_z(dt: datetime, ms: bool = False) -> str:
    """The way APIs and events write a moment: ISO 8601 with a Z."""
    dt = dt.astimezone(UTC)
    if ms:
        return dt.strftime("%Y-%m-%dT%H:%M:%S.") + f"{dt.microsecond // 1000:03d}Z"
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")


def business_moment(rng: random.Random, d: date, region: int, lo: int = 9, hi: int = 17) -> datetime:
    return to_utc(local_dt(d, time(rng.randint(lo, hi - 1), rng.randrange(60), rng.randrange(60)),
                           region))


def month_starts(first: date, last: date):
    m = date(first.year, first.month, 1)
    while m <= last:
        yield m
        m = date(m.year + (m.month == 12), m.month % 12 + 1, 1)


def nth_business_day(m: date, n: int) -> date:
    d, k = m, 0
    while True:
        if d.weekday() < 5:
            k += 1
            if k == n:
                return d
        d += timedelta(days=1)


WEB_HOURS = [1, 0.6, 0, 0, 0, 0.5, 1.5, 3, 5, 6, 6, 6, 5.5, 5.5, 6, 6, 5.5, 5, 4.5, 4,
             3.5, 3, 2.5, 1.5]       # no orders 02:00-04:59, which keeps clear of DST gaps
DESK_HOURS = [0] * 7 + [2, 5, 6, 6, 5, 4, 5, 6, 5, 4, 2] + [0] * 6


def order_moment(rng: random.Random, d: date, channel: str, region: int) -> datetime:
    weights = WEB_HOURS if channel == "Web" else DESK_HOURS
    hour = rng.choices(range(24), weights=weights)[0]
    return to_utc(local_dt(d, time(hour, rng.randrange(60), rng.randrange(60)), region))


# --------------------------------------------------------------------------- input

def ensure_upstream(ml_dir: str) -> None:
    """Book 2's warehouse, generated (with Book 1's) if it is missing."""
    here = os.path.dirname(os.path.abspath(__file__))
    db = os.path.join(ml_dir, "warehouse", "meridian_ml.db")
    if not os.path.exists(db):
        print("  Book 2's warehouse is missing; generating it first")
        subprocess.run([sys.executable, os.path.join(here, "generate_ml.py"), "--out", ml_dir],
                       check=True)


def read_book2(ml_dir: str) -> dict:
    db = os.path.join(ml_dir, "warehouse", "meridian_ml.db")
    con = sqlite3.connect(f"file:{db}?mode=ro", uri=True)
    q = lambda sql: con.execute(sql).fetchall()                          # noqa: E731
    out = {
        "regions": q("SELECT region_id, name, depot FROM regions ORDER BY region_id"),
        "suppliers": q("SELECT supplier_id, name, country, tier FROM suppliers ORDER BY supplier_id"),
        "products": q("SELECT sku, name, category, supplier_id, unit_cost, list_price, launched_on "
                      "FROM products ORDER BY sku"),
        "accounts": q("SELECT account_id, name, region_id, segment, account_manager, since, postcode,"
                      " is_key_account, crm_source, closed_on FROM accounts ORDER BY account_id"),
        "history": q("SELECT account_id, valid_from, valid_to, segment, account_manager "
                     "FROM account_history ORDER BY account_id, valid_from, rowid"),
        "contracts": q("SELECT contract_id, account_id, start_date, end_date, term_months, "
                       "legacy_terms, discount_pct, outcome, notice_date, cancellation_reason "
                       "FROM contracts ORDER BY contract_id"),
        "orders": q("SELECT order_id, account_id, order_date, channel FROM orders ORDER BY order_id"),
        "lines": q("SELECT order_id, line_no, sku, qty, unit_price, unit_cost, discount_pct "
                   "FROM order_lines ORDER BY order_id, line_no"),
        "tickets": q("SELECT ticket_id, account_id, opened_at, channel, sku, language, body, desk, "
                     "category, priority FROM tickets ORDER BY ticket_id"),
        "price_events": q("SELECT effective_on, scope, change_pct, note FROM price_events "
                          "ORDER BY effective_on"),
    }
    con.close()
    with open(db, "rb") as f:
        out["digest"] = hashlib.sha256(f.read()).hexdigest()
    return out


def cents(x: float) -> int:
    """Dollars to cents without the floating-point trap: 136.89 * 100 is 13688.999..."""
    return int(round(x * 100))


def normalise_name(s: str) -> str:
    s = " ".join(s.lower().split())
    for tail in (" (old)", " inc"):
        if s.endswith(tail):
            s = s[: -len(tail)]
    return s


def legacy_pairs(accounts: list) -> dict[int, int]:
    """Which legacy id is which account. Recovered from the way generate_ml.py wrote them."""
    main = {}
    for a in accounts:
        if a[8] != "Legacy CRM":
            main.setdefault((a[2], a[5], a[6], normalise_name(a[1])), []).append(a[0])
    pairs = {}
    for a in accounts:
        if a[8] == "Legacy CRM":
            match = main[(a[2], a[5], a[6], normalise_name(a[1]))]
            assert len(match) == 1, a
            pairs[a[0]] = match[0]
    return pairs


# --------------------------------------------------------------------------- Tradeline

TRADELINE_SCHEMA = """
CREATE TABLE channels (code TEXT PRIMARY KEY, label TEXT NOT NULL);
CREATE TABLE orders (
    id           INTEGER PRIMARY KEY,   -- the application's row id, in creation order
    order_number INTEGER NOT NULL UNIQUE,   -- the number printed on the invoice
    account_id   INTEGER NOT NULL,      -- the CRM's account id
    placed_at    TEXT NOT NULL,         -- UTC
    channel      TEXT NOT NULL REFERENCES channels(code),
    status       TEXT NOT NULL,         -- placed | cancelled
    created_at   TEXT NOT NULL,         -- UTC
    updated_at   TEXT NOT NULL          -- UTC; set when the row is written
);
CREATE TABLE order_lines (
    order_id         INTEGER NOT NULL REFERENCES orders(id),
    line_no          INTEGER NOT NULL,
    sku              TEXT NOT NULL,
    qty              INTEGER NOT NULL,
    unit_price_cents INTEGER NOT NULL,  -- the price charged per unit, after the discount
    discount_pct     INTEGER NOT NULL,
    created_at       TEXT NOT NULL,
    updated_at       TEXT NOT NULL,
    PRIMARY KEY (order_id, line_no)
);
CREATE INDEX idx_orders_updated ON orders(updated_at);
CREATE INDEX idx_orders_account ON orders(account_id);
CREATE INDEX idx_lines_updated ON order_lines(updated_at);
"""

HISTORY_SCHEMA = """
-- Moments are seconds since 1970 (UTC) here, to keep the file small; the simulator writes
-- them out as text, the way Tradeline does.
CREATE TABLE orders_v (lsn INTEGER PRIMARY KEY, committed_at INTEGER NOT NULL, op TEXT NOT NULL,
    id INTEGER NOT NULL, order_number INTEGER, account_id INTEGER, placed_at INTEGER,
    channel TEXT, status TEXT, created_at INTEGER, updated_at INTEGER);
CREATE TABLE order_lines_v (lsn INTEGER PRIMARY KEY, committed_at INTEGER NOT NULL,
    op TEXT NOT NULL, order_id INTEGER NOT NULL, line_no INTEGER NOT NULL, sku TEXT,
    qty INTEGER, unit_price_cents INTEGER, discount_pct INTEGER, created_at INTEGER,
    updated_at INTEGER);
CREATE INDEX idx_ov_commit ON orders_v(committed_at);
CREATE INDEX idx_lv_commit ON order_lines_v(committed_at);
CREATE TABLE amend_job_runs (night TEXT PRIMARY KEY, started_at TEXT, committed_at TEXT,
    rows_written INTEGER);
"""


def build_tradeline(rng: random.Random, b2: dict, region_of: dict) -> dict:
    """
    Every order as the application held it through time.

    Returns the row versions: each is (committed_at, seq, table, op, row). An insert commits
    the moment it is written. Amendments are applied by a nightly job that starts at 02:00
    UTC, stamps every row it touches with its start time, and commits when it finishes —
    minutes later. That gap is the watermark race of Chapter 7.
    """
    lines_by_order: dict[int, list] = {}
    for oid, line_no, sku, qty, price, cost, disc in b2["lines"]:
        lines_by_order.setdefault(oid, []).append((line_no, sku, qty, cents(price), disc))

    events = []          # (committed_at: datetime, seq, table, op, row: dict)
    seq = 0

    def emit(when, table, op, row):
        nonlocal seq
        seq += 1
        events.append((when, seq, table, op, row))

    job_rows: dict[date, list] = {}        # night -> [(order_key, change)]
    orders = []          # (created_at, order_number, account_id, placed_at, channel, final lines, kind, extra)

    # 1. Book 2's orders, each placed at a moment of its local day.
    for oid, account_id, order_date, channel in b2["orders"]:
        d = date.fromisoformat(order_date)
        placed = order_moment(rng, d, channel, region_of[account_id])
        final = lines_by_order[oid]
        initial = [list(x) for x in final]
        amend = None
        if rng.random() < AMEND_RATE:
            if len(final) >= 2 and rng.random() < 0.4:
                amend = ("add_line", final[-1])
                initial = initial[:-1]
            else:
                k = rng.randrange(len(final))
                delta = rng.choice([-3, -2, -1, 1, 2, 3, 5])
                if initial[k][2] + delta < 1:
                    delta = abs(delta)
                initial[k][2] += delta
                amend = ("qty", k)
        orders.append({"number": oid, "account": account_id, "placed": placed,
                       "channel": channel, "initial": [tuple(x) for x in initial],
                       "final": final, "amend": amend, "kind": "order"})

    # 2. Orders placed and then cancelled: real orders Book 2 rightly never counted.
    template_pool = [o for o in orders]
    next_key, next_tail = 66_677, 1_411_567
    n_cancel = int(len(orders) * CANCEL_RATE)
    for t in sorted(rng.sample(range(len(template_pool)), n_cancel)):
        src = template_pool[t]
        if src["number"] < 1_000_000:
            number, next_key = next_key, next_key + 1
        else:
            number, next_tail = next_tail, next_tail + 1
        region = region_of[src["account"]]
        day = src["placed"].astimezone(ZoneInfo(TZ[region])).date()
        placed = order_moment(rng, day, src["channel"], region)
        cancel_at = placed + timedelta(minutes=rng.randint(20, 50 * 60))
        if cancel_at >= NOW:
            cancel_at = NOW - timedelta(minutes=rng.randint(5, 60))
        orders.append({"number": number, "account": src["account"], "placed": placed,
                       "channel": src["channel"], "initial": list(src["final"]),
                       "final": list(src["final"]), "amend": None, "kind": "cancelled",
                       "cancel_at": cancel_at})

    # 3. Test orders, created by staff on a real account and deleted the next day.
    skus = sorted({p[0] for p in b2["products"] if p[2] != "Sanitation"})
    price_of = {p[0]: p[5] for p in b2["products"]}
    for k in range(N_TEST_ORDERS):
        d = date(2023, 1, 2) + timedelta(days=rng.randrange((END - date(2023, 1, 2)).days - 3))
        while d.weekday() >= 5:
            d += timedelta(days=1)
        placed = business_moment(rng, d, region_of[TEST_ACCOUNT])
        picks = rng.sample(skus, rng.randint(1, 2))
        lines = [(i + 1, s, rng.choice([1, 1, 2, 999]), cents(price_of[s]), 0)
                 for i, s in enumerate(picks)]
        number, next_key = next_key, next_key + 1
        orders.append({"number": number, "account": TEST_ACCOUNT, "placed": placed,
                       "channel": "Web", "initial": lines, "final": lines, "amend": None,
                       "kind": "test", "delete_at": placed + timedelta(hours=rng.randint(14, 40))})

    # Row ids are handed out in the order rows were created, as an application does.
    orders.sort(key=lambda o: (o["placed"], o["number"]))
    for i, o in enumerate(orders, start=1):
        o["id"] = i

    for o in orders:
        t0 = ts(o["placed"])
        head = {"id": o["id"], "order_number": o["number"], "account_id": o["account"],
                "placed_at": t0, "channel": CHANNEL_CODE[o["channel"]], "status": "placed",
                "created_at": t0, "updated_at": t0}
        emit(o["placed"], "orders", "I", dict(head))
        for line_no, sku, qty, pc, disc in o["initial"]:
            emit(o["placed"], "order_lines", "I",
                 {"order_id": o["id"], "line_no": line_no, "sku": sku, "qty": qty,
                  "unit_price_cents": pc, "discount_pct": disc, "created_at": t0, "updated_at": t0})
        o["head"] = head
        if o["kind"] == "cancelled":
            head = dict(head, status="cancelled", updated_at=ts(o["cancel_at"]))
            emit(o["cancel_at"], "orders", "U", head)
        elif o["kind"] == "test":
            emit(o["delete_at"], "orders", "D", {"id": o["id"]})
            for line_no, *_ in o["initial"]:
                emit(o["delete_at"], "order_lines", "D", {"order_id": o["id"], "line_no": line_no})
        elif o["amend"]:
            night = (o["placed"] + timedelta(hours=1)).date()
            start = datetime.combine(night, time(2, 0), tzinfo=UTC)
            if start < o["placed"] + timedelta(hours=1):
                night += timedelta(days=1)
            job_rows.setdefault(night, []).append(o)

    # The nightly amendment job: every row it writes carries its start time, and none of
    # them is visible until it commits, three to twelve minutes later.
    runs = []
    for night in sorted(job_rows):
        started = datetime.combine(night, time(2, 0), tzinfo=UTC)
        committed = started + timedelta(seconds=rng.randint(180, 720))
        stamp = ts(started)
        written = 0
        for o in job_rows[night]:
            emit(committed, "orders", "U", dict(o["head"], updated_at=stamp))
            kind, arg = o["amend"]
            if kind == "qty":
                line_no, sku, qty, pc, disc = o["final"][arg]
                row = {"order_id": o["id"], "line_no": line_no, "sku": sku, "qty": qty,
                       "unit_price_cents": pc, "discount_pct": disc,
                       "created_at": o["head"]["created_at"], "updated_at": stamp}
                emit(committed, "order_lines", "U", row)
            else:
                line_no, sku, qty, pc, disc = arg
                emit(committed, "order_lines", "I",
                     {"order_id": o["id"], "line_no": line_no, "sku": sku, "qty": qty,
                      "unit_price_cents": pc, "discount_pct": disc,
                      "created_at": stamp, "updated_at": stamp})
            written += 2
        runs.append((night.isoformat(), stamp, ts(committed), written))

    events = [e for e in events if e[0] <= NOW]
    events.sort(key=lambda e: (e[0], e[1]))
    return {"events": events, "orders": orders, "runs": runs}


def write_tradeline(out: str, tl: dict) -> dict:
    tdir = os.path.join(out, "tradeline")
    os.makedirs(tdir, exist_ok=True)
    hist_path = os.path.join(tdir, "_history.db")
    for p in (hist_path, os.path.join(tdir, "tradeline.db")):
        if os.path.exists(p):
            os.remove(p)

    hist = sqlite3.connect(hist_path)
    hist.executescript(HISTORY_SCHEMA)
    def secs(s):
        return None if s is None else int(datetime.fromisoformat(s).replace(tzinfo=UTC).timestamp())

    ov, lv = [], []
    for lsn, (when, _, table, op, row) in enumerate(tl["events"], start=1):
        c = int(when.timestamp())
        if table == "orders":
            ov.append((lsn, c, op, row["id"], row.get("order_number"), row.get("account_id"),
                       secs(row.get("placed_at")), row.get("channel"), row.get("status"),
                       secs(row.get("created_at")), secs(row.get("updated_at"))))
        else:
            lv.append((lsn, c, op, row["order_id"], row["line_no"], row.get("sku"), row.get("qty"),
                       row.get("unit_price_cents"), row.get("discount_pct"),
                       secs(row.get("created_at")), secs(row.get("updated_at"))))
    hist.executemany("INSERT INTO orders_v VALUES (?,?,?,?,?,?,?,?,?,?,?)", ov)
    hist.executemany("INSERT INTO order_lines_v VALUES (?,?,?,?,?,?,?,?,?,?,?)", lv)
    hist.executemany("INSERT INTO amend_job_runs VALUES (?,?,?,?)", tl["runs"])
    hist.commit()
    hist.close()

    # Today's database: the history replayed to NOW, by the same code the simulator uses.
    sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    from meridian.sources.tradeline import materialise
    materialise(hist_path, os.path.join(tdir, "tradeline.db"), NOW)
    return {"versions": len(ov) + len(lv)}


# --------------------------------------------------------------------------- Relay (CRM)

RELAY_SCHEMA = """
CREATE TABLE account_versions (account_id INTEGER, name TEXT, region_id INTEGER, segment TEXT,
    owner TEXT, customer_since TEXT, postcode TEXT, key_account INTEGER, status TEXT,
    closed_on TEXT, contact_name TEXT, contact_email TEXT, contact_phone TEXT,
    valid_from TEXT NOT NULL, valid_to TEXT);
CREATE INDEX idx_av ON account_versions(valid_from, valid_to);
CREATE TABLE contract_versions (contract_id INTEGER, account_id INTEGER, start_date TEXT,
    end_date TEXT, term_months INTEGER, legacy_terms INTEGER, discount_pct INTEGER,
    outcome TEXT, notice_date TEXT, cancellation_reason TEXT,
    valid_from TEXT NOT NULL, valid_to TEXT);
CREATE INDEX idx_cv ON contract_versions(valid_from, valid_to);
CREATE TABLE field_history (changed_at TEXT, object TEXT, record_id INTEGER, field TEXT,
    old_value TEXT, new_value TEXT, changed_by TEXT);
CREATE TABLE nights (night TEXT PRIMARY KEY, exported INTEGER, system TEXT, exported_at TEXT);
"""


def slug(name: str) -> str:
    keep = "".join(ch if ch.isalnum() else "-" for ch in name.lower())
    return "-".join(x for x in keep.split("-") if x)


def make_contact(rng: random.Random, account_name: str, region: int) -> tuple[str, str, str]:
    first, last = rng.choice(FIRST_NAMES), rng.choice(LAST_NAMES)
    ascii_first = first.translate(str.maketrans("ëéèíáóúñöüøÓÁ", "eeeiaounouoOA"))
    ascii_last = "".join(ch for ch in last.translate(
        str.maketrans("ëéèíáóúñöüøÓÁ", "eeeiaounouoOA")) if ch.isalpha()).lower()
    email = f"{ascii_first.lower()}.{ascii_last}@{slug(account_name)}.example"
    phone = f"({AREA_CODE[region]}) 555-01{rng.randrange(100):02d}"
    return f"{first} {last}", email, phone


def build_relay(rng: random.Random, b2: dict, pairs: dict) -> dict:
    """
    The CRM's record of every account and contract, version by version.

    A version is the state of a record from `valid_from` until `valid_to`. The nightly
    export at 23:00 in Newark writes out every record's version at that moment. The
    field history records each change to a tracked field from 1 January 2022, when the
    audit was switched on.
    """
    region_of = {a[0]: a[2] for a in b2["accounts"]}
    history: dict[int, list] = {}
    for acct, vf, vt, seg, mgr in b2["history"]:
        history.setdefault(acct, []).append((date.fromisoformat(vf), seg, mgr))
    audit_from = datetime.combine(EXPORTS_FROM, time(0), tzinfo=UTC)

    acct_versions, field_hist, contacts = [], [], {}
    main_accounts = [a for a in b2["accounts"] if a[8] != "Legacy CRM"]
    for a in main_accounts:
        acct, name, region, seg, mgr, since, postcode, key, _, closed_on = a
        since_d = date.fromisoformat(since)
        contacts[acct] = make_contact(rng, name, region)
        if key:
            created = to_utc(local_dt(KEY_IMPORT, time(0, 5), region))
        elif since_d >= EXPORTS_FROM:
            created = business_moment(rng, since_d, region, 9, 12)
        else:
            created = EPOCH
        state = {"account_id": acct, "name": name, "region_id": region,
                 "segment": history[acct][0][1], "owner": history[acct][0][2],
                 "customer_since": since, "postcode": postcode, "key_account": key,
                 "status": "active", "closed_on": None, "contact_name": contacts[acct][0],
                 "contact_email": contacts[acct][1], "contact_phone": contacts[acct][2]}
        changes = []     # (moment, {field: value})
        for i, (vf, s, m) in enumerate(history[acct][1:], start=1):
            # Two changes can share a day; the later one happens later in it.
            same_day = sum(1 for x in history[acct][1:i] if x[0] == vf)
            moment = to_utc(local_dt(vf, time(10 + 3 * same_day, rng.randrange(60)), region))
            changes.append((moment, {"segment": s, "owner": m}))
        # Two accounts changed segment after they were handed to the Retention desk, a
        # change Book 2's history lost and its accounts table kept. The CRM saw it happen,
        # on the first of the month after the handover.
        last_vf, last_seg, last_mgr = history[acct][-1]
        if (last_seg, last_mgr) != (seg, mgr):
            m = date(last_vf.year + (last_vf.month == 12), last_vf.month % 12 + 1, 1)
            changes.append((to_utc(local_dt(m, time(11, rng.randrange(60)), region)),
                            {"segment": seg, "owner": mgr}))
        if closed_on:
            cd = date.fromisoformat(closed_on)
            changes.append((to_utc(local_dt(cd, time(17, rng.randrange(60)), region)),
                            {"status": "closed", "closed_on": closed_on}))
        changes.sort(key=lambda c: c[0])
        start = created
        if created >= audit_from:
            field_hist.append((ts(created), "account", acct, "created", None, None, "import"
                               if key else rng.choice(AGENTS)))
        for moment, fields in changes:
            if moment <= start:           # a change older than the record's first version
                state.update(fields)
                continue
            acct_versions.append(dict(state, valid_from=ts(start), valid_to=ts(moment)))
            for f, v in fields.items():
                if state.get(f) != v and moment >= audit_from:
                    field_hist.append((ts(moment), "account", acct, f, state.get(f), v,
                                       state["owner"] if f != "owner" else "sales-ops"))
            state.update(fields)
            start = moment
        acct_versions.append(dict(state, valid_from=ts(start), valid_to=None))

    # The legacy duplicates: second records in the old CRM, frozen since they were made,
    # and gone the night Relay replaced it.
    first_version = {}
    for v in acct_versions:
        first_version.setdefault(v["account_id"], v)
    for legacy_id, main_id in sorted(pairs.items()):
        la = next(a for a in b2["accounts"] if a[0] == legacy_id)
        mv = first_version[main_id]
        region = la[2]
        cut = to_utc(local_dt(MIGRATION, time(0), region))
        acct_versions.append({"account_id": legacy_id, "name": la[1], "region_id": region,
                              "segment": la[3], "owner": la[4], "customer_since": la[5],
                              "postcode": la[6], "key_account": 0, "status": "active",
                              "closed_on": None, "contact_name": mv["contact_name"],
                              "contact_email": None, "contact_phone": mv["contact_phone"],
                              "valid_from": mv["valid_from"], "valid_to": ts(cut)})

    # Contracts: created when they start (or when their account arrived), closed out
    # the day they end, and given a reason for leaving after the exit interview.
    contract_versions = []
    key_of = {a[0]: a[7] for a in b2["accounts"]}
    for c in b2["contracts"]:
        cid, acct, start_d, end_d, term, legacy, disc, outcome, notice, reason = c
        region = region_of[acct]
        sd, ed = date.fromisoformat(start_d), date.fromisoformat(end_d)
        if key_of[acct] and sd < KEY_IMPORT:
            created = to_utc(local_dt(KEY_IMPORT, time(0, 5), region))
        elif sd >= EXPORTS_FROM:
            created = business_moment(rng, sd, region, 9, 11)
        else:
            created = EPOCH
        base = {"contract_id": cid, "account_id": acct, "start_date": start_d,
                "end_date": end_d, "term_months": term, "legacy_terms": legacy,
                "discount_pct": disc, "outcome": None, "notice_date": None,
                "cancellation_reason": None}
        if created >= audit_from:
            field_hist.append((ts(created), "contract", cid, "created", None, None,
                               "import" if key_of[acct] else rng.choice(AGENTS)))
        if outcome is None:
            contract_versions.append(dict(base, valid_from=ts(created), valid_to=None))
            continue
        closed = to_utc(local_dt(ed, time(16, rng.randrange(60)), region))
        contract_versions.append(dict(base, valid_from=ts(created), valid_to=ts(closed)))
        state = dict(base, outcome=outcome, notice_date=notice)
        for f in ("outcome", "notice_date"):
            if state[f] is not None and closed >= audit_from:
                field_hist.append((ts(closed), "contract", cid, f, None, state[f], "sales-ops"))
        if reason is None:
            contract_versions.append(dict(state, valid_from=ts(closed), valid_to=None))
            continue
        later = min(ed + timedelta(days=rng.randint(3, 40)), TODAY - timedelta(days=2))
        filled = to_utc(local_dt(later, time(15, rng.randrange(60)), region))
        contract_versions.append(dict(state, valid_from=ts(closed), valid_to=ts(filled)))
        contract_versions.append(dict(state, cancellation_reason=reason,
                                      valid_from=ts(filled), valid_to=None))
        field_hist.append((ts(filled), "contract", cid, "cancellation_reason", None, reason,
                           "retention-desk"))

    field_hist.sort(key=lambda r: (r[0], r[1], r[2], r[3]))
    nights = []
    d = EXPORTS_FROM
    while d <= TODAY - timedelta(days=1):
        at = to_utc(local_dt(d, time(23, 0), 1))
        nights.append((d.isoformat(), int(d not in MISSING_NIGHTS),
                       "legacy" if d < MIGRATION else "relay", ts(at)))
        d += timedelta(days=1)
    return {"accounts": acct_versions, "contracts": contract_versions,
            "field_history": field_hist, "nights": nights, "contacts": contacts}


def write_relay(out: str, relay: dict, pairs: dict, rng: random.Random) -> dict:
    rdir = os.path.join(out, "relay")
    os.makedirs(rdir, exist_ok=True)
    path = os.path.join(rdir, "_store.db")
    if os.path.exists(path):
        os.remove(path)
    con = sqlite3.connect(path)
    con.executescript(RELAY_SCHEMA)
    acols = ["account_id", "name", "region_id", "segment", "owner", "customer_since", "postcode",
             "key_account", "status", "closed_on", "contact_name", "contact_email",
             "contact_phone", "valid_from", "valid_to"]
    con.executemany(f"INSERT INTO account_versions VALUES ({','.join('?' * len(acols))})",
                    [tuple(v[c] for c in acols) for v in relay["accounts"]])
    ccols = ["contract_id", "account_id", "start_date", "end_date", "term_months",
             "legacy_terms", "discount_pct", "outcome", "notice_date", "cancellation_reason",
             "valid_from", "valid_to"]
    con.executemany(f"INSERT INTO contract_versions VALUES ({','.join('?' * len(ccols))})",
                    [tuple(v[c] for c in ccols) for v in relay["contracts"]])
    con.executemany("INSERT INTO field_history VALUES (?,?,?,?,?,?,?)", relay["field_history"])
    con.executemany("INSERT INTO nights VALUES (?,?,?,?)", relay["nights"])
    con.commit()
    con.close()

    # What has landed by today: the last fortnight of nightly exports, a month of audit
    # per file since the audit began, and the map IT made at the migration.
    from meridian.sources import relay as relay_sim
    land = os.path.join(out, "landing", "relay")
    if os.path.isdir(land):
        shutil.rmtree(land)
    os.makedirs(land)
    kept = [n for n in relay["nights"][-KEEP_NIGHTS:]]
    for night, exported, _, _ in kept:
        if exported:
            relay_sim.export(date.fromisoformat(night), land, store=path)
    relay_sim.write_audit(land, store=path)
    keep = sorted(rng.sample(sorted(pairs), MIGRATION_MAP_KEEPS))
    mp = os.path.join(land, "migration_map.csv")
    with open(mp, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f, lineterminator="\r\n")
        w.writerow(["old_crm_id", "relay_id", "matched_by"])
        for lid in keep:
            w.writerow([lid, pairs[lid], "name+postcode"])
    stamp(mp, datetime(2024, 3, 1, 14, 30, tzinfo=UTC))
    return {"exports_on_disk": sum(1 for n in kept if n[1])}


# --------------------------------------------------------------------------- Frontline

FRONTLINE_SCHEMA = """
CREATE TABLE tickets (ticket_id TEXT PRIMARY KEY, created_at TEXT NOT NULL,
    requester_account_id INTEGER, requester_name TEXT, requester_email TEXT,
    channel TEXT, language TEXT, description TEXT, sku TEXT);
CREATE TABLE ticket_versions (ticket_id TEXT, updated_at TEXT, status TEXT, priority TEXT,
    category TEXT, desk TEXT, assignee TEXT, PRIMARY KEY (ticket_id, updated_at));
CREATE INDEX idx_tv_updated ON ticket_versions(updated_at);
CREATE TABLE comments (comment_id INTEGER PRIMARY KEY, ticket_id TEXT, created_at TEXT,
    author TEXT, public INTEGER, body TEXT);
CREATE INDEX idx_comments_ticket ON comments(ticket_id);
"""

PRIORITIES = ["low", "normal", "high", "urgent"]
CATEGORIES = ["account", "billing", "delivery", "quality", "returns", "stock", "other"]
FOLLOW_UPS = ["Any update on this?", "Still waiting to hear back.", "Thanks, that worked.",
              "Please call me when you can.", "Chasing this again.", "Received, thank you."]


def build_frontline(rng: random.Random, b2: dict, contacts: dict, pairs: dict) -> dict:
    region_of = {a[0]: a[2] for a in b2["accounts"]}
    tickets, versions, comments = [], [], []
    cid = 0
    for tid, acct, opened_at, channel, sku, lang, body, desk, cat, pri in b2["tickets"]:
        region = region_of[acct]
        opened = datetime.fromisoformat(opened_at)
        created = to_utc(local_dt(opened.date(), opened.time(), region))
        who = contacts[pairs.get(acct, acct)]
        tickets.append((tid, ts(created), acct, who[0], who[1], TICKET_CHANNEL[channel], lang,
                        body, sku))
        final_p, final_c = pri.lower(), cat.lower()
        desk_v = desk.lower().replace(" ", "_")
        p0, c0 = final_p, final_c
        t_open = created + timedelta(minutes=rng.randint(4, 180))
        t_solved = created + timedelta(minutes=rng.randint(180, 8 * 24 * 60))
        t_closed = t_solved + timedelta(hours=96)
        t_pri = created + timedelta(minutes=rng.randint(30, 26 * 60))
        t_cat = created + timedelta(minutes=rng.randint(60, 30 * 60))
        change_p = rng.random() < 0.08 and t_pri < NOW
        change_c = rng.random() < 0.06 and t_cat < NOW
        if change_p:
            i = PRIORITIES.index(final_p)
            p0 = PRIORITIES[i - 1] if i > 0 and rng.random() < 0.7 else PRIORITIES[min(i + 1, 3)]
            if p0 == final_p:
                change_p = False
        if change_c:
            c0 = rng.choice([c for c in CATEGORIES if c != final_c])
        agent = rng.choice(AGENTS)
        steps = [(created, {"status": "new", "priority": p0, "category": c0, "assignee": None}),
                 (t_open, {"status": "open", "assignee": agent})]
        if change_p:
            steps.append((t_pri, {"priority": final_p}))
        if change_c:
            steps.append((t_cat, {"category": final_c}))
        steps += [(t_solved, {"status": "solved"}), (t_closed, {"status": "closed"})]
        steps.sort(key=lambda s: s[0])
        state = {"status": None, "priority": None, "category": None, "desk": desk_v,
                 "assignee": None}
        last = None
        for when, fields in steps:
            if when > NOW:
                break
            state.update(fields)
            key = ts(when)
            if key == last:                       # two changes in one second: keep the later
                versions[-1] = (tid, key, state["status"], state["priority"],
                                state["category"], state["desk"], state["assignee"])
            else:
                versions.append((tid, key, state["status"], state["priority"],
                                 state["category"], state["desk"], state["assignee"]))
            last = key
        assert state["priority"] == final_p and state["category"] == final_c, tid
        if rng.random() < 0.25:
            when = created + timedelta(minutes=rng.randint(30, 5 * 24 * 60))
            if when < NOW:
                cid += 1
                text = rng.choice(FOLLOW_UPS)
                if rng.random() < 0.16:
                    text += rng.choice([f" You can reach me on {who[2]}.",
                                        f" Best to email me at {who[1]}.",
                                        f" My direct line is {who[2]}, ask for {who[0].split()[0]}."])
                comments.append((cid, tid, ts(when), who[0], 1, text))
    return {"tickets": tickets, "versions": versions, "comments": comments}


def write_frontline(out: str, fl: dict) -> None:
    fdir = os.path.join(out, "frontline")
    os.makedirs(fdir, exist_ok=True)
    path = os.path.join(fdir, "_store.db")
    if os.path.exists(path):
        os.remove(path)
    con = sqlite3.connect(path)
    con.executescript(FRONTLINE_SCHEMA)
    con.executemany("INSERT INTO tickets VALUES (?,?,?,?,?,?,?,?,?)", fl["tickets"])
    con.executemany("INSERT INTO ticket_versions VALUES (?,?,?,?,?,?,?)", fl["versions"])
    con.executemany("INSERT INTO comments VALUES (?,?,?,?,?,?)", fl["comments"])
    con.commit()
    con.close()


# --------------------------------------------------------------------------- files

def stamp(path: str, when: datetime) -> None:
    """A landed file's modification time is when it arrived. Discovery depends on it."""
    t = when.timestamp()
    os.utime(path, (t, t))


def write_csv(path: str, header: list, rows: list, delimiter: str = ",",
              encoding: str = "utf-8", lineterminator: str = "\n") -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", newline="", encoding=encoding) as f:
        w = csv.writer(f, delimiter=delimiter, lineterminator=lineterminator)
        w.writerow(header)
        w.writerows(rows)


def xlsx_bytes(rows: list[list], sheet: str) -> bytes:
    """A minimal, valid .xlsx: one sheet, inline strings, no styles. Deterministic."""
    def col(i):
        s = ""
        i += 1
        while i:
            i, r = divmod(i - 1, 26)
            s = chr(65 + r) + s
        return s

    def esc(s):
        return (str(s).replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;"))

    xml_rows = []
    for r, row in enumerate(rows, start=1):
        cells = []
        for c, v in enumerate(row):
            ref = f"{col(c)}{r}"
            if v is None or v == "":
                continue
            if isinstance(v, (int, float)) and not isinstance(v, bool):
                cells.append(f'<c r="{ref}"><v>{v}</v></c>')
            else:
                cells.append(f'<c r="{ref}" t="inlineStr"><is><t>{esc(v)}</t></is></c>')
        xml_rows.append(f'<row r="{r}">{"".join(cells)}</row>')
    files = {
        "[Content_Types].xml":
            '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
            '<Types xmlns="http://schemas.openxmlformats.org/package/2006/content-types">'
            '<Default Extension="rels" ContentType="application/vnd.openxmlformats-package.relationships+xml"/>'
            '<Default Extension="xml" ContentType="application/xml"/>'
            '<Override PartName="/xl/workbook.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet.main+xml"/>'
            '<Override PartName="/xl/worksheets/sheet1.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.worksheet+xml"/>'
            '</Types>',
        "_rels/.rels":
            '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
            '<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">'
            '<Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/officeDocument" Target="xl/workbook.xml"/>'
            '</Relationships>',
        "xl/workbook.xml":
            '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
            '<workbook xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main" '
            'xmlns:r="http://schemas.openxmlformats.org/officeDocument/2006/relationships">'
            f'<sheets><sheet name="{esc(sheet)}" sheetId="1" r:id="rId1"/></sheets></workbook>',
        "xl/_rels/workbook.xml.rels":
            '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
            '<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">'
            '<Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/worksheet" Target="worksheets/sheet1.xml"/>'
            '</Relationships>',
        "xl/worksheets/sheet1.xml":
            '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
            '<worksheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main">'
            f'<sheetData>{"".join(xml_rows)}</sheetData></worksheet>',
    }
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as z:
        for name, text in files.items():
            info = zipfile.ZipInfo(name, date_time=(2020, 1, 1, 0, 0, 0))
            info.compress_type = zipfile.ZIP_DEFLATED
            z.writestr(info, text)
    return buf.getvalue()


def write_price_lists(rng: random.Random, out: str, b2: dict) -> dict:
    """
    Every supplier's monthly price list, in its own layout, landed when it arrived.
    Costs come from Book 2's order lines, so a correct as-of join reproduces them.
    """
    root = os.path.join(out, "landing", "suppliers")
    if os.path.isdir(root):
        shutil.rmtree(root)
    suppliers = {s[0]: s[1] for s in b2["suppliers"]}
    products = b2["products"]
    voss = next(i for i, n in suppliers.items() if n == "Voss Industrial")
    files, arrivals = 0, []
    for sid in sorted({p[3] for p in products}):
        name = suppliers[sid]
        sdir = os.path.join(root, slug(name))
        os.makedirs(sdir, exist_ok=True)
        layout = LAYOUT.get(name, "default")
        for m in month_starts(EXPORTS_FROM, END):
            if name == "Brackley Plastics" and m == BRACKLEY_MISSING:
                continue
            items = []
            for sku, pname, cat, psid, cost, _, launched in products:
                if psid != sid or (launched and date.fromisoformat(launched) > m):
                    continue
                c = round(cost * 1.12, 2) if sid == voss and m >= VOSS_RISE else cost
                items.append((sku, pname, c))
            if not items:
                continue
            arrived = business_moment(rng, m + timedelta(days=rng.choice([0, 0, 0, 1, 1, 2])),
                                      1, 8, 17)
            if sid == voss and m == VOSS_RISE:
                arrived = VOSS_LATE_ARRIVAL
            versions = [(items, arrived, "")]
            if layout == "aldridge" and m == ALDRIDGE_RESEND:
                typo = [(s, n, round(c * 10, 2) if k == 0 else c) for k, (s, n, c) in enumerate(items)]
                versions = [(typo, arrived, ""),
                            (items, arrived + timedelta(days=3, hours=2), " (corrected)")]
            for its, when, suffix in versions:
                path = write_one_list(sdir, layout, name, m, its, suffix)
                stamp(path, when)
                arrivals.append((os.path.relpath(path, out), ts(when)))
                files += 1
    return {"files": files, "arrivals": arrivals}


def write_one_list(sdir: str, layout: str, name: str, m: date, items: list, suffix: str) -> str:
    if layout == "pemberton":
        cost_col = "Unit Cost USD" if m >= PEMBERTON_RENAME else "Cost (USD)"
        path = os.path.join(sdir, f"PEMBERTON-PRICES-{m:%Y%m}.csv")
        write_csv(path, ["Item Code", "Item", cost_col, "Valid From"],
                  [(s, n, f"{c:.2f}", f"{m:%d/%m/%Y}") for s, n, c in items],
                  lineterminator="\r\n")
    elif layout == "kestrel":
        path = os.path.join(sdir, f"kestrel_{m:%Y%m%d}.csv")
        write_csv(path, ["sku", "cost_cents", "effective"],
                  [(s, cents(c), m.isoformat()) for s, n, c in items])
    elif layout == "aldridge":
        path = os.path.join(sdir, f"Aldridge Trade Prices {m:%b %Y}{suffix}.xlsx")
        rows = [["Aldridge & Co — Trade Price List"],
                [f"Prices valid from {m.day} {m:%B %Y}"], [],
                ["Product Code", "Description", "Trade Price"]]
        rows += [[s, n, c] for s, n, c in items]
        with open(path, "wb") as f:
            f.write(xlsx_bytes(rows, "Trade prices"))
    else:
        path = os.path.join(sdir, f"{slug(name)}_{m:%Y-%m}.csv")
        write_csv(path, ["sku", "description", "unit_cost", "currency", "effective_from"],
                  [(s, n, f"{c:.2f}", "USD", m.isoformat()) for s, n, c in items])
    return path


def write_catalogue(out: str, b2: dict) -> None:
    suppliers = {s[0]: s[1] for s in b2["suppliers"]}
    rows = []
    for i, (sku, name, cat, sid, cost, price, launched) in enumerate(b2["products"]):
        shown = sku
        if sku in ("MRD-SAN-043", "MRD-FAC-031"):
            shown = sku + " "                      # typed with a trailing space
        if sku == "MRD-PAC-022":
            shown = sku.lower()
        sname = suppliers[sid]
        rows.append((shown, name, cat, CATALOGUE_SUPPLIER.get(sname, sname), f"${price:,.2f}",
                     date.fromisoformat(launched).strftime("%d-%b-%Y") if launched else "",
                     "New range" if launched else ""))
    path = os.path.join(out, "landing", "catalogue", "catalogue.csv")
    write_csv(path, ["SKU", "Product Name", "Category", "Supplier", "List Price", "Launched",
                     "Notes"], rows, encoding="utf-8")
    stamp(path, datetime(2025, 11, 14, 16, 41, tzinfo=UTC))


def write_reference(out: str, b2: dict) -> None:
    rdir = os.path.join(out, "reference")
    write_csv(os.path.join(rdir, "regions.csv"), ["region_id", "name", "depot", "timezone"],
              [(r, n, d, TZ[r]) for r, n, d in b2["regions"]])
    write_csv(os.path.join(rdir, "suppliers.csv"), ["supplier_id", "name", "country", "tier"],
              b2["suppliers"])
    write_csv(os.path.join(rdir, "price_events.csv"), ["effective_on", "scope", "change_pct", "note"],
              b2["price_events"])


# --------------------------------------------------------------------------- Ledger

def order_local_date(o, region_of) -> date:
    return o["placed"].astimezone(ZoneInfo(TZ[region_of[o["account"]]])).date()


def write_ledger(out: str, tl: dict, region_of: dict) -> dict:
    """
    Finance's monthly revenue journal: what was invoiced, by region, for orders that were
    not cancelled, dated by the depot's calendar. Plus the journals finance makes itself.
    A month's file lands when the month closes, on its fifth working day.
    """
    by = {}
    for o in tl["orders"]:
        if o["kind"] != "order":
            continue
        d = order_local_date(o, region_of)
        key = (f"{d:%Y-%m}", region_of[o["account"]])
        by[key] = by.get(key, 0) + sum(qty * pc for _, _, qty, pc, _ in o["final"])
    root = os.path.join(out, "landing", "ledger")
    if os.path.isdir(root):
        shutil.rmtree(root)
    files = 0
    for m in month_starts(EXPORTS_FROM, END):
        period = f"{m:%Y-%m}"
        nxt = date(m.year + (m.month == 12), m.month % 12 + 1, 1)
        closes = nth_business_day(nxt, 5)
        if closes > TODAY:
            continue
        rows = []
        for r in sorted(REGION_CODE):
            amount = by.get((period, r), 0)
            rows.append((period, "MERIDIAN-US", REGION_CODE[r], 4000, "Product sales",
                         "Sales journal", f"{amount / 100:.2f}"))
        for p, r, amount, desc in LEDGER_JOURNALS:
            if p == period:
                rows.append((period, "MERIDIAN-US", REGION_CODE[r], 4000, "Product sales", desc,
                             f"{amount / 100:.2f}"))
        path = os.path.join(root, f"gl_revenue_{period}.csv")
        write_csv(path, ["period", "entity", "region", "gl_account", "gl_name", "description",
                         "amount"], rows)
        stamp(path, datetime.combine(closes, time(22, 0), tzinfo=UTC))
        files += 1
    return {"files": files}


# --------------------------------------------------------------------------- Portal

def uuid_from(rng: random.Random) -> str:
    h = f"{rng.getrandbits(128):032x}"
    return f"{h[:8]}-{h[8:12]}-4{h[13:16]}-{'89ab'[int(h[16], 16) % 4]}{h[17:20]}-{h[20:32]}"


def write_portal(rng: random.Random, out: str, tl: dict, region_of: dict, contacts: dict,
                 pairs: dict, b2: dict) -> dict:
    """
    The customer portal's clickstream from its launch on 1 July 2025: every web order is a
    session that ends in `order_submitted`, and more sessions end in nothing. Written in
    hourly files by the hour the server received each event, which is not always the hour
    it happened.
    """
    catalogue = [p[0] for p in b2["products"]]
    launch = datetime.combine(PORTAL_FROM, time(0), tzinfo=ZoneInfo("America/New_York")).astimezone(UTC)
    web = [o for o in tl["orders"] if o["channel"] == "Web" and o["kind"] != "test"
           and o["placed"] >= launch]
    active = sorted({o["account"] for o in web})
    sessions = []
    for o in web:
        sessions.append(("order", o["placed"], o["account"], o))
    n_browse = int(len(web) * 1.3)
    span = int((NOW - launch - timedelta(days=5)).total_seconds())
    for _ in range(n_browse):
        at = launch + timedelta(seconds=rng.randrange(span))
        sessions.append(("browse", at, rng.choice(active), None))
    sessions.sort(key=lambda s: (s[1], s[2]))

    events = []          # (received_at, occurred_at_text, dict)
    skewed_sessions = dupes = late = 0
    for kind, end_at, acct, order in sessions:
        mobile = rng.random() < 0.3
        device = "mobile" if mobile else "desktop"
        ua = ("MeridianApp/3.4 (iOS 18.2)" if rng.random() < 0.5 else "MeridianApp/3.4 (Android 15)") \
            if mobile else rng.choice(["Mozilla/5.0 (Windows NT 10.0) Chrome/131.0",
                                       "Mozilla/5.0 (Macintosh) Safari/18.2",
                                       "Mozilla/5.0 (Windows NT 10.0) Edg/131.0"])
        skew = timedelta(0)
        if mobile and rng.random() < 0.02:
            skew = timedelta(hours=rng.choice([-1, 1]) * rng.randint(1, 26), minutes=rng.randint(0, 59))
            skewed_sessions += 1
        person = pairs.get(acct, acct)
        user = f"u-{person:06d}-1"
        sid = f"s-{rng.getrandbits(48):012x}"
        steps = []
        if kind == "order":
            skus = [line[1] for line in order["final"]]
            n = 1 + rng.randint(0, 3) + len(skus) + 2 + rng.randint(1, 4)
            start = end_at - timedelta(seconds=rng.randint(90, 40 * 60))
            steps.append(("session_start", {}))
            for _ in range(rng.randint(0, 3)):
                steps.append(("search", {"query": rng.choice(["gloves", "cloth", "tape", "bin bags",
                                                              "sanitiser", "mop", "labels", "boxes"])}))
            for s in rng.sample(catalogue, rng.randint(0, 3)) + skus:
                steps.append(("product_view", {"sku": s}))
            value = 0
            for line in order["final"]:
                value += line[2] * line[3]
                steps.append(("add_to_cart", {"sku": line[1], "qty": line[2],
                                              "value_cents": line[2] * line[3]}))
            steps.append(("checkout_started", {"value_cents": value}))
            steps.append(("order_submitted", {"order_number": order["number"],
                                              "line_count": len(order["final"]),
                                              "value_cents": value}))
            del n
        else:
            start = end_at
            steps.append(("session_start", {}))
            for _ in range(rng.randint(0, 2)):
                steps.append(("search", {"query": rng.choice(["gloves", "cloth", "tape", "labels"])}))
            for s in rng.sample(catalogue, rng.randint(1, 4)):
                steps.append(("product_view", {"sku": s}))
            if rng.random() < 0.3:
                s = rng.choice(catalogue)
                steps.append(("add_to_cart", {"sku": s, "qty": rng.randint(1, 6), "value_cents": None}))
                if rng.random() < 0.3:
                    steps.append(("checkout_started", {"value_cents": None}))
            end_at = start + timedelta(seconds=rng.randint(30, 20 * 60))
        k = len(steps)
        total = max(1, int((end_at - start).total_seconds()))
        moments = sorted(start + timedelta(seconds=total * i / max(1, k - 1)) for i in range(k))
        moments[-1] = end_at
        for (etype, props), at in zip(steps, moments):
            at = at + timedelta(milliseconds=rng.randrange(1000)) if etype != "order_submitted" else at
            v2 = at >= PORTAL_V2 and (not mobile or at >= PORTAL_APP_V2)
            received = at + timedelta(milliseconds=rng.randint(150, 2500))
            if mobile and rng.random() < 0.03:
                received = at + timedelta(minutes=rng.randint(60, 72 * 60))
                late += 1
            p = dict(props)
            if "sku" in p and v2:
                p["product_sku"] = p.pop("sku")
            if not v2:
                p.pop("value_cents", None)
            elif "value_cents" in p:
                p["cart_value_cents"] = p.pop("value_cents")
                if p["cart_value_cents"] is None:
                    del p["cart_value_cents"]
            ev = {"event_id": uuid_from(rng), "event_type": etype, "schema_version": 2 if v2 else 1,
                  "occurred_at": iso_z(at + skew, ms=True), "session_id": sid,
                  "account_id": acct, "user_id": user, "device": device, "user_agent": ua,
                  "properties": p}
            if received > NOW:
                continue
            events.append((received, ev))
            if rng.random() < 0.012:
                events.append((received + timedelta(seconds=rng.randint(1, 300)), dict(ev)))
                dupes += 1

    # A price-scraping bot, logged in with a real customer's password.
    bot_acct = active[len(active) // 3]
    bot_events = 0
    for day in (date(2025, 11, 17), date(2025, 11, 18), date(2025, 11, 19)):
        sid = f"s-{rng.getrandbits(48):012x}"
        at = datetime.combine(day, time(3, 10), tzinfo=UTC)
        for i in range(4000):
            at += timedelta(milliseconds=rng.randint(400, 900))
            ev = {"event_id": uuid_from(rng), "event_type": "product_view", "schema_version": 2,
                  "occurred_at": iso_z(at, ms=True), "session_id": sid, "account_id": bot_acct,
                  "user_id": f"u-{bot_acct:06d}-1", "device": "desktop",
                  "user_agent": "PriceWatchBot/2.1 (+https://pricewatch.example/bot)",
                  "properties": {"product_sku": catalogue[i % len(catalogue)]}}
            events.append((at + timedelta(milliseconds=rng.randint(40, 200)), ev))
            bot_events += 1

    events.sort(key=lambda e: (e[0], e[1]["event_id"]))
    root = os.path.join(out, "landing", "portal")
    if os.path.isdir(root):
        shutil.rmtree(root)
    by_hour: dict[datetime, list] = {}
    for received, ev in events:
        ev = dict(ev, received_at=iso_z(received, ms=True))
        by_hour.setdefault(received.replace(minute=0, second=0, microsecond=0), []).append(ev)
    for hour, evs in sorted(by_hour.items()):
        path = os.path.join(root, f"{hour:%Y/%m/%d/%H}.jsonl.gz")
        os.makedirs(os.path.dirname(path), exist_ok=True)
        body = "".join(json.dumps(e, ensure_ascii=False, separators=(",", ":")) + "\n" for e in evs)
        with open(path, "wb") as raw:
            with gzip.GzipFile(filename="", mode="wb", fileobj=raw, mtime=0) as gz:
                gz.write(body.encode("utf-8"))
        stamp(path, hour + timedelta(hours=1, minutes=5))
    return {"events": len(events), "files": len(by_hour), "sessions": len(sessions) + 3,
            "web_orders": len(web), "duplicates": dupes, "late": late,
            "skewed_sessions": skewed_sessions, "bot_events": bot_events,
            "bot_account": bot_acct}


# --------------------------------------------------------------------------- truth

def write_truth(out: str, pairs: dict, tl: dict) -> None:
    tdir = os.path.join(out, "truth")
    os.makedirs(tdir, exist_ok=True)
    write_csv(os.path.join(tdir, "legacy_map.csv"), ["legacy_id", "account_id"],
              sorted(pairs.items()))
    write_csv(os.path.join(tdir, "orders_kind.csv"), ["order_number", "kind", "amended"],
              sorted((o["number"], o["kind"], o["amend"][0] if o["amend"] else "")
                     for o in tl["orders"]))


# --------------------------------------------------------------------------- main

def main() -> None:
    here = os.path.dirname(os.path.abspath(__file__))
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--ml", default=os.path.join(here, "..", "..", "data", "meridian-ml"))
    ap.add_argument("--out", default=os.path.join(here, "..", "..", "data", "meridian-sources"))
    args = ap.parse_args()
    out = os.path.abspath(args.out)
    os.makedirs(out, exist_ok=True)
    sys.path.insert(0, os.path.dirname(here))

    ensure_upstream(os.path.abspath(args.ml))
    b2 = read_book2(os.path.abspath(args.ml))
    rng = random.Random(SEED_SOURCES)
    region_of = {a[0]: a[2] for a in b2["accounts"]}
    pairs = legacy_pairs(b2["accounts"])
    print(f"Reading Book 2's warehouse: {len(b2['orders']):,} orders, "
          f"{len(b2['accounts']):,} account records, {len(b2['tickets']):,} tickets")

    tl = build_tradeline(random.Random(rng.random()), b2, region_of)
    info_tl = write_tradeline(out, tl)
    kinds = {k: sum(1 for o in tl["orders"] if o["kind"] == k) for k in ("order", "cancelled", "test")}
    amended = sum(1 for o in tl["orders"] if o["amend"])
    print(f"  tradeline   {sum(kinds.values()):,} orders ({kinds['cancelled']:,} cancelled, "
          f"{kinds['test']} test orders deleted, {amended:,} amended); "
          f"{info_tl['versions']:,} row versions")

    relay = build_relay(random.Random(rng.random()), b2, pairs)
    info_relay = write_relay(out, relay, pairs, random.Random(rng.random()))
    print(f"  relay       {len(relay['accounts']):,} account versions, "
          f"{len(relay['contracts']):,} contract versions, {len(relay['field_history']):,} "
          f"audited changes; {info_relay['exports_on_disk']} nightly exports landed")

    fl = build_frontline(random.Random(rng.random()), b2, relay["contacts"], pairs)
    write_frontline(out, fl)
    print(f"  frontline   {len(fl['tickets']):,} tickets, {len(fl['versions']):,} versions, "
          f"{len(fl['comments']):,} comments")

    info_prices = write_price_lists(random.Random(rng.random()), out, b2)
    write_catalogue(out, b2)
    write_reference(out, b2)
    info_ledger = write_ledger(out, tl, region_of)
    print(f"  files       {info_prices['files']} supplier price lists, the catalogue, "
          f"{info_ledger['files']} ledger months")

    info_portal = write_portal(random.Random(rng.random()), out, tl, region_of,
                               relay["contacts"], pairs, b2)
    print(f"  portal      {info_portal['events']:,} events in {info_portal['files']:,} hourly "
          f"files ({info_portal['web_orders']:,} web orders)")
    write_truth(out, pairs, tl)

    manifest = {
        "name": "Meridian Supply Co., its operational sources",
        "seed": SEED_SOURCES, "generated_by": "code/meridian/generate_sources.py",
        "synthetic": True, "contains_personal_data": False,
        "note": "Contact names, emails and phone numbers are invented; phones use the "
                "fictional 555-01xx range and emails the reserved .example domain.",
        "book2_warehouse_sha256": b2["digest"],
        "today": TODAY.isoformat(), "now_utc": iso_z(NOW),
        "dates": {"exports_from": EXPORTS_FROM.isoformat(), "key_accounts_imported":
                  KEY_IMPORT.isoformat(), "crm_migration": MIGRATION.isoformat(),
                  "portal_launch": PORTAL_FROM.isoformat(),
                  "portal_schema_v2": iso_z(PORTAL_V2), "portal_app_v2": iso_z(PORTAL_APP_V2),
                  "pemberton_rename": PEMBERTON_RENAME.isoformat(),
                  "voss_price_list_arrived": iso_z(VOSS_LATE_ARRIVAL)},
        "depots": {str(r): TZ[r] for r in TZ},
        "counts": {
            "tradeline_orders": sum(kinds.values()), "cancelled_orders": kinds["cancelled"],
            "test_orders_deleted": kinds["test"], "amended_orders": amended,
            "amend_job_nights": len(tl["runs"]),
            "relay_accounts": len({v["account_id"] for v in relay["accounts"]}),
            "legacy_ids": len(pairs), "migration_map_rows": MIGRATION_MAP_KEEPS,
            "missing_nights": len(MISSING_NIGHTS),
            "tickets": len(fl["tickets"]), "ticket_comments": len(fl["comments"]),
            "price_list_files": info_prices["files"], "ledger_months": info_ledger["files"],
            "portal_events": info_portal["events"], "portal_files": info_portal["files"],
            "portal_duplicates": info_portal["duplicates"], "portal_late": info_portal["late"],
            "portal_skewed_sessions": info_portal["skewed_sessions"],
            "portal_bot_events": info_portal["bot_events"],
        },
        "missing_nights": [d.isoformat() for d in MISSING_NIGHTS],
        "ledger_journals": [{"period": p, "region": REGION_CODE[r], "amount": a / 100,
                             "description": d} for p, r, a, d in LEDGER_JOURNALS],
        "problems": {
            "cancelled_orders": "Tradeline keeps cancelled orders, status 'cancelled' (Ch 3, 13)",
            "amendments": "orders amended by the nightly job keep created_at; only updated_at moves (Ch 7)",
            "watermark_race": "the amendment job stamps rows at 02:00:00 and commits minutes later (Ch 7)",
            "hard_deletes": "test orders on account 2 are deleted the next day (Ch 7)",
            "utc_dates": "placed_at is UTC; an evening order's UTC date is the next day (Ch 4, 13, 15)",
            "cents": "Tradeline stores unit_price_cents, the price after discount (Ch 3, 5, 15)",
            "offset_paging": "Frontline tickets change while you page (Ch 8)",
            "crm_migration_format": "exports before 2024-03-01: ';', cp1252, MM/DD/YYYY, other headers (Ch 5, 9, 10)",
            "lost_leading_zeros": "the old CRM wrote postcodes as numbers (Ch 5)",
            "missing_nights": "eight nights have no export (Ch 9, 12)",
            "pemberton_rename": "Pemberton's cost column is renamed from July 2024 (Ch 10)",
            "kestrel_cents": "Kestrel sends costs in cents (Ch 10)",
            "aldridge_banner": "Aldridge sends a spreadsheet with a banner; the date is in the banner (Ch 9)",
            "aldridge_resend": "Aldridge's March 2024 list was resent corrected (Ch 9)",
            "voss_late_list": "Voss's January 2025 list, effective the 1st, arrived on the 6th (Ch 13)",
            "brackley_missing_list": "Brackley Plastics' August 2023 list never arrived (Ch 9)",
            "catalogue_names": "the catalogue spells four suppliers differently from procurement (Ch 11)",
            "catalogue_skus": "three catalogue SKUs are not written as the orders write them (Ch 11)",
            "legacy_ids": "212 accounts have a second, frozen record in the old CRM; orders used it (Ch 12)",
            "overwritten_owner": "the CRM keeps only today's owner (Ch 12)",
            "late_cancellation_reason": "cancellation_reason is filled in days after a contract ends (Ch 12, 22)",
            "portal_duplicates": "about 1.2% of events are delivered twice (Ch 19)",
            "portal_late": "some mobile events arrive hours or days late (Ch 19)",
            "portal_clock_skew": "some mobile clocks are hours wrong (Ch 19)",
            "portal_bot": "a price-scraping bot logged in as a customer (Ch 19)",
            "portal_schema_v2": "web events change shape on 1 October 2025; the app on 10 November (Ch 19)",
            "ledger_journals": "finance's manual journals have no source row (Ch 15)",
            "pii_in_text": "some ticket comments contain phone numbers and emails (Ch 24)",
        },
    }
    with open(os.path.join(out, "manifest.json"), "w") as f:
        json.dump(manifest, f, indent=2)
        f.write("\n")
    print(f"Wrote {out}")


if __name__ == "__main__":
    main()
