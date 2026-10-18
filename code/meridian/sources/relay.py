"""
Relay, Meridian's CRM: the nightly export for any night, and the field audit.

The CRM keeps current state. Every night at 23:00 in Newark a job writes two files,
`accounts.csv` and `contracts.csv`, describing every record as it stands at that moment.
The export folder keeps a fortnight. This module can write the export for any night since
1 January 2022, because Meridian's IT kept an archive, and the book needs older nights.

Before 1 March 2024 the CRM was an older system, and its exports look different: semicolons,
Windows-1252, US dates, other column names, postcodes written as numbers. On eight nights
there was no export at all, and `export()` writes nothing for them.

    from meridian.sources import relay
    relay.export("2024-02-29", "data/sandbox/landing/relay")   # -> list of paths
"""
from __future__ import annotations

import csv
import os
import sqlite3
from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo

from meridian.sources import SOURCES, UTC, parse_moment

STORE = SOURCES / "relay" / "_store.db"
MIGRATION = date(2024, 3, 1)

LEGACY_ACCOUNTS = ["Acct ID", "Account Name", "Region", "Segment", "Owner", "Customer Since",
                   "Zip", "Key Acct", "Status", "Closed Date", "Primary Contact",
                   "Contact Email", "Contact Phone"]
LEGACY_CONTRACTS = ["Contract ID", "Acct ID", "Start", "End", "Term (months)", "Legacy Terms",
                    "Discount %", "Outcome", "Notice Date", "Cancellation Reason"]
RELAY_ACCOUNTS = ["account_id", "account_name", "region", "segment", "owner", "customer_since",
                  "postcode", "key_account", "status", "closed_on", "contact_name",
                  "contact_email", "contact_phone", "updated_at"]
RELAY_CONTRACTS = ["contract_id", "account_id", "start_date", "end_date", "term_months",
                   "legacy_terms", "discount_pct", "outcome", "notice_date",
                   "cancellation_reason", "updated_at"]
REGIONS = {1: "Northeast", 2: "Southeast", 3: "Midwest", 4: "West", 5: "Southwest"}


def _night(night) -> date:
    return night if isinstance(night, date) and not isinstance(night, datetime) \
        else date.fromisoformat(str(night)[:10])


def nights(store=STORE) -> list[tuple[str, bool, str]]:
    """Every night since exports began: (night, exported?, which system)."""
    con = sqlite3.connect(f"file:{store}?mode=ro", uri=True)
    rows = con.execute("SELECT night, exported, system FROM nights ORDER BY night").fetchall()
    con.close()
    return [(n, bool(e), s) for n, e, s in rows]


def _us(d: str | None) -> str:
    return "" if not d else f"{d[5:7]}/{d[8:10]}/{d[0:4]}"


def _last_first(name: str) -> str:
    first, _, last = name.partition(" ")
    return f"{last}, {first}"


def _iso_z(s: str) -> str:
    return s.replace(" ", "T") + "Z"


def export(night, dest_root, store=STORE) -> list[str]:
    """Write night's export into dest_root/<night>/, as the CRM wrote it. [] if none ran."""
    n = _night(night)
    con = sqlite3.connect(f"file:{store}?mode=ro", uri=True)
    row = con.execute("SELECT exported, system, exported_at FROM nights WHERE night = ?",
                      (n.isoformat(),)).fetchone()
    if row is None:
        con.close()
        raise ValueError(f"no export is scheduled for {n}; exports run 2022-01-01 to 2026-01-04")
    exported, system, at = row
    if not exported:
        con.close()
        return []
    valid = "valid_from <= :at AND (valid_to IS NULL OR valid_to > :at)"
    accounts = con.execute(
        f"SELECT account_id, name, region_id, segment, owner, customer_since, postcode, "
        f"key_account, status, closed_on, contact_name, contact_email, contact_phone, "
        f"valid_from FROM account_versions WHERE {valid} ORDER BY account_id", {"at": at}).fetchall()
    contracts = con.execute(
        f"SELECT contract_id, account_id, start_date, end_date, term_months, legacy_terms, "
        f"discount_pct, outcome, notice_date, cancellation_reason, valid_from "
        f"FROM contract_versions WHERE {valid} ORDER BY contract_id", {"at": at}).fetchall()
    con.close()

    folder = os.path.join(str(dest_root), n.isoformat())
    os.makedirs(folder, exist_ok=True)
    paths = []
    if system == "legacy":
        a_rows = [(a, name, REGIONS[r], seg, owner, _us(since), str(int(pc)),
                   "Y" if key else "N", status.capitalize(), _us(closed),
                   _last_first(cname), cemail or "", cphone)
                  for a, name, r, seg, owner, since, pc, key, status, closed, cname, cemail,
                  cphone, _ in accounts]
        c_rows = [(c, a, _us(s), _us(e), term, "Y" if leg else "N",
                   "" if disc is None else disc,
                   {"renewed": "Renewed", "not_renewed": "Not renewed"}.get(out, ""),
                   _us(notice), reason or "")
                  for c, a, s, e, term, leg, disc, out, notice, reason, _ in contracts]
        for name, header, rows in (("accounts.csv", LEGACY_ACCOUNTS, a_rows),
                                   ("contracts.csv", LEGACY_CONTRACTS, c_rows)):
            path = os.path.join(folder, name)
            with open(path, "w", newline="", encoding="cp1252") as f:
                w = csv.writer(f, delimiter=";", lineterminator="\r\n")
                w.writerow(header)
                w.writerows(rows)
            paths.append(path)
    else:
        a_rows = [(a, name, REGIONS[r], seg, owner, since, pc, "true" if key else "false",
                   status, closed or "", cname, cemail or "", cphone, _iso_z(vf))
                  for a, name, r, seg, owner, since, pc, key, status, closed, cname, cemail,
                  cphone, vf in accounts]
        c_rows = [(c, a, s, e, term, "true" if leg else "false", "" if disc is None else disc,
                   out or "", notice or "", reason or "", _iso_z(vf))
                  for c, a, s, e, term, leg, disc, out, notice, reason, vf in contracts]
        for name, header, rows in (("accounts.csv", RELAY_ACCOUNTS, a_rows),
                                   ("contracts.csv", RELAY_CONTRACTS, c_rows)):
            path = os.path.join(folder, name)
            with open(path, "w", newline="", encoding="utf-8-sig") as f:
                w = csv.writer(f, lineterminator="\n")
                w.writerow(header)
                w.writerows(rows)
            paths.append(path)
    t = parse_moment(at).timestamp()
    for p in paths:
        os.utime(p, (t, t))
    return paths


def write_audit(dest_root, store=STORE, until=None) -> list[str]:
    """
    The field audit, one file per month. Each night's export job rewrites the current
    month's file with the month so far, so a month is complete once the next begins.
    The audit came across in the migration, so every file is in Relay's format.
    """
    until = parse_moment(until) if until else parse_moment("2026-01-05 06:00")
    con = sqlite3.connect(f"file:{store}?mode=ro", uri=True)
    rows = con.execute("SELECT changed_at, object, record_id, field, old_value, new_value, "
                       "changed_by FROM field_history ORDER BY changed_at, object, record_id, "
                       "field").fetchall()
    con.close()
    by_month: dict[str, list] = {}
    for r in rows:
        by_month.setdefault(r[0][:7], []).append(r)
    folder = os.path.join(str(dest_root), "audit")
    os.makedirs(folder, exist_ok=True)
    paths = []
    last_export = None                     # the most recent 23:00 Newark export before until
    for n, exported, _ in nights(store):
        at = datetime.fromisoformat(n + " 23:00").replace(
            tzinfo=ZoneInfo("America/New_York")).astimezone(UTC)
        if exported and at <= until:
            last_export = at
    for month, rs in sorted(by_month.items()):
        y, m = int(month[:4]), int(month[5:7])
        complete = datetime(y + (m == 12), m % 12 + 1, 1, 4, 0, tzinfo=UTC)
        lands = min(complete, last_export)
        rs = [r for r in rs if parse_moment(r[0]) <= lands]
        if not rs or datetime(y, m, 1, tzinfo=UTC) > lands:
            continue
        path = os.path.join(folder, f"field_history_{month}.csv")
        with open(path, "w", newline="", encoding="utf-8") as f:
            w = csv.writer(f, lineterminator="\n")
            w.writerow(["changed_at", "object", "record_id", "field", "old_value", "new_value",
                        "changed_by"])
            w.writerows([(_iso_z(c), o, rid, fld, old or "", new or "", by)
                         for c, o, rid, fld, old, new, by in rs])
        os.utime(path, (lands.timestamp(), lands.timestamp()))
        paths.append(path)
    return paths


def exports_between(first, last, dest_root, store=STORE) -> list[str]:
    """Every night's export from first to last inclusive: what an archive restore gives you."""
    out, d = [], _night(first)
    while d <= _night(last):
        out += export(d, dest_root, store)
        d += timedelta(days=1)
    return out
