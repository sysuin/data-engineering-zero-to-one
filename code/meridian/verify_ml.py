#!/usr/bin/env python3
"""
Prove that the scaled dataset is what the book says it is.

Three kinds of check:

1.  **Book 1 is untouched.** Its warehouse hashes to the value Book 1 shipped with.
2.  **Every planted fact and trap is present**, at roughly the size the chapters describe.
3.  **The truth is a function of the warehouse.** For a sample of renewals, the inputs to
    `renewal_logit()` are recomputed from SQL alone — no generator state — and must give
    the probability recorded in truth/renewals.csv. If this ever fails, the book's
    "best possible score" is a number about some other dataset.

Standard library only.

    python3 verify_ml.py [--data ../../data/meridian-ml] [--book1 ../../data/meridian]
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
import sys
from datetime import date, timedelta

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import generate_ml as g                                     # noqa: E402

BOOK1_WAREHOUSE_SHA256 = "f0bbc0ccc14de01e2bc8c19456dd291154cc55fb11758a95fb0fd2fd4bfc3100"

failures = []


def check(ok: bool, what: str, detail: str = "") -> None:
    print(f"  {'ok  ' if ok else 'FAIL'}  {what}" + (f"  ({detail})" if detail else ""))
    if not ok:
        failures.append(what)


def recompute(con: sqlite3.Connection, contract_id: int) -> dict:
    """The truth's inputs for one renewal, from the warehouse and nothing else."""
    (acct, end, legacy, disc) = con.execute(
        "SELECT account_id, end_date, legacy_terms, discount_pct FROM contracts "
        "WHERE contract_id = ?", (contract_id,)).fetchone()
    d = date.fromisoformat(end) - timedelta(days=g.DECIDE_BEFORE)
    name, since, postcode = con.execute(
        "SELECT name, since, postcode FROM accounts WHERE account_id = ?", (acct,)).fetchone()
    # The same company may also exist under a legacy CRM id: same postcode, same start date.
    ids = [r[0] for r in con.execute(
        "SELECT account_id FROM accounts WHERE postcode = ? AND since = ? "
        "AND (account_id = ? OR crm_source = 'Legacy CRM')", (postcode, since, acct))]
    marks = ",".join("?" * len(ids))
    lo90, lo180, lo365 = (d - timedelta(days=k) for k in (90, 180, 365))
    rows = con.execute(
        f"""SELECT o.order_date, SUM(l.qty * l.unit_price),
                   SUM(CASE WHEN p.supplier_id = (SELECT supplier_id FROM suppliers
                                                  WHERE name = 'Voss Industrial')
                            THEN l.qty * l.unit_price ELSE 0 END)
            FROM orders o JOIN order_lines l USING (order_id) JOIN products p USING (sku)
            WHERE o.account_id IN ({marks}) AND o.order_date < ?
            GROUP BY o.order_id ORDER BY o.order_date""", (*ids, d.isoformat())).fetchall()
    dates = [date.fromisoformat(r[0]) for r in rows]
    last = dates[-1] if dates else None
    spend = sum(r[1] for r, dt in zip(rows, dates) if dt >= lo365)
    voss = sum(r[2] for r, dt in zip(rows, dates) if dt >= lo365)
    n90 = sum(1 for dt in dates if dt >= lo90)
    n_prev = sum(1 for dt in dates if lo180 <= dt < lo90)
    tickets = con.execute(
        f"""SELECT substr(opened_at, 1, 10), category, sku FROM tickets
            WHERE account_id IN ({marks}) AND substr(opened_at, 1, 10) < ?""",
        (*ids, d.isoformat())).fetchall()
    complaints = sum(1 for t, cat, _ in tickets
                     if date.fromisoformat(t) >= lo90 and cat in ("Quality", "Delivery"))
    pem = any(sku == g.DEFECT_SKU and cat in ("Quality", "Delivery")
              and g.DEFECT_START <= date.fromisoformat(t) <= g.DEFECT_END
              for t, cat, sku in tickets)
    seg = con.execute(
        "SELECT segment FROM account_history WHERE account_id = ? AND valid_from <= ? "
        "ORDER BY valid_from DESC LIMIT 1", (acct, d.isoformat())).fetchone()[0]
    return {
        "decided_on": d, "recency_days": min((d - last).days, 365) if last else 365,
        "order_trend": max(-2.0, min(2.0, math.log((n90 + 1) / (n_prev + 1)))),
        "complaints_90": complaints, "spend_365": spend,
        "voss_share": voss / spend if spend else 0.0,
        "tenure_years": (d - date.fromisoformat(since)).days / 365.25,
        "legacy_terms": bool(legacy), "discount_pct": disc or 0, "segment": seg,
        "pemberton_complaint": pem,
    }


def main() -> int:
    here = os.path.dirname(os.path.abspath(__file__))
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--data", default=os.path.join(here, "..", "..", "data", "meridian-ml"))
    ap.add_argument("--book1", default=os.path.join(here, "..", "..", "data", "meridian"))
    args = ap.parse_args()
    db = os.path.join(args.data, "warehouse", "meridian_ml.db")
    con = sqlite3.connect(db)
    q = lambda sql, *a: con.execute(sql, a).fetchall()        # noqa: E731
    one = lambda sql, *a: con.execute(sql, a).fetchone()[0]    # noqa: E731
    manifest = json.load(open(os.path.join(args.data, "manifest.json")))

    print("Book 1")
    with open(os.path.join(args.book1, "warehouse", "meridian.db"), "rb") as f:
        digest = hashlib.sha256(f.read()).hexdigest()
    check(digest == BOOK1_WAREHOUSE_SHA256, "Book 1's warehouse is byte-identical")
    check(one("SELECT COUNT(*) FROM orders WHERE order_id < 1000000") == 66_676,
          "the key accounts' 66,676 orders are carried over")

    print("Size and base rate")
    n_tail = one("SELECT COUNT(*) FROM accounts WHERE is_key_account = 0 "
                 "AND crm_source = 'Meridian CRM'")
    check(n_tail == g.N_TAIL, "long-tail accounts", f"{n_tail:,}")
    rate = one("SELECT AVG(outcome = 'not_renewed') FROM contracts c JOIN accounts a "
               "USING (account_id) WHERE a.is_key_account = 0 AND outcome IS NOT NULL "
               "AND end_date >= '2023-01-01'")
    check(0.05 <= rate <= 0.10, "non-renewal rate, 2023-2025", f"{rate:.1%}")

    print("Planted facts")
    check(one("SELECT COUNT(*) FROM v_sales WHERE category = 'Sanitation' "
              "AND order_date < ?", g.SANITATION_LAUNCH.isoformat()) == 0,
          "no Sanitation sales before the launch")
    hall = q("SELECT MAX(o.order_date), c.outcome FROM accounts a JOIN orders o USING (account_id) "
             "JOIN contracts c ON c.account_id = a.account_id AND c.end_date = ? "
             "WHERE a.name LIKE 'Halloway%'", g.HALLOWAY_END.isoformat())[0]
    check(hall == (g.HALLOWAY_END.isoformat(), "not_renewed"), "Halloway leaves on 30 June 2024",
          str(hall))
    h_q1 = one("SELECT COUNT(*) FROM tickets t JOIN accounts a USING (account_id) "
               "WHERE a.name LIKE 'Halloway%' AND opened_at BETWEEN '2024-01-01' AND '2024-04-01'")
    h_prior = one("SELECT COUNT(*) FROM tickets t JOIN accounts a USING (account_id) "
                  "WHERE a.name LIKE 'Halloway%' AND opened_at < '2024-01-01'")
    check(h_q1 > h_prior / 4 * 2, "Halloway's complaints rise in early 2024",
          f"{h_q1} in 2024 Q1, {h_prior} in all of 2023")
    q3 = one("SELECT COUNT(*) FROM tickets WHERE opened_at BETWEEN '2024-07-01' AND '2024-10-01'")
    q4 = one("SELECT COUNT(*) FROM tickets WHERE opened_at BETWEEN '2024-10-01' AND '2025-01-01'")
    check(q4 / q3 > 1.8, "ticket volume spikes in 2024 Q4 (the Pemberton defect)",
          f"{q4 / q3:.1f}x the previous quarter")
    voss_before = one("SELECT AVG(unit_price) FROM v_sales WHERE supplier = 'Voss Industrial' "
                      "AND account_id >= 59 AND account_id < 90000 "
                      "AND order_date BETWEEN '2024-10-01' AND '2025-01-31'")
    voss_after = one("SELECT AVG(unit_price) FROM v_sales WHERE supplier = 'Voss Industrial' "
                     "AND account_id >= 59 AND account_id < 90000 AND order_date >= '2025-02-01'")
    check(voss_after > voss_before * 1.03, "Voss prices rise for the long tail in 2025",
          f"average unit price {voss_before:.2f} -> {voss_after:.2f}")

    print("Traps")
    leak = q("SELECT outcome, SUM(cancellation_reason IS NOT NULL), COUNT(*) FROM contracts "
             "WHERE outcome IS NOT NULL GROUP BY outcome ORDER BY outcome")
    check(leak[0][1] == leak[0][2] and leak[1][1] == 0,
          "cancellation_reason exists only for accounts that left")
    ret = dict(q("SELECT c.outcome, AVG(a.account_manager = 'Retention desk') FROM contracts c "
                 "JOIN accounts a USING (account_id) WHERE c.outcome IS NOT NULL "
                 "AND c.end_date >= '2023-01-01' GROUP BY c.outcome"))
    check(ret["not_renewed"] > 0.5 and ret["renewed"] < 0.1,
          "today's account manager gives the leavers away",
          f"Retention desk: {ret['not_renewed']:.0%} of leavers, {ret['renewed']:.0%} of renewals")
    dup = one("SELECT COUNT(*) FROM accounts WHERE crm_source = 'Legacy CRM'")
    after = one("SELECT COUNT(*) FROM orders WHERE account_id >= 90000 AND order_date >= ?",
                g.MIGRATION.isoformat())
    check(dup == g.N_DUPLICATES and after == 0,
          "legacy CRM ids stop ordering at the migration", f"{dup} ids")
    nulls = q("SELECT legacy_terms, SUM(discount_pct IS NULL), COUNT(*), "
              "AVG(outcome = 'not_renewed') FROM contracts WHERE outcome IS NOT NULL "
              "GROUP BY legacy_terms ORDER BY legacy_terms")
    check(nulls[0][1] == 0 and nulls[1][1] == nulls[1][2] and nulls[1][3] > nulls[0][3],
          "discount is missing exactly on legacy terms, which leave more often",
          f"{nulls[1][3]:.1%} vs {nulls[0][3]:.1%}")
    truth_t = {r["ticket_id"]: r for r in csv.DictReader(
        open(os.path.join(args.data, "truth", "tickets.csv")))}
    damaged = {}
    for tid, desk, cat in q("SELECT ticket_id, desk, category FROM tickets"):
        if truth_t[tid]["true_intent"] == "damaged":
            damaged.setdefault(desk, []).append(cat)
    north = damaged["North desk"].count("Quality") / len(damaged["North desk"])
    south = damaged["South desk"].count("Delivery") / len(damaged["South desk"])
    check(north > 0.9 and south > 0.9, "the two desks file damaged goods differently",
          f"North {north:.0%} Quality, South {south:.0%} Delivery")

    print("The truth is a function of the warehouse")
    truth = [r for r in csv.DictReader(open(os.path.join(args.data, "truth", "renewals.csv")))
             if r["key_account"] == "0" and r["end_date"] >= "2023-01-01"]
    sample = random.Random(7).sample(truth, 300)
    worst = 0.0
    for r in sample:
        p = g.sigmoid(g.renewal_logit(recompute(con, int(r["contract_id"]))))
        worst = max(worst, abs(p - float(r["p_leave"])))
    check(worst < 1e-5, "300 renewals recomputed from SQL match the recorded probability",
          f"largest difference {worst:.1e}")
    scored = [(float(r["p_leave"]), r["outcome"] == "not_renewed") for r in truth if r["outcome"]]
    ceiling = g.auc([p for p, _ in scored], [y for _, y in scored])
    check(abs(ceiling - manifest["renewals"]["ceiling_auc"]) < 1e-4,
          "the best possible AUC matches the manifest", f"{ceiling:.3f}")

    print()
    if failures:
        print(f"{len(failures)} check(s) failed")
        return 1
    print("All checks passed.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
