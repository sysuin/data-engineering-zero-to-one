# timeout: 3600
# How late the quarter's data came: for every night, how far back the
# days it changed lie, source by source; and what a window would cover.
import json
from collections import Counter
from datetime import timedelta

import duckdb
from _sandbox import FIRST, quarter

LAGS = """
WITH home AS (
    SELECT a.account_id, r.timezone FROM marts.dim_account AS a
    JOIN marts.dim_region AS r USING (region_key) WHERE a.is_current),
day AS (                    -- each order's day, on its depot's calendar
    SELECT o.order_id, CAST(timezone(coalesce(h.timezone, 'UTC'),
           timezone('UTC', o.placed_at)) AS DATE) AS day
    FROM staging.stg_tradeline__orders AS o
    LEFT JOIN intermediate.int_account_identity AS i
           ON i.crm_id = o.account_id
    LEFT JOIN home AS h ON h.account_id = i.account_id),
changed AS (                -- an order a night's log touched
    SELECT load_date, id AS order_id FROM raw.tradeline_orders_log
    UNION
    SELECT load_date, order_id FROM raw.tradeline_order_lines_log)
SELECT 'tradeline' AS source, load_date AS night,
       load_date - d.day AS lag
FROM changed JOIN day AS d USING (order_id)
UNION ALL                   -- a ticket changed, against its first day
SELECT 'frontline', load_date, load_date - CAST(strptime(
           rtrim(created_at, 'Z'), '%Y-%m-%dT%H:%M:%S') AS DATE)
FROM raw.frontline_tickets WHERE load_date > $first
UNION ALL                   -- a list arrived, against its first day
SELECT 'suppliers', CAST(f.arrived_at AS DATE),
       CAST(f.arrived_at AS DATE) - min(p.effective_date)
FROM audit.file_ledger AS f
JOIN staging.stg_suppliers__price_lists AS p
  ON p.file_sha256 = f.sha256
WHERE f.source = 'suppliers' AND f.status = 'loaded'
  AND f.load_date > $first
GROUP BY f.sha256, f.arrived_at
"""

CONTRACTS = """
WITH h AS (                 -- each contract's row each night, hashed
    SELECT load_date, contract_id, end_date,
           md5(row(account_id, start_date, end_date, term_months,
                   legacy_terms, discount_pct, outcome, notice_date,
                   cancellation_reason)::VARCHAR) AS h
    FROM raw.relay_contracts WHERE load_date >= $first),
pairs AS (
    SELECT night, lag(night) OVER (ORDER BY night) AS before
    FROM (SELECT DISTINCT load_date AS night FROM h)),
moved AS (                  -- a contract whose row changed that night
    SELECT p.night, r.end_date
    FROM pairs AS p JOIN h AS r ON r.load_date = p.night
    ANTI JOIN h AS b ON b.load_date = p.before
         AND b.contract_id = r.contract_id AND b.h = r.h
    WHERE p.before IS NOT NULL)
SELECT 'relay', night, night - CAST(end_date AS DATE)
FROM moved WHERE CAST(end_date AS DATE) < night
"""

BUCKETS = [(-10_000, -1, "<0"), (0, 0, "0"), (1, 1, "1"),
           (2, 7, "2-7"), (8, 30, "8-30"), (31, 10_000, "31+")]

if __name__ == "__main__":
    env = quarter()
    con = duckdb.connect(str(env.warehouse), read_only=True)
    rows = con.execute(LAGS, {"first": FIRST}).fetchall()
    rows += con.execute(CONTRACTS, {"first": FIRST}).fetchall()
    con.close()
    by = {}
    for source, night, lag in rows:
        by.setdefault(source, []).append(lag)
    print("how many days after its day a change arrived, nights "
          f"{FIRST + timedelta(days=1)} on")
    print(f"{'source':<11}{'changes':>8}"
          + "".join(f"{b[2]:>7}" for b in BUCKETS) + f"{'max':>6}")
    for source in ("tradeline", "frontline", "relay", "suppliers"):
        lags = by[source]
        n = [sum(lo <= x <= hi for x in lags) for lo, hi, _ in BUCKETS]
        print(f"{source:<11}{len(lags):>8,}"
              + "".join(f"{k:>7,}" for k in n) + f"{max(lags):>6}")
    print("(tradeline: an order's changes; frontline: a ticket's; "
          "relay: a\n contract's, after it ended; suppliers: a list's "
          "arrival, after\n the day it took effect)")
    t = by["tradeline"]
    print("\nwindow   tradeline changes inside it")
    for w in (0, 1, 3, 7, 30):
        inside = sum(x <= w for x in t)
        print(f"{w:>4} d   {inside:>8,}  {100 * inside / len(t):6.2f}%"
              f"   {len(t) - inside:>6,} outside")
    with open("code/18/04_late_window.json", "w") as f:
        json.dump({s: {str(k): n for k, n in sorted(Counter(v).items())}
                   for s, v in by.items()}, f, indent=1)
