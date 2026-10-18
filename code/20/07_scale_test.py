# Meridian's order lines at 1x to 100x, three kinds of query, on one
# machine with DuckDB held to 1 GB of memory and no disk to spill to.
# nondeterministic: seconds and memory differ from run to run
# timeout: 300
import json
import shutil
import threading
import time
from concurrent.futures import ProcessPoolExecutor
from pathlib import Path

import duckdb
import psutil

from groundwork.config import SANDBOX, TRADELINE_DB

BASE = SANDBOX / "ch20" / "base.parquet"
SCALES = (1, 10, 20, 30, 50, 100)
# k copies of every line, each copy with its own orders and accounts.
LINES = """(SELECT order_id + r * 10_000_000 AS order_id,
                   account_id + r * 100_000 AS account_id,
                   qty, cents, placed_at
            FROM base, range({k}) copies(r))"""
QUERIES = {
    "revenue by month": """
        SELECT count(*), sum(s)::BIGINT FROM (
          SELECT date_trunc('month', placed_at), SUM(qty * cents) AS s
          FROM {lines} GROUP BY 1)""",
    "revenue per order": """
        SELECT count(*), sum(s)::BIGINT FROM (
          SELECT order_id, SUM(qty * cents) AS s
          FROM {lines} GROUP BY 1)""",
    "gap since last order": """
        SELECT count(*), max(gap) FROM (
          SELECT placed_at - lag(placed_at) OVER (
                   PARTITION BY account_id ORDER BY placed_at, order_id)
                 AS gap
          FROM {lines})""",
}


def measure(query, k):
    """One query at one scale, in a fresh process: seconds, peak MB."""
    con = duckdb.connect(config={"memory_limit": "1GB",
                                 "temp_directory": ""})   # no spilling
    con.execute(f"CREATE TABLE base AS FROM '{BASE}'")
    me, peak, done = psutil.Process(), [0], threading.Event()

    def watch():
        while not done.is_set():
            peak[0] = max(peak[0], me.memory_info().rss)
            time.sleep(0.02)

    watcher = threading.Thread(target=watch)
    watcher.start()
    t = time.perf_counter()
    try:
        con.execute(QUERIES[query].format(lines=LINES.format(k=k)))
        con.fetchall()
        ok = True
    except duckdb.OutOfMemoryException:
        ok = False
    secs = time.perf_counter() - t
    done.set()
    watcher.join()
    return ok, secs, peak[0] / 1e6


if __name__ == "__main__":
    BASE.parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect()
    con.execute(f"ATTACH '{TRADELINE_DB}' AS t "
                "(TYPE sqlite, READ_ONLY)")
    con.execute(f"""COPY (
        SELECT l.order_id, o.account_id, l.qty,
               l.unit_price_cents AS cents,
               o.placed_at::TIMESTAMP AS placed_at
        FROM t.order_lines l JOIN t.orders o ON o.id = l.order_id
        WHERE o.status = 'placed') TO '{BASE}'""")
    (rows,) = con.execute(f"SELECT count(*) FROM '{BASE}'").fetchone()
    vm = psutil.virtual_memory()
    print(f"machine: {vm.total / 2**30:.0f} GiB, "
          f"{vm.available / 2**30:.1f} GiB free now; DuckDB given 1 GB")
    results = {}
    with ProcessPoolExecutor(1, max_tasks_per_child=1) as pool:
        for q in QUERIES:
            print(f"{q}")
            for k in SCALES:
                ok, s, mb = pool.submit(measure, q, k).result()
                results.setdefault(q, []).append([k, ok, s, mb])
                what = f"{s:>7.2f} s{mb:>8,.0f} MB" if ok else \
                    "    out of memory"
                print(f"  {k:>4}x {rows * k:>13,} rows {what}")
    print("extrapolated from the last two that fitted, not run:")
    for q, runs in results.items():
        fitted = [r for r in runs if r[1]]
        (k1, _, s1, m1), (k2, _, s2, m2) = fitted[-2:]
        per_copy = (m2 - m1) / (k2 - k1)            # MB per 1x more
        if per_copy < 1:
            print(f"  {q}: memory flat; 1,000x in about "
                  f"{s2 * 1000 / k2:.0f} s")
        else:
            k = k2 + (vm.total / 1e6 - m2) / per_copy
            print(f"  {q}: all {vm.total / 2**30:.0f} GiB used "
                  f"at about {k:.0f}x")
    shutil.rmtree(BASE.parent)
    print(f"sandbox cleared: {not BASE.parent.exists()}")
    json.dump({"rows": rows, "results": results},
              open(Path(__file__).with_suffix(".json"), "w"), indent=1)
