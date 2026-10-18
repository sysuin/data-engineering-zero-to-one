# One job, revenue per account per month, run with threads, processes
# and DuckDB's own parallelism, all on one machine.
# nondeterministic: wall-clock timings differ from run to run
import sqlite3
import time
from collections import Counter
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor

import duckdb

from groundwork.config import TRADELINE_DB

LINES = """
    SELECT o.account_id, substr(o.placed_at, 1, 7) AS month,
           l.qty * l.unit_price_cents AS cents
    FROM order_lines l JOIN orders o ON o.id = l.order_id
    WHERE o.status = 'placed'
"""


def work(ids):
    """Sum cents per (account, month) for orders with id in [lo, hi]."""
    lo, hi = ids
    con = sqlite3.connect(f"file:{TRADELINE_DB}?mode=ro", uri=True)
    totals = Counter()
    rows = con.execute(LINES + " AND l.order_id BETWEEN ? AND ?",
                       (lo, hi))
    for account, month, cents in rows:
        totals[(account, month)] += cents
    return totals


def slices(n):
    """n ranges of order id that together cover every order."""
    con = sqlite3.connect(f"file:{TRADELINE_DB}?mode=ro", uri=True)
    (top,) = con.execute("SELECT max(id) FROM orders").fetchone()
    step = top // n + 1
    return [(i * step, (i + 1) * step - 1) for i in range(n)]


def merged(parts):
    total = Counter()
    for p in parts:
        total.update(p)
    return total


def python_pool(pool, n):
    with pool(n) as ex:
        return merged(ex.map(work, slices(n)))


def with_duckdb(threads):
    con = duckdb.connect(config={"threads": threads})
    con.execute(f"ATTACH '{TRADELINE_DB}' AS t "
                "(TYPE sqlite, READ_ONLY)")
    con.execute("USE t")
    rows = con.execute(f"SELECT account_id, month, SUM(cents) "
                       f"FROM ({LINES}) GROUP BY ALL").fetchall()
    return Counter({(a, m): int(c) for a, m, c in rows})


if __name__ == "__main__":
    runs = [("python, one process", 1, lambda: work((0, 10**12)))]
    for n in (4, 8):
        runs.append(("python, threads", n,
                     lambda n=n: python_pool(ThreadPoolExecutor, n)))
    for n in (2, 4, 8):
        runs.append(("python, processes", n,
                     lambda n=n: python_pool(ProcessPoolExecutor, n)))
    for n in (1, 8):
        runs.append(("duckdb", n, lambda n=n: with_duckdb(n)))

    answers, secs = [], []
    for name, n, job in runs:
        t = time.perf_counter()
        answers.append(job())
        secs.append(time.perf_counter() - t)

    print(f"{'approach':<22}{'workers':>8}{'seconds':>9}"
          f"{'speed-up':>10}")
    for (name, n, _), s in zip(runs, secs):
        print(f"{name:<22}{n:>8}{s:>9.2f}{secs[0] / s:>9.1f}x")
    print(f"groups: {len(answers[0]):,}; "
          f"same answer from all {len(runs)}: "
          f"{all(a == answers[0] for a in answers)}")
    def best(kind):
        return min(s for (nm, _, _), s in zip(runs, secs) if nm == kind)
    threads, procs = best("python, threads"), best("python, processes")
    print(f"best threads beat one process: {threads < secs[0]}")
    print(f"best processes beat one process: {procs < secs[0]}")
    print(f"duckdb, 8 threads, fastest of all: {min(secs) == secs[-1]}")
