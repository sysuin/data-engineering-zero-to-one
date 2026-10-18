# Skew: a key that owns most of the rows, and one way to split it up.
# nondeterministic: the reducers' seconds differ from run to run
import heapq
import json
import time
import zlib
from collections import Counter, defaultdict
from concurrent.futures import ProcessPoolExecutor
from pathlib import Path

import duckdb

from groundwork.config import LANDING, TRADELINE_DB

R = 8
PLANS = {"by channel": lambda ch, o: ch,
         "salted": lambda ch, o: (ch, o % R)}    # channel plus a salt


def reduce_task(rows):
    """Total each order, then sort the order totals of each key."""
    t0 = time.perf_counter()
    orders = defaultdict(Counter)
    for key, order_id, cents in rows:
        orders[key][order_id] += cents
    out = {k: sorted(v.values()) for k, v in orders.items()}
    return out, time.perf_counter() - t0


def shuffle(lines, key_of):
    parts = [[] for _ in range(R)]
    for _, channel, order_id, cents in lines:
        key = key_of(channel, order_id)
        owner = zlib.crc32(str(key).encode()) % R
        parts[owner].append((key, order_id, cents))
    return parts


def median(values):
    return values[(len(values) - 1) // 2]


def hottest_account(con, lines):
    per_account = Counter(a for a, _, _, _ in lines)
    (hot, n), = per_account.most_common(1)
    (name,) = con.execute(f"""
        SELECT account_name FROM read_csv(
            '{LANDING}/relay/2026-01-04/accounts.csv')
        WHERE account_id = {hot}""").fetchone()
    mean = len(lines) / len(per_account)
    print(f"hottest account: {hot} ({name}), {n:,} lines, "
          f"{n / mean:.0f}x the mean")
    print(f"{'reducers':>8}{'mean rows':>11}{'largest':>9}"
          f"{'largest/mean':>14}")
    for r in (4, 16, 64, 256, 1024):
        sizes = Counter()
        for account, count in per_account.items():
            sizes[account % r] += count
        big = max(sizes.values())
        print(f"{r:>8}{len(lines) / r:>11,.0f}{big:>9,}"
              f"{big / (len(lines) / r):>13.1f}x")


def median_by_channel(pool, lines, plan):
    parts = shuffle(lines, PLANS[plan])
    results = list(pool.map(reduce_task, parts))
    # A salted key needs one more, small step: merge each channel's
    # sorted pieces back together before taking the middle.
    pieces = defaultdict(list)
    for out, _ in results:
        for key, values in out.items():
            channel = key if plan == "by channel" else key[0]
            pieces[channel].append(values)
    medians = {ch: median(list(heapq.merge(*p)))
               for ch, p in sorted(pieces.items())}
    return [len(p) for p in parts], [s for _, s in results], medians


if __name__ == "__main__":
    con = duckdb.connect()
    con.execute(f"ATTACH '{TRADELINE_DB}' AS t "
                "(TYPE sqlite, READ_ONLY)")
    lines = con.execute("""
        SELECT o.account_id, o.channel, l.order_id,
               l.qty * l.unit_price_cents
        FROM t.order_lines l JOIN t.orders o ON o.id = l.order_id
        WHERE o.status = 'placed' ORDER BY l.order_id, l.line_no
    """).fetchall()
    hottest_account(con, lines)

    with ProcessPoolExecutor(R) as pool:
        runs = {p: median_by_channel(pool, lines, p) for p in PLANS}
    print()
    print(f"{'':>8}{'by channel':>22}{'salted':>22}")
    print(f"{'reducer':>8}" + f"{'rows':>12}{'seconds':>10}" * 2)
    for r in range(R):
        print(f"{r:>8}" + "".join(f"{runs[p][0][r]:>12,}"
                                  f"{runs[p][1][r]:>10.3f}"
                                  for p in PLANS))
    rows, secs, _ = runs["by channel"]
    print(f"by channel: busiest reducer holds "
          f"{max(rows) / len(lines):.0%} of the rows; "
          f"{rows.count(0)} reducers idle")
    slow = {p: max(runs[p][1]) for p in PLANS}
    print(f"slowest reducer faster after salting: "
          f"{slow['salted'] < slow['by channel']}")
    same = runs["by channel"][2] == runs["salted"][2]
    print(f"same medians both ways: {same}")
    print("median order  " + "  ".join(
        f"{ch} ${v // 100:,}.{v % 100:02d}"
        for ch, v in runs["salted"][2].items()))
    owners = {ch: zlib.crc32(ch.encode()) % R
              for ch in runs["salted"][2]}
    json.dump({"owners": owners, "share": max(rows) / len(lines),
               **{p: {"rows": runs[p][0], "seconds": runs[p][1]}
                  for p in PLANS}},
              open(Path(__file__).with_suffix(".json"), "w"), indent=1)
