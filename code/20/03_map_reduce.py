# Revenue per account as a map, a shuffle and a reduce, each task in
# its own process, the shuffle written to files the way a cluster would.
import json
import shutil
import sqlite3
import time
from pathlib import Path
from collections import Counter
from concurrent.futures import ProcessPoolExecutor

import duckdb

from groundwork.config import SANDBOX, TRADELINE_DB

WHERE = SANDBOX / "ch20" / "shuffle"
LINES = """
    SELECT o.account_id, l.qty * l.unit_price_cents
    FROM order_lines l JOIN orders o ON o.id = l.order_id
    WHERE o.status = 'placed' AND l.order_id BETWEEN ? AND ?
"""


def dollars(cents):
    """Integer cents as dollars, with no floating point on the way."""
    return f"${cents // 100:,}.{cents % 100:02d}"


def map_task(i, ids, reducers, combine=False):
    """Read one slice of order lines; write one file per reducer."""
    t0 = time.perf_counter()
    con = sqlite3.connect(f"file:{TRADELINE_DB}?mode=ro", uri=True)
    rows = con.execute(LINES, ids).fetchall()
    t_read = time.perf_counter()
    if combine:          # pre-sum on the map side (a combiner, §20.4)
        sums, lines = Counter(), Counter()
        for account, cents in rows:
            sums[account] += cents
            lines[account] += 1
        rows = [(a, sums[a], lines[a]) for a in sorted(sums)]
    else:
        rows = [(a, c, 1) for a, c in rows]
    # Partition: which reducer owns each key. Same key, same reducer.
    parts = [[] for _ in range(reducers)]
    for row in rows:
        parts[row[0] % reducers].append(row)
    t1 = time.perf_counter()
    out = WHERE / f"map-{i}"
    out.mkdir(parents=True, exist_ok=True)
    sizes = []
    for r, part in enumerate(parts):
        path = out / f"part-{r}.txt"
        path.write_text("".join(f"{a},{c},{n}\n" for a, c, n in part))
        sizes.append(path.stat().st_size)
    t2 = time.perf_counter()
    return {"records": [len(p) for p in parts], "bytes": sizes,
            "read_s": t_read - t0, "map_s": t1 - t_read,
            "write_s": t2 - t1}


def reduce_task(r, mappers):
    """Fetch this reducer's file from every mapper; sum by key."""
    t0 = time.perf_counter()
    records = []
    for i in range(mappers):
        text = (WHERE / f"map-{i}" / f"part-{r}.txt").read_text()
        records.extend(tuple(map(int, rec.split(",")))
                       for rec in text.splitlines())
    t1 = time.perf_counter()
    cents, lines = Counter(), Counter()
    for account, c, n in records:
        cents[account] += c
        lines[account] += n
    t2 = time.perf_counter()
    return {"records_in": len(records), "cents": dict(cents),
            "lines": dict(lines),
            "fetch_s": t1 - t0, "reduce_s": t2 - t1}


def run(mappers, reducers, combine=False):
    shutil.rmtree(WHERE, ignore_errors=True)
    con = sqlite3.connect(f"file:{TRADELINE_DB}?mode=ro", uri=True)
    (top,) = con.execute("SELECT max(id) FROM orders").fetchone()
    step = top // mappers + 1
    ids = [(i * step, (i + 1) * step - 1) for i in range(mappers)]
    with ProcessPoolExecutor(max(mappers, reducers)) as pool:
        maps = list(pool.map(map_task, range(mappers), ids,
                             [reducers] * mappers, [combine] * mappers))
        # The barrier: no reducer starts until every mapper is done.
        reds = list(pool.map(reduce_task, range(reducers),
                             [mappers] * reducers))
    shutil.rmtree(WHERE.parent)          # the whole sandbox, ch20
    return maps, reds


if __name__ == "__main__":
    M, R = 4, 4
    maps, reds = run(M, R)
    print(f"{'mapper':<8}{'records out, by reducer':>40}{'KB':>8}")
    for i, m in enumerate(maps):
        by = "".join(f"{n:>10,}" for n in m["records"])
        print(f"{i:<8}{by}{sum(m['bytes']) / 1000:>8,.0f}")
    print(f"{'reducer':<8}{'records in':>12}{'accounts':>10}")
    for r, red in enumerate(reds):
        print(f"{r:<8}{red['records_in']:>12,}{len(red['cents']):>10,}")
    shuffled = sum(sum(m["bytes"]) for m in maps)
    print(f"shuffled: {sum(r['records_in'] for r in reds):,} records, "
          f"{shuffled / 1e6:.1f} MB")

    # Check against one query in DuckDB.
    result = {}
    for red in reds:
        result.update(red["cents"])
    db = duckdb.connect()
    db.execute(f"ATTACH '{TRADELINE_DB}' AS t (TYPE sqlite, READ_ONLY)")
    truth = dict(db.execute("""
        SELECT o.account_id, SUM(l.qty * l.unit_price_cents)::BIGINT
        FROM t.order_lines l JOIN t.orders o ON o.id = l.order_id
        WHERE o.status = 'placed' GROUP BY 1""").fetchall())
    print(f"accounts: {len(result):,} by hand, "
          f"{len(truth):,} in DuckDB")
    print(f"revenue:  {dollars(sum(result.values()))} by hand")
    print(f"          {dollars(sum(truth.values()))} in DuckDB")
    print(f"every account agrees: {result == truth}")
    print(f"shuffle files left behind: {WHERE.parent.exists()}")

    json.dump({"mappers": [m["records"] for m in maps],
               "bytes": [m["bytes"] for m in maps],
               "reducers": [[r["records_in"], len(r["cents"])]
                            for r in reds],
               "accounts": len(result), "agrees": result == truth},
              open(Path(__file__).with_suffix(".json"), "w"), indent=1)
