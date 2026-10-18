# Six versions of one price list, each changed the way producers
# change files, read three ways: by name, by position, and by a
# declared header.
import json
import sys
from pathlib import Path

import duckdb

from groundwork.config import SANDBOX
from groundwork.ingest import files, suppliers

sys.path.insert(0, str(Path(__file__).parent))
from _variants import JUNE, KINDS, variant  # noqa: E402

here = SANDBOX / "ch10" / "drift"
here.mkdir(parents=True, exist_ok=True)
con = duckdb.connect()
TRUE = {r.split(",")[0]: (r.split(",")[1], r.split(",")[2])
        for r in JUNE.read_text().splitlines()[1:]}   # sku: item, cost
DECLARED = {"Item Code": "VARCHAR", "Item": "VARCHAR",
            "Cost (USD)": "DECIMAL(10, 2)", "Valid From": "VARCHAR"}


def by_name(path):                       # Chapter 9's reader
    a = files.Arrival(path, f"suppliers/pemberton-mills/{path.name}",
                      0, None)
    return [(r["sku"], r["description"], r["cost"]) for r in
            suppliers.read(path.read_bytes(), a).rows]


def by_position(path):                   # columns= in DuckDB
    return con.execute(
        f"SELECT \"Item Code\", \"Item\", \"Cost (USD)\"::VARCHAR "
        f"FROM read_csv('{path}', header = true, "
        f"columns = {DECLARED})").fetchall()


def by_header(path):                     # read_declared, from 03
    header = path.read_text().splitlines()[0].split(",")
    if sorted(header) != sorted(DECLARED):
        raise ValueError(f"header {header}")
    cols = {c: DECLARED[c] for c in header}
    return con.execute(
        f"SELECT \"Item Code\", \"Item\", \"Cost (USD)\"::VARCHAR "
        f"FROM read_csv('{path}', header = true, columns = {cols}, "
        "auto_detect = false)").fetchall()


def outcome(read, path):
    try:
        got = read(path)
    except Exception as e:
        return "stops", type(e).__name__.removesuffix("Exception")
    right = sum(1 for sku, *rest in got if TRUE.get(sku) == tuple(rest))
    return ("right" if right == len(TRUE) == len(got) else "wrong",
            f"{right} of {len(got)}")


readers = {"by name": by_name, "by position": by_position,
           "declared": by_header}
print(f"{'change':<11}" + "".join(f"{r:<19}" for r in readers).rstrip())
grid = {}
for kind in KINDS:
    path = here / f"{kind.replace(' ', '_')}.csv"
    path.write_bytes(variant(kind))
    grid[kind] = {r: outcome(f, path) for r, f in readers.items()}
    print((f"{kind:<11}" + "".join(f"{o:<6}{why[:12]:<13}" for o, why
                                   in grid[kind].values())).rstrip())
    path.unlink()
here.rmdir()

Path(__file__).with_suffix(".json").write_text(json.dumps(
    {"kinds": KINDS, "grid": grid}, indent=1))
