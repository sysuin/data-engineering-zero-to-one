# The price lists' contract, read from its file and held to real and
# changed lists: the six kinds of drift, Kestrel's cents, and a unit
# that changes without anyone saying so.
import json
import textwrap
from decimal import Decimal
import sys
from pathlib import Path

from groundwork.config import LANDING
from groundwork.contracts import check
from groundwork.ingest import files, suppliers

sys.path.insert(0, str(Path(__file__).parent))
from _variants import KINDS, first_contract, variant  # noqa: E402

c = first_contract()
print(f"{c.table}, version {c.version}: {len(c.columns)} columns, "
      f"{len(c.formats)} formats")
print(f"  producer: {c.producer['who']}\n  owner:    {c.owner}")
for unit, rule in c.columns["cost"].when["cost_unit"].items():
    print(f"  cost in {unit:<8}" + ", ".join(f"{k} {v}" for k, v
                                             in rule.items()))

SUP = LANDING / "suppliers"


def arrival(name):
    return files.Arrival(SUP / name, f"suppliers/{name}", 0, None)


def held(data, name, history=None):
    """What the guard makes of a file: 'passed', 'parked n' or why
    it broke."""
    g = check.Guard(suppliers.read, c, history=history)
    try:
        p = g(data, arrival(name))
    except check.ContractBroken as e:
        return "broken", str(e).split(": ", 1)[1]
    res = g.seen[-1][2]
    return ("parked" if res.parked else "passed",
            "; ".join(res.notes) or f"{len(p.rows)} rows")


print("\nthe six changes, through Chapter 9's reader and the contract:")
grid = {}
for kind in KINDS:
    grid[kind] = held(variant(kind), "pemberton-mills/x.csv")
    status, why = grid[kind]
    print(f"  {kind:<10} {status:<7} {why if kind == 'as sent' else ''}"
          .rstrip())
    if kind != "as sent":
        print(textwrap.fill(why, 66, initial_indent="    ",
                            subsequent_indent="      "))

# Kestrel declares cents, and its cents are held to cents' rules.
k = "kestrel-supply/kestrel_20251201.csv"
real = (SUP / k).read_bytes()
print(f"\nKestrel as sent: {' with '.join(held(real, k))}")
undeclared = real.replace(b"sku,cost_cents,effective",
                          b"sku,description,unit_cost,currency,"
                          b"effective_from")
undeclared = b"\n".join(
    ln if i == 0 else b",".join([ln.split(b",")[0], b"", ln.split(b",")
                                 [1], b"USD", ln.split(b",")[2]])
    for i, ln in enumerate(undeclared.strip().split(b"\n"))) + b"\n"
print("Kestrel's cents under the dollar header:")
print(f"  {held(undeclared, k)[1]}")

# Wrenfield starts sending cents in its dollar column, unannounced.
w = "wrenfield-products/wrenfield-products_2025-12.csv"
nov = arrival("wrenfield-products/wrenfield-products_2025-11.csv")
lines = (SUP / w).read_text().splitlines()
rows = [x.split(",") for x in lines[1:]]
cents = [lines[0]] + [",".join([r[0], r[1],
                                str(int(Decimal(r[2]) * 100)), *r[3:]])
                      for r in rows]
data = ("\n".join(cents) + "\n").encode()
rows = suppliers.read(data, arrival(w)).rows
alone = check.check(c, rows)
before = check.History(c)
before.add(suppliers.read(nov.path.read_bytes(), nov).rows)
seen = before.check(rows)
print("\nWrenfield's December list, written in cents:")
for i, r in enumerate(rows, 1):
    rng = [b.rule for b in alone.bad if b.row == i] or ["ok"]
    his = [b.reason.split(" is ")[1].split(" times")[0]
           for b in seen if b.row == i]
    print(f"  {r['sku']} {r['cost']:>5}  range: {rng[0]:<6} "
          f"history: {his[0] + ' times' if his else 'ok'}")
print(f"  with history: {held(data, w, before)[1]}")

Path(__file__).with_suffix(".json").write_text(json.dumps(
    {"contract": grid}, indent=1))
