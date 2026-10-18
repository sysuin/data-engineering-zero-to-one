# Exercise 5.5: rollback for the tiny table format, as one more commit.
import contextlib
import importlib.util
import io
import json
import os
from pathlib import Path

import duckdb

# Build §5.11's three-version table by running its listing quietly.
spec = importlib.util.spec_from_file_location(
    "tiny", Path(__file__).with_name("13_tiny_table_format.py"))
tiny = importlib.util.module_from_spec(spec)
with contextlib.redirect_stdout(io.StringIO()):
    spec.loader.exec_module(tiny)
t = tiny.t


def rollback(table, to: int) -> int:
    """Make the table read as at version `to`, by a new commit."""
    now, then = table.files(), table.files(to)
    v = len(table.versions())
    entry = {"version": v, "op": f"rollback to v{to}",
             "add": [f for f in then if f not in now],
             "remove": [f for f in now if f not in then]}
    tmp = table.log / f".{v:05d}.{os.getpid()}.tmp"
    tmp.write_text(json.dumps(entry))
    try:
        os.link(tmp, table.log / f"{v:05d}.json")
    finally:
        tmp.unlink()
    return v


disk_before = sorted(p.name for p in t.root.glob("*.parquet"))
rollback(t, 1)                      # undo the rewrite of 1 Dec
con = duckdb.connect()
print("version  rows    cents          files (part-...)")
for v in t.versions():
    rows, cents = con.execute(f"SELECT count(*), sum(qty * "
                              f"unit_price_cents) FROM {t.read(v)}"
                              ).fetchone()
    names = ", ".join(sorted(f[5:13] for f in t.files(v)))
    print(f"{v:>7} {rows:>5,} {cents:>13,}  {names}")
disk_after = sorted(p.name for p in t.root.glob("*.parquet"))
print(f"data files unchanged: {disk_before == disk_after}")
print(f"v3 reads as v1: {sorted(t.files(3)) == sorted(t.files(1))}")
