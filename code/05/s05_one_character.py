# Exercise 5.1: single-character edits to one line of a Relay export,
# and what DuckDB's default read_csv makes of each.
import shutil

import duckdb

from groundwork.config import LANDING, SANDBOX

src = LANDING / "relay" / "2026-01-04" / "accounts.csv"
lines = src.read_bytes().split(b"\n")
n_lines = len([ln for ln in lines[1:] if ln])
out = SANDBOX / "ch05" / "exercise"
out.mkdir(parents=True, exist_ok=True)
row = 101                                   # account 101's line
print(f"lines in the file {n_lines:,}; line edited:")
print(" ", lines[row][:60].decode())

edits = {                                   # each changes one byte
    "none": lambda s: s,
    "quote opens name": lambda s: s.replace(b",", b',"', 1),
    "comma to newline": lambda s: s.replace(b",", b"\n", 1),
    "day 01 to 0l": lambda s: s.replace(b"-01,", b"-0l,", 1),
    "false to fal5e": lambda s: s.replace(b",false,", b",fal5e,", 1),
}
con = duckdb.connect()
print(f"\n{'edit':<18} {'rows':>6} {'cols':>4}  types that changed")
base = None
for name, edit in edits.items():
    changed = lines[:row] + [edit(lines[row])] + lines[row + 1:]
    path = out / f"{name.replace(' ', '_')}.csv"
    path.write_bytes(b"\n".join(changed))
    try:
        types = dict(con.execute(f"""SELECT column_name, column_type
            FROM (DESCRIBE FROM read_csv('{path}'))""").fetchall())
        rows = con.execute(f"SELECT count(*) FROM (FROM read_csv("
                           f"'{path}'))").fetchone()[0]
    except duckdb.Error as e:
        print(f"{name:<18}  error: {str(e).splitlines()[0][:40]}")
        continue
    base = base or types
    diff = [f"{k} {types[k]}" for k in base
            if k in types and types[k] != base[k]]
    if len(types) != len(base):
        diff = ["every column: one VARCHAR"]
    print(f"{name:<18} {rows:>6,} {len(types):>4}  "
          f"{'; '.join(diff) or '-'}")

shutil.rmtree(out)
