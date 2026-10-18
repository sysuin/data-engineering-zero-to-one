# Tradeline's tables, read through a read-only connection.
import sqlite3
import textwrap

from meridian.sources import tradeline

con = tradeline.connect()        # today's database, read-only

tables = [t for (t,) in con.execute(
    "SELECT name FROM sqlite_master WHERE type = 'table' "
    "ORDER BY name")]
for t in tables:
    (n,) = con.execute(f"SELECT COUNT(*) FROM {t}").fetchone()
    info = con.execute(f"PRAGMA table_info({t})").fetchall()
    key = [r[1] for r in sorted(info, key=lambda r: r[5]) if r[5]]
    print(f"{t:<16}{n:>11,} rows   key: {', '.join(key) or '-'}")

for t in ("orders", "order_lines"):
    cols = [f"{r[1]} {r[2]}" for r in
            con.execute(f"PRAGMA table_info({t})")]
    print(textwrap.fill(f"{t}: " + ", ".join(cols), width=68,
                        subsequent_indent="  "))

print("channels:", con.execute(
    "SELECT group_concat(code || '=' || label, ', ') "
    "FROM (SELECT * FROM channels ORDER BY code)").fetchone()[0])

try:
    con.execute("UPDATE orders SET status = 'placed' WHERE id = 1")
except sqlite3.OperationalError as e:
    print(f"an attempt to write: {e}")
