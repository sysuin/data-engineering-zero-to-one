# How big Meridian's data is, system by system, and what one laptop has.
import shutil

import duckdb
import psutil

from groundwork.config import LANDING, SANDBOX, SOURCES, TRADELINE_DB

MB = 1_000_000


def size(path):
    """Bytes under a path: a file, or every file in a folder."""
    if path.is_file():
        return path.stat().st_size
    return sum(p.stat().st_size for p in path.rglob("*") if p.is_file())


systems = [
    ("Tradeline (orders database)", TRADELINE_DB),
    ("Relay (CRM, its own store)", SOURCES / "relay"),
    ("Frontline (support desk)", SOURCES / "frontline"),
    ("Portal events (landed)", LANDING / "portal"),
    ("Relay exports (landed)", LANDING / "relay"),
    ("Price lists, ledger, catalogue", [LANDING / "suppliers",
                                        LANDING / "ledger",
                                        LANDING / "catalogue"]),
]
total = 0
print(f"{'system':<30}{'MB':>8}")
for name, where in systems:
    where = where if isinstance(where, list) else [where]
    b = sum(size(p) for p in where)
    total += b
    print(f"{name:<30}{b / MB:>8.1f}")
print(f"{'all of Meridian':<30}{total / MB:>8.1f}")

# The biggest table, three ways: in SQLite, as Parquet, in memory.
out = SANDBOX / "ch20"
out.mkdir(parents=True, exist_ok=True)
con = duckdb.connect()
con.execute(f"ATTACH '{TRADELINE_DB}' AS t (TYPE sqlite, READ_ONLY)")
con.execute("CREATE TABLE lines AS SELECT * FROM t.order_lines")
con.execute(f"COPY lines TO '{out}/lines.parquet'")
lines = con.execute("SELECT * FROM lines").to_arrow_table()
pq = (out / "lines.parquet").stat().st_size
print()
print(f"order_lines: {lines.num_rows:,} rows")
print(f"  in memory (Arrow)     {lines.nbytes / MB:>7.1f} MB")
print(f"  on disk as Parquet    {pq / MB:>7.1f} MB")

vm = psutil.virtual_memory()
print()
print(f"this machine: {vm.total / 2**30:.0f} GiB of memory, "
      f"{psutil.cpu_count(logical=False)} cores")
print(f"order_lines in memory is {lines.nbytes / vm.total:.1%} "
      "of it")
shutil.rmtree(out)
print(f"sandbox cleared: {not out.exists()}")
