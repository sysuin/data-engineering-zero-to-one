# The exact versions behind this book's printed output, and its floors.
import platform
import re
from importlib.metadata import PackageNotFoundError, version

import duckdb

from groundwork.config import ROOT

# A line of requirements.txt that names a package: name>=floor.
REQUIREMENT = re.compile(r"^([A-Za-z0-9_.-]+)(\[[a-z]+\])?>=([\d.]+)")
TOOLCHAIN = "# --- The toolchain"     # the heading of the last group

rows, tools = [], []
group = rows
for line in (ROOT / "requirements.txt").read_text().splitlines():
    if line.startswith(TOOLCHAIN):
        group = tools
    m = REQUIREMENT.match(line.strip())
    if m:
        name, floor = m.group(1), m.group(3)
        try:
            installed = version(name)
        except PackageNotFoundError:
            installed = "not installed"
        group.append((name, floor, installed))

print(f"Python {platform.python_version()}"
      f" ({platform.python_implementation()}),"
      f" {platform.platform(terse=True)} on {platform.machine()}\n")
print(f"{'package':<18}{'at least':>10}{'this book ran':>16}")
for name, floor, installed in rows:
    print(f"{name:<18}{floor:>10}{installed:>16}")
print("\nThe repository's own toolchain:")
for i in range(0, len(tools), 3):
    print("  " + "   ".join(f"{n} {v}" for n, _, v in tools[i:i + 3]))

con = duckdb.connect()
for name in ("sqlite", "encodings"):   # Chapters 2 and 5 load these
    con.execute(f"INSTALL {name}; LOAD {name}")
engine, source, codename = con.execute(
    "SELECT library_version, source_id, codename"
    " FROM pragma_version()").fetchone()
print(f"\nDuckDB's engine: {engine} ({codename}), build {source}")
print(f"{'extension':<16}{'version':>10}{'how it arrived':>20}")
for name, ext_version, mode in con.execute("""
        SELECT extension_name, extension_version, install_mode
        FROM duckdb_extensions() WHERE loaded
        ORDER BY extension_name""").fetchall():
    print(f"{name:<16}{ext_version:>10}{mode.lower():>20}")
