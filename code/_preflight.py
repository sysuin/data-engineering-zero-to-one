#!/usr/bin/env python3
"""
Check that everything the book needs is in place, before you spend an afternoon
discovering it is not.

    python3 code/_preflight.py

Nothing here needs a network connection. The one secret the book uses, Frontline's
token, is a demo value the local server accepts; the check shows whether it is set and
never prints it.
"""
from __future__ import annotations

import importlib
import os
import shutil
import subprocess
import sys

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__))))

CHECKS: list[tuple[str, bool, str]] = []

# What each part of the book first needs. Import name, and the part that needs it.
PACKAGES = [
    ("duckdb", "Part I"), ("pandas", "Part I"), ("pyarrow", "Part I"),
    ("httpx", "Part II"), ("openpyxl", "Part II"), ("pydantic", "Part II"),
    ("dbt", "Part III"), ("sqlglot", "Part IV"), ("fastapi", "Part V"),
]


def check(label: str, ok: bool, detail: str = "") -> bool:
    CHECKS.append((label, ok, detail))
    print(f"  {'PASS' if ok else 'FAIL'}  {label}" + (f"  — {detail}" if detail else ""))
    return ok


def main() -> int:
    print("\nEnvironment")
    check("Python 3.12 or newer", sys.version_info >= (3, 12),
          ".".join(map(str, sys.version_info[:3])))
    for package, part in PACKAGES:
        try:
            module = importlib.import_module(package)
            version = getattr(module, "__version__", "")
            if package == "dbt":
                from dbt.version import __version__ as version
            check(f"{package} installed", True, version)
        except ImportError:
            check(f"{package} installed", False,
                  f"needed from {part}: pip install -r requirements.txt")

    print("\nThe sources")
    from groundwork.config import BOOK2_WAREHOUSE, DATA, ROOT, SOURCES
    for label, path in (("Book 2's warehouse present", BOOK2_WAREHOUSE),
                        ("Meridian's sources present", SOURCES / "manifest.json"),
                        ("Tradeline's database present", SOURCES / "tradeline" / "tradeline.db")):
        check(label, path.exists(),
              str(path.relative_to(ROOT)) if path.exists() else "run: make data")
    free = shutil.disk_usage(ROOT).free / 1e9
    check("at least 1 GB of disk free", free >= 1.0, f"{free:.1f} GB")
    del DATA

    print("\nSecrets")
    env_file = ROOT / ".env"
    if env_file.exists():
        ignored = subprocess.run(["git", "check-ignore", ".env"], cwd=ROOT,
                                 capture_output=True).returncode == 0
        check(".env is ignored by git", ignored,
              "" if ignored else "add '.env' to .gitignore before doing anything else")
    token = os.getenv("FRONTLINE_TOKEN")
    if token is None and env_file.exists():
        token = next((line.split("=", 1)[1].strip() for line in env_file.read_text().splitlines()
                      if line.startswith("FRONTLINE_TOKEN=")), None)
    check("Frontline's token is set", bool(token),
          f"{len(token)} characters" if token else "copy .env.example to .env")

    failed = [label for label, ok, _ in CHECKS if not ok]
    print(f"\n  {len(CHECKS) - len(failed)}/{len(CHECKS)} checks passed")
    if failed:
        print(f"  Fix first: {failed[0]}")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
