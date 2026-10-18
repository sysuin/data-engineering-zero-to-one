# The portal's hourly files and the ledger's monthly journals, counted.
from datetime import datetime, timezone

import duckdb

from groundwork.config import LANDING


def utc(path) -> str:
    t = datetime.fromtimestamp(path.stat().st_mtime, timezone.utc)
    return f"{t:%Y-%m-%d %H:%M} UTC"


portal = sorted((LANDING / "portal").rglob("*.jsonl.gz"))
size = sum(p.stat().st_size for p in portal)
print(f"portal: {len(portal):,} hourly files, {size / 1e6:.1f} MB")
print(f"  the first: {portal[0].relative_to(LANDING).as_posix()}")

ledger = sorted((LANDING / "ledger").glob("gl_revenue_*.csv"))
print(f"\nledger: {len(ledger)} monthly files, "
      f"{ledger[0].stem[-7:]} to {ledger[-1].stem[-7:]}")
print(f"  the newest, {ledger[-1].name}, arrived {utc(ledger[-1])}")
con = duckdb.connect()
cols = [c for (c, *_) in con.execute(
    f"DESCRIBE SELECT * FROM read_csv('{ledger[-1]}')").fetchall()]
print(f"  columns {','.join(cols)}")
