# timeout: 900
# Every price list on disk through Chapter 9's reader, held to the
# contract: bad rows parked with a reason, broken files failed, the
# rest landed, and every row accounted for.
import json
import shutil
import sys
from pathlib import Path

import duckdb

from groundwork.config import LANDING, NOW, SANDBOX
from groundwork.ingest import files, suppliers
from groundwork.ingest.__main__ import land

sys.path.insert(0, str(Path(__file__).parent))
from _variants import first_contract  # noqa: E402

here = SANDBOX / "ch10" / "quarantine"
shutil.rmtree(here, ignore_errors=True)
day = NOW.date().replace(day=4)
con = duckdb.connect(str(here.parent / "quarantine.duckdb"))
x = land(con, "suppliers", "price_lists",
         files.discover(LANDING, "suppliers", by=NOW), day, NOW,
         suppliers.read, "suppliers", here / "raw", here / "landed",
         contract=first_contract())

runs = con.execute("""
    SELECT status, count(*), sum(rows), sum(parked)
    FROM audit.contract_runs GROUP BY 1 ORDER BY 1""").fetchall()
print(f"{x.files} lists, {x.rows:,} rows, checked against "
      f"version {first_contract().version} of the contract:")
for status, n, rows, parked in runs:
    print(f"  {status:<7} {n:>4} file{'s' * (n > 1)} {rows:>6,} rows"
          f" {parked:>2} parked")
broken = sum(r[2] for r in runs if r[0] == "broken")
print(f"landed {x.loaded:,} + parked {x.parked} + in broken files "
      f"{broken} = {x.loaded + x.parked + broken:,}")
(st,) = con.execute("SELECT status FROM audit.load_log WHERE "
                    "load_id = ?", [x.load_id]).fetchone()
print(f"load {x.load_id} {st}; the table: {x.status}")

print("\naudit.quarantine:")
for file, n, rule, reason, row in con.execute("""
    SELECT file, row_number, rule, reason, row_json
    FROM audit.quarantine ORDER BY file, row_number""").fetchall():
    print(f"  {file.split('/')[-1]}, row {n}: {rule}\n  {reason}")
    r = json.loads(row)
    print(f"  {r['sku']} {r['cost']} {r['effective_from']}")

print("\nthe file ledger's broken files:")
fails = con.execute("""
    SELECT name, note FROM audit.file_ledger
    WHERE status = 'failed' ORDER BY name""").fetchall()
print(f"  {len(fails)} failed, {fails[0][0].split('/')[-1]}\n"
      f"    to {fails[-1][0].split('/')[-1]}")
print("  " + fails[0][1].split(": ", 2)[2].replace(", found",
                                                  ",\n  found"))

Path(__file__).with_suffix(".json").write_text(json.dumps({
    "files": x.files, "rows": x.rows, "loaded": x.loaded,
    "parked": x.parked, "broken_rows": broken,
    "runs": {s: [n, r, p] for s, n, r, p in runs},
    "broken_files": len(fails), "quarantined": con.execute(
        "SELECT file, row_number, reason FROM audit.quarantine"
    ).fetchall()}, indent=1))
con.close()
shutil.rmtree(here)
(here.parent / "quarantine.duckdb").unlink()
