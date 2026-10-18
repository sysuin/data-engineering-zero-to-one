# Partition pruning: asking raw for Relay's latest night of accounts.
import json
from pathlib import Path

from _plans import analyze, connect, name, scans

from groundwork.config import RAW

con = connect()
sizes = {p.parent.name[len("load_date="):]: p.stat().st_size
         for p in (RAW / "relay" / "accounts").glob("*/part-0.parquet")}
LATEST = max(sizes)
QUERIES = {
    "every night": "SELECT count(*) FROM raw.relay_accounts",
    "one night, a constant":
        "SELECT count(*) FROM raw.relay_accounts "
        f"WHERE load_date = DATE '{LATEST}'",
    "latest, from itself":
        "SELECT count(*) FROM staging.stg_relay__accounts "
        "WHERE snapshot_date = (SELECT max(snapshot_date) "
        "FROM staging.stg_relay__accounts)",
    "latest, from the ledger":
        "SELECT count(*) FROM staging.stg_relay__accounts "
        "WHERE snapshot_date = (SELECT max(load_date) "
        "FROM audit.current_files WHERE source = 'relay' "
        "AND table_name = 'accounts')",
}
print(f"raw.relay_accounts: {len(sizes)} nights, "
      f"{sum(sizes.values()):,} bytes")
print(f"{'query':<25}{'files opened':>13}{'bytes':>12}"
      f"{'rows read':>11}")
pruning = {}
for label, sql in QUERIES.items():
    files = rows = size = 0
    for scan in scans(analyze(con, sql)):
        if name(scan) != "READ_PARQUET":
            continue
        info = scan["extra_info"]
        opened = int(info.get("Total Files Read",
                              info.get("Scanning Files", "0/0")
                              .split("/")[0]))
        files += opened
        size += (sum(sizes.values()) if opened == len(sizes)
                 else opened * sizes[LATEST])
        rows += scan["operator_cardinality"]
    pruning[label] = size
    print(f"{label:<25}{files:>13}{size:>12,}{rows:>11,}")
(Path(__file__).parent / "07_pruning.json").write_text(
    json.dumps(pruning, indent=1) + "\n")
