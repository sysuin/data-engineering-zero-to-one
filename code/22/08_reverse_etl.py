# The renewal call list sent back to Relay as tasks: two runs a month
# apart, one run repeated, and Relay's import done two ways.
# timeout: 300
import hashlib
import json
from datetime import date
from pathlib import Path

import duckdb

from groundwork.serve import reverse_etl

from _serve import HERE, tidy, warehouse

con = duckdb.connect(str(warehouse()), read_only=True)
outbox = HERE / "outbox" / "relay"
tidy("outbox")
runs = [date(2025, 12, 1), date(2026, 1, 5), date(2026, 1, 5)]
files = []
for day in runs:
    path = reverse_etl.export(con, day, outbox)
    rows = reverse_etl.read(path)
    files.append(rows)
    digest = hashlib.sha256(path.read_bytes()).hexdigest()[:12]
    print(f"{day}  {path.name}  open "
          f"{sum(r['status'] == 'open' for r in rows):>2}  closed "
          f"{sum(r['status'] == 'closed' for r in rows):>2}  {digest}")
before = {r["external_id"] for r in files[0]}
after = {r["external_id"] for r in files[1] if r["status"] == "open"}
print(f"  on both lists {len(before & after)}, new "
      f"{len(after - before)}, dropped {len(before - after)}")
contract = {r["external_id"]: int(r["note"].split()[1])
            for r in files[0]}          # "contract N ends ..."
why = {}
for eid in sorted(before - after):
    end, owner = con.execute("""
        SELECT c.end_date, a.owner FROM marts.fct_contracts AS c
        JOIN marts.dim_account AS k USING (account_key)
        JOIN marts.dim_account AS a ON a.account_id = k.account_id
         AND a.is_current
        WHERE c.contract_id = ?""", [contract[eid]]).fetchone()
    reason = ("contract ended" if end < runs[1] else
              "handed to the Retention desk"
              if owner == "Retention desk" else "fell below the top 40")
    why[reason] = why.get(reason, 0) + 1
print("  dropped because: " + ", ".join(
    f"{k} {v}" for k, v in sorted(why.items())))
print("one task, as Relay's import reads it:")
for k, v in files[1][0].items():
    print(f"  {k:<12}{v}")


def import_by_id(tasks, rows):
    """Relay's import keyed on external_id: insert or update."""
    for r in rows:
        tasks[r["external_id"]] = r


def import_appending(tasks, rows):
    """The same file read by an import with no key: every row new."""
    for r in rows:
        tasks[len(tasks)] = r


relay = {}
for name, load in (("keyed on external_id", import_by_id),
                   ("appending every row", import_appending)):
    tasks = {}
    for rows in files:
        load(tasks, rows)
    open_ = sum(t["status"] == "open" for t in tasks.values())
    print(f"Relay after three imports, {name}: {len(tasks)} tasks, "
          f"{open_} open")
    relay[name] = [len(tasks), open_]
Path(__file__).with_suffix(".json").write_text(json.dumps(
    {"header": list(files[1][0]), "row": list(files[1][0].values()),
     "files": [[len(f), sum(r["status"] == "open" for r in f)]
               for f in files],
     "relay": relay}))
con.close()
tidy("warehouse.duckdb", "outbox")
