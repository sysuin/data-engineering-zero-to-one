"""
Reverse ETL: the warehouse's renewal call list, sent back to Relay as
tasks for the account team, in the CSV that Relay's task import reads.

The list is the contracts whose 90-day mark has passed and that have
not ended, on accounts still active and not yet handed to the
Retention desk, longest since their last order first. Each task is
assigned to the account's manager as Relay had it last night: for a
call made today, today's manager is the right one.

The export is idempotent, and so is the import it feeds:

  * every task has an external_id minted from its contract, so Relay
    updates a task it already has instead of adding a second one;
  * a run's file is computed from the warehouse and the previous
    run's file only, so running a day again writes the same bytes;
  * a task that has left the list is sent once more, as closed, so
    Relay never keeps a call nobody should make.

    call_list(con, day, size=40)           the list, as of a day
    export(con, day, outbox, size=40)      write that day's file
    read(path)                             a file's rows
"""
from __future__ import annotations

import csv
import hashlib
import io
import os
from datetime import date, timedelta
from pathlib import Path

FIELDS = ("external_id", "account_id", "assign_to", "subject",
          "due_date", "status", "note")

CALL_LIST = """
WITH v AS (                       -- each account as Relay had it
    SELECT * FROM marts.dim_account
    WHERE ? - INTERVAL 1 DAY >= valid_from
      AND ? - INTERVAL 1 DAY < coalesce(valid_to, DATE '9999-12-31')
),
last AS (
    SELECT a.account_id, max(o.order_date) AS last_order
    FROM marts.fct_orders AS o
    JOIN marts.dim_account AS a USING (account_key)
    WHERE o.order_date < ?
    GROUP BY a.account_id
)
SELECT c.contract_id, v.account_id, v.account_name, v.owner,
       c.end_date, date_diff('day', l.last_order, ?) AS idle_days
FROM marts.fct_contracts AS c
JOIN marts.dim_account AS k USING (account_key)
JOIN v ON v.account_id = k.account_id
LEFT JOIN last AS l ON l.account_id = v.account_id
WHERE c.end_date >= ? AND c.end_date - INTERVAL 90 DAY <= ?
  AND v.status = 'active' AND v.owner <> 'Retention desk'
ORDER BY idle_days DESC NULLS FIRST, c.contract_id
LIMIT ?
"""


def external_id(contract_id: int) -> str:
    """The task's id in Relay: the same contract, the same task."""
    text = f"renewal-call|{contract_id}".encode()
    return hashlib.md5(text).hexdigest()[:16]


def call_list(con, day: date, size: int = 40) -> list[dict]:
    """The calls to make, as the warehouse knew things on `day`."""
    rows = con.execute(CALL_LIST, [day, day, day, day, day, day,
                                   size]).fetchall()
    return [{"external_id": external_id(cid), "account_id": acct,
             "assign_to": owner, "status": "open",
             "subject": f"Renewal call: {name}",
             "due_date": str(day + timedelta(days=7)),
             "note": (f"contract {cid} ends {end}; "
                      + (f"no order for {idle} days" if idle
                         is not None else "no order on record"))}
            for cid, acct, name, owner, end, idle in rows]


def read(path: Path) -> list[dict]:
    with open(path, newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def _previous(outbox: Path, day: date) -> list[dict]:
    """The last file written for a day before `day`, if any."""
    earlier = sorted(p for p in outbox.glob("tasks_*.csv")
                     if p.stem.removeprefix("tasks_") < str(day))
    return read(earlier[-1]) if earlier else []


def export(con, day: date, outbox: Path, size: int = 40) -> Path:
    """Write tasks_<day>.csv: today's list, open, and every task the
    previous file had open that is no longer on it, closed."""
    rows = call_list(con, day, size)
    on_list = {r["external_id"] for r in rows}
    for old in _previous(outbox, day):
        gone = old["external_id"] not in on_list
        if old["status"] == "open" and gone:
            rows.append({**old, "status": "closed",
                         "note": f"off the list on {day}"})
    rows.sort(key=lambda r: (r["status"] != "open", r["external_id"]))
    buf = io.StringIO()
    out = csv.DictWriter(buf, FIELDS, lineterminator="\n")
    out.writeheader()
    out.writerows(rows)
    outbox.mkdir(parents=True, exist_ok=True)
    path = outbox / f"tasks_{day}.csv"
    tmp = path.with_suffix(".tmp")
    tmp.write_text(buf.getvalue(), encoding="utf-8")
    os.replace(tmp, path)            # all of the file, or none of it
    return path
