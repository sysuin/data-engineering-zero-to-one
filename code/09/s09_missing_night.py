# The Relay fortnight loaded with one night's folder left out, and an
# alert that names every night with no accounts export in force.
import shutil
from datetime import date, timedelta

import duckdb

from groundwork import loadlog
from groundwork.config import LANDING, SANDBOX
from groundwork.ingest import files, relay

here = SANDBOX / "ch09" / "s_night"
shutil.rmtree(here, ignore_errors=True)
shutil.copytree(LANDING / "relay", here / "landing" / "relay",
                ignore=shutil.ignore_patterns("2025-12-28"))
where = dict(landing=here / "landing", root=here / "raw",
             warehouse=here / "w.duckdb", archive=here / "landed")
night = date(2025, 12, 22)
while night <= date(2026, 1, 4):
    try:
        relay.load(night, **where)
    except files.MissingFile:
        pass
    night += timedelta(days=1)


def missing_night_alert(con, today, days=14):
    """One line per recent night with no accounts export in force."""
    first = today - timedelta(days=days)
    lines = []
    for n in relay.missing_nights(con, first, today - timedelta(1)):
        errors = [x.extra.get("error", "never attempted")
                  for x in loadlog.history(con, "relay", "accounts")
                  if x.load_date == n and x.status == "failed"]
        why = errors[-1].split(": ", 1)[-1] if errors else \
            "never attempted"
        lines += [f"ALERT relay {n}:", f"  {why}"]
    return lines


con = duckdb.connect(str(where["warehouse"]))
for line in missing_night_alert(con, date(2026, 1, 5)):
    print(line)
con.close()
shutil.rmtree(here)
