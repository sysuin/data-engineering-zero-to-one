# The crontab, replayed over every night it would have run in 2025,
# against when Tradeline's 02:00 job committed and Relay's export came.
import json
import sqlite3
from datetime import date, datetime, time, timedelta
from pathlib import Path

from groundwork.config import ROOT
from meridian.sources import relay, tradeline

CRONTAB = ROOT / "code" / "17" / "nightly.crontab"
steps = {}
for line in CRONTAB.read_text().splitlines():
    if line.strip() and not line.startswith("#"):
        m, h, *_, cmd = line.split(maxsplit=6)
        steps[cmd.removeprefix("gw ")] = time(int(h), int(m))

# Each run is for yesterday: the run on 1 January 2025 loads 31 Dec.
runs = [date(2025, 1, 1) + timedelta(days=k) for k in range(365)]
days = [r - timedelta(days=1) for r in runs]

# When the orders team's job finished: the last commit of the rows it
# stamped 02:00:00 that night (the stand-in keeps every commit time).
hist = sqlite3.connect(f"file:{tradeline.HISTORY}?mode=ro", uri=True)
job = dict(hist.execute("""
    SELECT date(updated_at, 'unixepoch'),
           time(max(committed_at), 'unixepoch')
    FROM (SELECT updated_at, committed_at FROM orders_v
          UNION ALL SELECT updated_at, committed_at FROM order_lines_v)
    WHERE updated_at % 86400 = 7200 AND committed_at > updated_at
    GROUP BY 1""").fetchall())
# When each night's export was written (and so arrived), or None.
crm = sqlite3.connect(f"file:{relay.STORE}?mode=ro", uri=True)
export = {n: (at if done else None) for n, done, at in crm.execute(
    "SELECT night, exported, exported_at FROM nights")}

nights = []
for run, day in zip(runs, days):
    commit = time.fromisoformat(job[run.isoformat()])
    came = export[day.isoformat()]
    came = datetime.fromisoformat(came) if came else None
    relay_at = datetime.combine(run, steps["ingest --only relay"])
    build_at = datetime.combine(run, steps["dbt build"])
    nights.append({
        "day": day.isoformat(), "commit": commit.isoformat(),
        "export": came and came.isoformat(sep=" "),
        "overlap": commit > steps["ingest --only tradeline"],
        "no_relay": came is None or came > relay_at,
        "build_first": came is None or came > build_at})

first, last = min(steps.values()), max(steps.values())
print(f"{CRONTAB.name}: {len(steps)} steps, {first:%H:%M} to "
      f"{last:%H:%M} UTC")
print(f"replayed over the {len(runs)} runs due in 2025, "
      f"{days[0]} to {days[-1]}\n")
n = sum(x["overlap"] for x in nights)
print(f"ingest --only tradeline at "
      f"{steps['ingest --only tradeline']:%H:%M}")
print(f"  the 02:00 job still committing    {n:>4} nights")
latest = max(job[r.isoformat()] for r in runs)
print(f"  its latest commit             {latest:>8}")
print(f"ingest --only relay at {steps['ingest --only relay']:%H:%M}")
for hour in (3, 4):
    k = sum(1 for x in nights if x["export"]
            and int(x["export"][11:13]) == hour)
    print(f"  export arrived at 0{hour}:00          {k:>4} nights")
k = sum(1 for x in nights if x["export"] is None)
print(f"  no export at all                  {k:>4} nights")
k = sum(x["no_relay"] for x in nights)
print(f"  found nothing to load             {k:>4} nights")
k = sum(x["no_relay"] and x["build_first"] for x in nights)
print(f"dbt build at {steps['dbt build']:%H:%M}, publish at "
      f"{steps['publish']:%H:%M}, ran anyway {k:>4} nights")
ok = sum(not x["overlap"] and not x["no_relay"] for x in nights)
print(f"nights every step's timing held     {ok:>4}")

# The change of clocks, where the Relay step started missing
flip = [x for a, x in zip(nights, nights[1:])
        if a["export"] and x["export"]
        and a["no_relay"] != x["no_relay"]]
for x in flip:
    print(f"  from {x['day']}: export at {x['export'][11:16]}, step "
          f"{'misses' if x['no_relay'] else 'finds'} it")

Path(__file__).with_suffix(".json").write_text(json.dumps(
    {"steps": {k: v.isoformat() for k, v in steps.items()},
     "nights": nights}, indent=0))
