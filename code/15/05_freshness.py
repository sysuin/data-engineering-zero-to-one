# Freshness from the load log: this morning as it is, the same morning
# with three loads gone wrong, and four years of Relay's real nights.
from datetime import date, datetime, timedelta

import duckdb

from groundwork import loadlog, quality
from meridian.sources import relay

from _sandbox import connect, tidy, work

AT = datetime(2026, 1, 5, 7, 0)            # the run's hour is up
path = work("freshness")
con = connect(path, read_only=False)
ok = quality.freshness(con, AT)
print(f"{AT:%a %d %b %H:%M}: {sum(c.status == 'pass' for c in ok)} "
      f"of {len(ok)} fresh")

# The same morning, three ways a night goes wrong.
con.execute("""DELETE FROM audit.load_log WHERE load_date = '2026-01-04'
               AND (source IN ('tradeline', 'frontline')
                    OR (source, table_name) = ('relay', 'accounts'))""")
night = loadlog.start(con, "relay", "accounts", "2026-01-04",
                      started_at=AT - timedelta(hours=1))
loadlog.fail(con, night, finished_at=AT - timedelta(minutes=59),
             error="MissingFile: no accounts.csv for 2026-01-04")
loadlog.start(con, "frontline", "tickets", "2026-01-04",
              started_at=AT - timedelta(hours=1))    # never finished
print("\n".join(quality.report(quality.freshness(con, AT)[:5])))

for at in (datetime(2026, 1, 6, 9), datetime(2026, 1, 8, 9)):
    (ledger,) = [c for c in quality.freshness(con, at)
                 if c.source == "ledger"]
    print(f"{at:%a %d %b}: ledger {ledger.status}, {ledger.detail}, "
          f"tell {ledger.owner}")
con.close()
tidy("freshness")

# Every night Relay ever had, replayed through the load log: a load
# that succeeded when the export was there, one that failed when not;
# then freshness for relay.accounts at 07:00 the next morning.
mem = duckdb.connect()
loadlog.ensure(mem)
nights = relay.nights()
alarms = []
for n, exported, _ in nights:
    day = date.fromisoformat(n)
    run = datetime.combine(day + timedelta(days=1), datetime.min.time())
    load = loadlog.start(mem, "relay", "accounts", day,
                         started_at=run + timedelta(hours=6))
    if exported:
        loadlog.finish(mem, load, finished_at=run + timedelta(hours=6),
                       rows_written=1)
    else:
        loadlog.fail(mem, load, finished_at=run + timedelta(hours=6),
                     error="MissingFile")
    (c,) = [c for c in quality.freshness(mem, run + timedelta(hours=7))
            if c.name == "relay.accounts"]
    if c.status != "pass":
        alarms.append(day)
missing = [date.fromisoformat(n) for n, e, _ in nights if not e]
print(f"\nRelay, {nights[0][0]} to {nights[-1][0]}: {len(nights):,} "
      f"nights, {len(missing)} with no export")
print(f"freshness alarms: {len(alarms)}; on exactly those nights: "
      f"{alarms == missing}")
print("each cleared the next morning:",
      all(d + timedelta(days=1) not in alarms for d in alarms))
