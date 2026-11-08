# timeout: 1800
# The freshness SLO and its error budget: what the sources alone
# spend over four years, and what July 2024 spent, watched or not.
from datetime import date, timedelta

import duckdb
from _sandbox import copy_of, morning, replay, tidy

from groundwork.config import FIRST_DAY, LAST_DAY
from groundwork.observe import alerts
from meridian.sources import relay



def spans(days):
    """Consecutive days as '2 Jul to 15 Jul', others as '4 Jul'."""
    out, run = [], []
    for d in days:
        if run and (d - run[-1]).days != 1:
            out.append(run)
            run = []
        run.append(d)
    out += [run] if run else []
    return ", ".join(f"{r[0].day} Jul" if len(r) == 1 else
                     f"{r[0].day} Jul to {r[-1].day} Jul" for r in out)


slo = alerts.FRESHNESS
print(f"SLO: {slo.name}, on {slo.target:.0%} of days in any "
      f"{slo.window}")
print(f"  error budget: {slo.budget:g} late days a window")

# The nightly DAG cannot publish a night Relay sent nothing for.
none = {date.fromisoformat(n) for n, sent, _ in relay.nights()
        if not sent}
days = [FIRST_DAY + timedelta(days=i)
        for i in range((LAST_DAY - FIRST_DAY).days + 1)]
late = [d for d in days if d in none]
window = [sum(1 for x in late if 0 <= (d - x).days < slo.window)
          for d in days]
print(f"\n{FIRST_DAY} to {LAST_DAY}: {len(days):,} days, "
      f"{len(late)} with no Relay export")
print(f"  most in any {slo.window} days: {max(window)}; budget spent "
      f"on {sum(n > slo.budget for n in window)} days")

print("\nJuly 2024, the window ending on the 31st:")
spent = {}
for name in ("unwatched", "watched"):
    replay(name)
    con = duckdb.connect(str(copy_of(name, "07")), read_only=True)
    b = alerts.budget(con, morning(date(2024, 7, 31)))
    bad = [d for d, ok in alerts.days(con, b.first, b.last) if not ok]
    out = next((d for d in bad if alerts.budget(
        con, morning(d)).left < 0), None)
    con.close()
    spent[name] = out
    print(f"  {name:<10} {b.good} on time, {b.bad} late, "
          f"{b.met:.1%}; budget left {b.left:g}")
    print(f"             late: {spans(bad)}")
    print(f"             spent on the morning after {out.day} Jul"
          if out else "             not spent")
tidy("07")
