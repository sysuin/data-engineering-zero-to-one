# Two volume checks over four years of Tradeline's new orders a day:
# how often each cries wolf, and what each catches when a night is
# short. Every one of these nights was delivered whole.
import statistics

from groundwork import quality
from groundwork.config import TRADELINE_DB
from meridian.sources import relay

from _sandbox import connect, save

con = connect(":memory:", read_only=False)
con.execute(f"ATTACH '{TRADELINE_DB}' AS t (TYPE sqlite, READ_ONLY)")
series = con.execute("""
    SELECT CAST(substr(created_at, 1, 10) AS DATE) AS day, count(*)
    FROM t.orders WHERE created_at < '2026-01-01'
    GROUP BY 1 ORDER BY 1""").fetchall()
values = [n for _, n in series]
W = 28


def textbook(past, v):
    """Mean plus or minus three standard deviations."""
    m, sd = statistics.mean(past), statistics.pstdev(past)
    return not m - 3 * sd <= v <= m + 3 * sd


def groundwork(past, v):
    """quality.anomalies: median plus or minus 4 MADs, floor 5%."""
    days = [(i, x) for i, x in enumerate(past + [v])]
    return quality.anomalies(days, window=W, history=len(past))[0].alarm


checks = {"mean +/- 3 sd": textbook, "median +/- 4 MAD": groundwork}
alarms = {name: [series[i][0] for i in range(W, len(values))
                 if f(values[i - W:i], values[i])]
          for name, f in checks.items()}
years = (len(values) - W) / 365.25
print(f"{series[0][0]} to {series[-1][0]}: {len(values):,} nights, "
      f"{min(values)} to {max(values)} orders a night")
print(f"{'':<18}{'alarms':>7}{'a year':>8}   first three")
for name, days in alarms.items():
    print(f"{name:<18}{len(days):>7}{len(days) / years:>8.1f}   "
          + ", ".join(f"{d:%d %b %y}" for d in days[:3]))

points = quality.anomalies(series, window=W, history=W)
print("median +/- 4 MAD, every alarm:")
for pt in points:
    if pt.alarm:
        print(f"  {pt.day:%a %d %b %Y}  {pt.value:>4.0f} orders, "
              f"band {pt.low:.0f} to {pt.high:.0f}")

print(f"\nnights short by p%, caught by each ({len(values) - W:,} "
      "nights each)")
print(f"{'p':>5}" + "".join(f"{n:>20}" for n in checks))
for p in (2, 10, 25, 50, 100):
    row = []
    for f in checks.values():
        hit = sum(f(values[i - W:i], values[i] * (100 - p) / 100)
                  for i in range(W, len(values)))
        row.append(f"{hit / (len(values) - W):>20.1%}")
    print(f"{p:>4}%" + "".join(row))

# One empty night, then a night half delivered one to 27 nights later.
caught = {name: 0 for name in checks}
trials = 0
for i in range(W, len(values) - W, 7):
    hole = values.copy()
    hole[i] = 0
    for k in range(1, W):
        trials += 1
        for name, f in checks.items():
            j = i + k
            caught[name] += f(hole[j - W:j], hole[j] * 0.5)
print(f"\nhalf a night, within four weeks of an empty one "
      f"({trials:,} trials)")
for name, n in caught.items():
    print(f"  {name:<18}{n / trials:>7.1%} caught")

nights = relay.nights()
missing = [n for n, e, _ in nights if not e]
save("07_anomalies", {
    "series": [[str(d), n] for d, n in series],
    "band": [[str(p.day), p.low, p.high] for p in points],
    "alarms": {k: [str(d) for d in v] for k, v in alarms.items()},
    "relay_missing": missing, "relay_nights": len(nights)})
