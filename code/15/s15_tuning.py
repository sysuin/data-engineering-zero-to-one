# Exercise 3: the robust band's two settings, the window and k, against
# false alarms and the share of 25%-short nights caught.
import statistics

from groundwork import quality
from groundwork.config import TRADELINE_DB

from _sandbox import connect

con = connect(":memory:", read_only=False)
con.execute(f"ATTACH '{TRADELINE_DB}' AS t (TYPE sqlite, READ_ONLY)")
values = [n for (n,) in con.execute("""
    SELECT count(*) FROM t.orders WHERE created_at < '2026-01-01'
    GROUP BY substr(created_at, 1, 10)
    ORDER BY substr(created_at, 1, 10)""").fetchall()]
START = 56                       # judge the same nights in every row


def alarm(past, v, k, floor=0.05):
    mid = statistics.median(past)
    mad = statistics.median(abs(x - mid) for x in past) * 1.4826
    half = max(k * mad, floor * mid)
    return not mid - half <= v <= mid + half


nights = len(values) - START
print(f"{'window':>6}{'k':>5}{'false alarms':>14}"
      f"{'25% short caught':>18}")
for window in (7, 28, 56):
    for k in (3, 4, 5):
        false = sum(alarm(values[i - window:i], values[i], k)
                    for i in range(START, len(values)))
        caught = sum(alarm(values[i - window:i], values[i] * 0.75, k)
                     for i in range(START, len(values)))
        print(f"{window:>6}{k:>5}{false:>14}{caught / nights:>18.1%}")
# The module's own function gives the same count for 28 and 4.
series = list(enumerate(values))
pts = quality.anomalies(series, window=28, k=4.0, history=28)
print("quality.anomalies, 28 and 4, from night 56:",
      sum(p.alarm for p in pts if p.day >= START))
