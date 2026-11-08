# timeout: 1800
# Exercise 5: the freshness SLO at several targets over 30 days, for
# the window ending 31 July 2024 in both replays.
from dataclasses import replace
from datetime import date

import duckdb
from _sandbox import morning, replay

from groundwork.observe import alerts

targets = (0.90, 0.93, 0.95, 0.97, 0.99)
print(f"{'target':>7}{'budget':>8}{'watched left':>14}"
      f"{'unwatched left':>16}")
left = {}
for name in ("watched", "unwatched"):
    con = duckdb.connect(str(replay(name) / "warehouse.duckdb"),
                         read_only=True)
    for t in targets:
        slo = replace(alerts.FRESHNESS, target=t)
        left[name, t] = alerts.budget(con, morning(date(2024, 7, 31)),
                                      slo).left
    con.close()
for t in targets:
    b = replace(alerts.FRESHNESS, target=t).budget
    print(f"{t:>7.0%}{b:>8g}{left['watched', t]:>14g}"
          f"{left['unwatched', t]:>16g}")
