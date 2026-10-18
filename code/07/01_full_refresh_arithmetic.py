# What a nightly full refresh copies, set beside what changed each
# night of December 2025, and the same sums for bigger businesses.
from datetime import datetime, timedelta

from meridian.sources import tradeline

RATE = 450_000               # rows a second, about Chapter 6's rate
src = tradeline.connect()
rows = sum(src.execute(f"SELECT count(*) FROM {t}").fetchone()[0]
           for t in ("orders", "order_lines"))
changed = {}
for day in range(2, 32):                  # the extracts of 2..31 Dec
    end = datetime(2025, 12, day, 2, 5)
    since = end - timedelta(days=1)
    changed[end.date()] = sum(src.execute(
        f"SELECT count(*) FROM {t} WHERE updated_at >= ? AND "
        "updated_at < ?", (str(since), str(end))).fetchone()[0]
        for t in ("orders", "order_lines"))
mean = sum(changed.values()) / len(changed)
busy = max(changed, key=changed.get)
print(f"Tradeline today: {rows:,} rows in orders and order_lines")
print("changed in a night, December 2025, by updated_at:")
print(f"  average {mean:,.0f}; busiest {changed[busy]:,} "
      f"({busy:%d %b}); quietest {min(changed.values()):,}")
print(f"a full refresh copies {rows / mean:,.0f} times the average")


def took(n):
    """Seconds, minutes or hours, whichever reads best."""
    s = n / RATE
    if s < 90:
        return f"{s:.3f} s" if s < 1 else f"{s:.1f} s"
    return f"{s / 60:.0f} min" if s < 5400 else f"{s / 3600:.1f} h"


print(f"\nat {RATE:,} rows a second:")
print(f"{'business':<18} {'full refresh':>14} {'changes only':>14}")
for scale in (1, 100, 1_000, 10_000):
    name = "Meridian" if scale == 1 else f"Meridian x {scale:,}"
    print(f"{name:<18} {took(rows * scale):>14} "
          f"{took(mean * scale):>14}")
