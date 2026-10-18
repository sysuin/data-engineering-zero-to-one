# A quarter of portal events, counted all in memory and then streamed.
import gzip
import json
import tracemalloc
from collections import Counter
from pathlib import Path

from groundwork.config import LANDING

PORTAL = LANDING / "portal"
MONTHS = ["2025/07", "2025/08", "2025/09"]    # its first quarter


def files():
    """Every hourly file of the quarter, in order."""
    for month in MONTHS:
        yield from sorted((PORTAL / month).glob("*/*.jsonl.gz"))


def lines(paths):
    """Each line of each file, one at a time."""
    for path in paths:
        with gzip.open(path, "rt", encoding="utf-8") as f:
            yield from f


def events(texts):
    """Each line parsed into a dict."""
    for text in texts:
        yield json.loads(text)


def in_memory():
    everything = [json.loads(t) for t in lines(files())]  # all at once
    return Counter(e["event_type"] for e in everything)


def streamed():
    counts = Counter()
    for event in events(lines(files())):              # one at a time
        counts[event["event_type"]] += 1
    return counts


print(f"{sum(1 for _ in files()):,} hourly files\n")
print(f"{'method':10} {'events':>9} {'peak memory':>12}")
saved = {}
for name, method in [("in memory", in_memory), ("streamed", streamed)]:
    tracemalloc.start()
    counts = method()
    peak = tracemalloc.get_traced_memory()[1]      # bytes, at most
    tracemalloc.stop()
    print(f"{name:10} {sum(counts.values()):>9,} {peak / 1e6:>9.1f} MB")
    saved[name] = {"events": sum(counts.values()),
                   "peak_mb": round(peak / 1e6, 1)}

print()
for event_type, n in sorted(counts.items(), key=lambda kv: -kv[1]):
    print(f"  {event_type:17} {n:>8,}")

# The numbers above, for the chapter's figure.
Path(__file__).with_suffix(".json").write_text(json.dumps(saved))
