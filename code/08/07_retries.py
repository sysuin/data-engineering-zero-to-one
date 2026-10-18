# Groundwork's client against a failing desk: retry, give up, or stop.
import json
import random
from pathlib import Path

from meridian.sources import Clock, frontline
from groundwork.config import SEED
from groundwork.ingest.frontline import Client, FrontlineError, pull

AT = Clock("2025-12-08 06:00")
SINCE = "2025-12-01T00:00:00Z"


def no_sleep(seconds):
    """Record the waits, don't sleep them: the faults are counted."""


def show(client):
    """One line per request: each attempt's answer, and the wait."""
    lines = []
    for n, outcome, wait in client.log:
        if n == 1:
            lines.append([])
        lines[-1].append(outcome + (f" (wait {wait:.2f} s)" if wait
                                    else ""))
    for i, tries in enumerate(lines, start=1):
        sep = ",\n" + " " * 15 if len(tries) > 2 else ", "
        print(f"    request {i}: " + sep.join(tries))


print("1. every 3rd request fails with 503:")
c = Client(frontline.serve(AT, fail_every=3), sleep=no_sleep)
pages = list(pull(c, SINCE, limit=50))
print(f"  {len(pages)} pages in {len(c.log)} attempts:")
show(c)

print("2. every request fails:")
c = Client(frontline.serve(AT, fail_every=1), sleep=no_sleep)
try:
    list(pull(c, SINCE, limit=50))
except FrontlineError as e:
    print(f"  FrontlineError: {e}")
show(c)

print("3. the wrong token:")
c = Client(frontline.serve(AT, fail_every=3), secret="expired",
           sleep=no_sleep)
try:
    list(pull(c, SINCE, limit=50))
except FrontlineError as e:
    print(f"  FrontlineError: {e}")
show(c)

# Five clients retrying one outage, with and without jitter.
waits = {}
for k in range(5):
    c = Client(frontline.serve(AT, fail_every=1), sleep=no_sleep,
               rng=random.Random(SEED + k))
    try:
        list(pull(c, SINCE))
    except FrontlineError:
        waits[f"client {k + 1}"] = [w for *_, w in c.log if w]
plain = [min(c.cap, c.first_wait * 2 ** n) for n in range(5)]
Path(__file__).with_suffix(".json").write_text(json.dumps(
    {"jittered": waits, "plain": plain}, indent=1) + "\n")
print("without jitter every client waits "
      + ", ".join(f"{w:g}" for w in plain) + " s")
