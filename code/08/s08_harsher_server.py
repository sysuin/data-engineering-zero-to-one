# Exercise 2: a harsher desk, the fewest attempts that survive it, and a
# retry budget for the whole run.
import shutil
from datetime import datetime

import duckdb

from meridian.sources import Clock, frontline
from groundwork import loadlog
from groundwork.config import SANDBOX
from groundwork.ingest.frontline import Client, FrontlineError, load

work = SANDBOX / "ch08" / "s08_harsher"
RUN = datetime(2025, 12, 3, 6, 0)
START = "2025-12-01T00:00:00Z"


def fresh():
    shutil.rmtree(work, ignore_errors=True)
    work.mkdir(parents=True)
    return {"root": work / "raw", "warehouse": work / "wh.duckdb"}


def night(client, where):
    return load("2025-12-02", run_at=RUN, client=client, start=START,
                **where)


# 1. rate_limit=3, fail_every=4, the client's defaults (real sleeps).
base = frontline.serve(Clock(RUN), rate_limit=3, fail_every=4)
x = night(Client(base), fresh())
print(f"429 every 3rd, 503 every 4th: {x.status}, "
      f"{x.extra['requests']} requests, {x.extra['retries']} retried")

# 2. and 503 on every 2nd as well: how many attempts are enough?
print("429 every 3rd, 503 every 2nd:")
for attempts in range(1, 7):
    base = frontline.serve(Clock(RUN), rate_limit=3, fail_every=2)
    c = Client(base, attempts=attempts, sleep=lambda s: None)
    try:
        x = night(c, fresh())
        print(f"  {attempts} attempts: succeeded, "
              f"{x.extra['requests']} requests")
        break
    except FrontlineError as e:
        print(f"  {attempts} attempts: {e}")


class Budgeted(Client):
    """A client that may spend at most `budget` seconds waiting."""

    def __init__(self, *a, budget: float, **kw):
        super().__init__(*a, **kw)
        self.budget, self.spent = budget, 0.0

    def wait(self, n, r):
        w = super().wait(n, r)
        if self.spent + w > self.budget:
            raise FrontlineError(f"budget of {self.budget:g} s: "
                                 f"{self.spent:.2f} s spent")
        self.spent += w
        return w


# 3. a run-wide budget of half a second; a desk failing every 2nd.
where = fresh()
base = frontline.serve(Clock(RUN), fail_every=2)
try:
    night(Budgeted(base, budget=0.5, sleep=lambda s: None), where)
except FrontlineError as e:
    print(f"budgeted run: {e}")
con = duckdb.connect(str(where["warehouse"]))
(x,) = loadlog.history(con, "frontline", "tickets")
print(f"load log: {x.load_id} {x.status}")
print(f"  {x.extra['error']}")
con.close()
shutil.rmtree(work)
