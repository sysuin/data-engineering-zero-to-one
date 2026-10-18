# groundwork.diff on the opening's two builds: every staging view and
# every table, main's against the branch's.
# timeout: 900
import json
import time
from dataclasses import asdict

from _sandbox import CHANGE, built

from groundwork import diff

main, _ = built("main")
branch, _ = built("branch", CHANGE)
start = time.perf_counter()
diffs = diff.compare(main, branch)
seconds = time.perf_counter() - start
print("main's build (A) against the branch's (B):\n")
print(diff.report(diffs))
rows = sum(d.rows_a for d in diffs)
print(f"\nrows compared in A: {rows:,}; in under five minutes: "
      f"{seconds < 300}")
with open("code/23/05_data_diff.json", "w") as f:
    json.dump({"tables": [asdict(d) for d in diffs],
               "rows": rows}, f, indent=1, default=str)
