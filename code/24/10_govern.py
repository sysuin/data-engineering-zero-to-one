# The Build: governance's two commands, run on a copy of Groundwork.
# timeout: 900
from groundwork.govern.__main__ import main

from _govern import tidy, work

copy = work("build")
paths = ["--warehouse", str(copy.warehouse), "--raw", str(copy.raw),
         "--landed", str(copy.landed)]
print("$ python -m groundwork.govern")
print(f"exit {main(paths)}")
request = ["forget", "3574", "--request", "FR-2026-0001"]
print(f"\n$ python -m groundwork.govern {' '.join(request)}")
print(f"exit {main(paths + request)}")
tidy("build")
