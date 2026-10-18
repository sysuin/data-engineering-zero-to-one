# nondeterministic: the seconds each CI job's tests take on this machine
# The suite sorted into its layers, and each CI job's share of it run
# here, without the generated data.
# timeout: 1200
import json
import re
import subprocess
import sys
import time

PYTEST = [sys.executable, "-m", "pytest", "tests/", "-q",
          "-p", "no:cacheprovider"]


def collect(marks: str) -> int:
    out = subprocess.run(PYTEST + ["--collect-only", "-m", marks],
                         capture_output=True, text=True).stdout
    return sum("::" in line for line in out.splitlines())


JOBS = {"unit": "fast: every push", "contract": "fast: every push",
        "integration": "diff: every pull request",
        "diff": "diff: every pull request"}
print(f"{'layer':<13}{'tests':>6}{'slow':>6}   "
      f"job (slow ones: nightly)")
total = slow = 0
counts = {}
for layer, job in JOBS.items():
    n, s = collect(layer), collect(f"{layer} and slow")
    total, slow = total + n, slow + s
    counts[layer] = {"tests": n, "slow": s, "job": job}
    print(f"{layer:<13}{n:>6}{s:>6}   {job}")
everything = collect("")
print(f"{'all':<13}{everything:>6}{slow:>6}")
print(f"every test in exactly one layer: {total == everything}")
with open("code/23/07_ci_layers.json", "w") as f:
    json.dump(counts, f, indent=1)

print("\nrun here, without the generated data:")
for marks in ("(unit or contract) and not slow",
              "(integration or diff) and not slow"):
    start = time.perf_counter()
    out = subprocess.run(PYTEST + ["-m", marks], capture_output=True,
                         text=True).stdout
    took = time.perf_counter() - start
    passed = re.search(r"(\d+) passed", out).group(1)
    failed = re.search(r"(\d+) failed", out)
    print(f"  {marks:<36}{passed:>4} passed, "
          f"{failed.group(1) if failed else 0} failed, {took:4.0f} s")
