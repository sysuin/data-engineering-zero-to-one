#!/usr/bin/env python3
"""
Run every code listing in the book and capture what it actually printed.

The rule this enforces: **no expected output in this book was typed by a human.**
Every listing is a real file; this runner executes it and writes its real stdout and
stderr next to it as a `.out` file, which the manuscript then includes. A listing whose
output was hand-written is a listing that is quietly wrong within two months.

A second rule: **determinism.** A pipeline that loads 1,229,062 rows today and 1,229,061
tomorrow makes the book wrong, and a reader comparing their screen with the page cannot
tell a bug from noise. Every listing runs against sources simulated to a clock the listing
sets, with thread counts pinned, and `--twice` runs each one a second time and fails if a
single character of output differs.

    python3 code/_runner.py                  # run everything that changed
    python3 code/_runner.py --all            # ignore the cache, run everything
    python3 code/_runner.py 02               # just chapter 02
    python3 code/_runner.py --check          # fail if any .out is stale (fast; runs nothing)
    python3 code/_runner.py --check --twice  # also run every listing twice, and fail if the
                                             # two runs, or either and its .out, disagree (CI)
    python3 code/_runner.py --twice          # run what changed, twice each, and compare

Conventions a listing may declare on its first few lines:

    # expect-fail              this listing is supposed to raise; a clean exit is the failure
    # timeout: 300             override the default 120-second limit
    # skip                     do not run (a fragment, or something needing manual setup)
    # nondeterministic: why    exempt from the --twice comparison, with the reason written
                               down — a timing. Rare, and never quiet.
    # compile-only: why        compiled, not run: code for a tool this book does not install
                               (Airflow, Dagster, PySpark), shown as a translation. The
                               prose must say it was not run. It has no .out.

Listings run with the project root as the working directory, so every path in the book
reads the same way: `data/meridian/...`.

DEPENDENCIES. A listing is stale when its own text changes, and also when a file of the
book's own code that it imported has changed since it ran: a module under code/groundwork/,
a helper beside it, a test file a listing ran. sitecustomize.py records what each run
imported (every process the listing started), and the report keeps each file's digest.
Data and settings files a listing reads are not tracked; a change to them needs --all.

Model calls are counted, not required. This book makes none; if a listing ever does,
`sitecustomize.py` records its tokens and this runner reports them.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import subprocess
import sys
import time

CODE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(CODE)
REPORT = os.path.join(CODE, "_report.json")
USAGE = os.path.join(CODE, "_usage.jsonl")
DEFAULT_TIMEOUT = 120
CHAPTER_DIR = re.compile(r"^\d{2}$")

# Everything a listing inherits that could make two runs differ. Seeds are the listing's
# own business and are set in the code the reader sees (groundwork/config.py holds them);
# these are the sources of variation a reader cannot see in the code.
DETERMINISM_ENV = {
    "PYTHONHASHSEED": "0",          # set and dict iteration order over strings
    "OMP_NUM_THREADS": "1",         # LightGBM, scikit-learn and PyTorch sum in thread order,
    "MKL_NUM_THREADS": "1",         # and floating-point addition is not associative: four
    "OPENBLAS_NUM_THREADS": "1",    # threads can change the last digit a listing prints
    "VECLIB_MAXIMUM_THREADS": "1",
    "NUMEXPR_NUM_THREADS": "1",
    "CUBLAS_WORKSPACE_CONFIG": ":4096:8",   # harmless on CPU; required if a GPU is ever used
    "TZ": "UTC",                    # a timestamp printed in local time differs by reader
    "PYTHONIOENCODING": "utf-8",    # the same bytes on every terminal
    "MPLBACKEND": "Agg",            # a listing that plots must not open a window
    "MLFLOW_DISABLE_AGENT_HINT": "1",   # MLflow prints a banner on import otherwise
}


def discover(only: str | None) -> list[str]:
    out = []
    for d in sorted(os.listdir(CODE)):
        if not CHAPTER_DIR.match(d):
            continue
        if only and d != only:
            continue
        for f in sorted(os.listdir(os.path.join(CODE, d))):
            if f.endswith(".py") and not f.startswith("_"):
                out.append(os.path.join(d, f))
    return out


def directives(path: str) -> dict:
    d = {"expect_fail": False, "timeout": DEFAULT_TIMEOUT, "skip": False,
         "nondeterministic": None, "compile_only": None}
    with open(os.path.join(CODE, path)) as f:
        for line in f.read().split("\n")[:12]:
            s = line.strip()
            if s == "# expect-fail":
                d["expect_fail"] = True
            elif s == "# skip":
                d["skip"] = True
            elif s.startswith("# timeout:"):
                d["timeout"] = int(s.split(":")[1])
            elif s.startswith("# compile-only:"):
                reason = s.split(":", 1)[1].strip()
                if not reason:
                    raise SystemExit(f"{path}: '# compile-only:' needs a reason")
                d["compile_only"] = reason
            elif s.startswith("# nondeterministic:"):
                reason = s.split(":", 1)[1].strip()
                if not reason:
                    raise SystemExit(f"{path}: '# nondeterministic:' needs a reason")
                d["nondeterministic"] = reason
    return d


def digest(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()[:16]


def source_digest(path: str) -> str:
    with open(os.path.join(CODE, path), "rb") as f:
        return digest(f.read())


def dependencies(listing: str, deps_file: str) -> dict[str, str]:
    """The book's own files the listing's processes imported, besides the listing itself,
    each with its digest. sitecustomize.py writes the list; see DEPENDENCIES."""
    files: set[str] = set()
    if os.path.exists(deps_file):
        with open(deps_file) as f:
            for line in f:
                files.update(json.loads(line))
        os.remove(deps_file)
    files.discard(os.path.join("code", listing))
    return {p: file_digest(p) for p in sorted(files)}


def file_digest(relative: str) -> str | None:
    path = os.path.join(ROOT, relative)
    if not os.path.exists(path):
        return None
    with open(path, "rb") as f:
        return digest(f.read())


def changed_dependencies(entry: dict) -> list[str]:
    """Recorded dependencies whose contents are no longer what the listing ran against. An
    entry recorded before dependencies were kept has none, and is judged by its text alone."""
    return [p for p, d in entry.get("deps", {}).items() if file_digest(p) != d]


def execute(path: str, spec: dict) -> tuple[str, int, float]:
    """Run one listing once. Returns (captured output, return code, seconds)."""
    body, code, elapsed, _ = execute_recording(path, spec)
    return body, code, elapsed


def execute_recording(path: str, spec: dict) -> tuple[str, int, float, dict]:
    """execute(), and the dependencies the run imported."""
    deps_file = os.path.join(CODE, f"_deps.{os.getpid()}.jsonl")
    if os.path.exists(deps_file):
        os.remove(deps_file)
    started = time.time()
    try:
        proc = subprocess.run(
            [sys.executable, os.path.join("code", path)],
            cwd=ROOT, capture_output=True, text=True, timeout=spec["timeout"],
            env={**os.environ, **DETERMINISM_ENV,
                 "PYTHONUNBUFFERED": "1", "BOOK_RUN": "1", "COLUMNS": "88",
                 # sitecustomize.py counts model calls; only the build ever sees it.
                 "PYTHONPATH": CODE + os.pathsep + os.environ.get("PYTHONPATH", ""),
                 "BOOK_LISTING": path, "BOOK_USAGE_FILE": USAGE,
                 "BOOK_DEPS_FILE": deps_file},
        )
        body, code = proc.stdout, proc.returncode
        if proc.stderr.strip():
            body += ("\n" if body and not body.endswith("\n") else "") + proc.stderr
    except subprocess.TimeoutExpired:
        body, code = f"TIMEOUT after {spec['timeout']}s\n", -9
    elapsed = time.time() - started

    # Tracebacks carry absolute paths. The book must not contain the author's home
    # directory, and a reader's output should match the book's on their own machine.
    body = body.replace(ROOT + os.sep, "").replace(ROOT, ".")
    return body.rstrip("\n") + "\n", code, elapsed, dependencies(path, deps_file)


def first_difference(a: str, b: str) -> str:
    """The first line on which two captures disagree, for a message a person can act on."""
    for number, (x, y) in enumerate(zip(a.splitlines(), b.splitlines()), 1):
        if x != y:
            return f"line {number}: {x.strip()[:60]!r} vs {y.strip()[:60]!r}"
    return f"lengths differ: {len(a.splitlines())} vs {len(b.splitlines())} lines"


def run_one(path: str, spec: dict, twice: bool) -> dict:
    body, code, elapsed, deps = execute_recording(path, spec)
    failed = code != 0
    ok = failed if spec["expect_fail"] else not failed
    result = {"ok": ok, "returncode": code, "seconds": round(elapsed, 2),
              "bytes": len(body), "expect_fail": spec["expect_fail"],
              "out_hash": digest(body.encode()), "deps": deps}

    if twice and ok and not spec["nondeterministic"]:
        again, _, more = execute(path, spec)
        result["seconds"] = round(elapsed + more, 2)
        if again != body:
            result["ok"] = False
            result["nondeterministic"] = first_difference(body, again)

    with open(os.path.join(CODE, path[:-3] + ".out"), "w") as f:
        f.write(body)
    return result


def check_twice(path: str, spec: dict) -> str | None:
    """
    CI's determinism gate: run the listing twice without touching its .out, and compare
    both runs with each other and with the capture the book prints. Returns a problem, or
    None. A capture that no longer reproduces is exactly the drift this book promises
    cannot happen.
    """
    first, code, _ = execute(path, spec)
    failed = code != 0
    if failed != spec["expect_fail"]:
        return f"exit status {code}"
    second, _, _ = execute(path, spec)
    if first != second:
        return f"two runs differ, {first_difference(first, second)}"
    outfile = os.path.join(CODE, path[:-3] + ".out")
    if os.path.exists(outfile):
        with open(outfile) as f:
            captured = f.read()
        if captured != first:
            return f"differs from its .out, {first_difference(captured, first)}"
    return None


def _env_file() -> dict[str, str]:
    """`.env`, read without a dependency: the runner must work before anything is installed."""
    values: dict[str, str] = {}
    path = os.path.join(ROOT, ".env")
    if os.path.exists(path):
        with open(path) as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith("#") and "=" in line:
                    key, _, value = line.partition("=")
                    values[key.strip()] = value.strip().strip('"').strip("'")
    return values


def _rate(model: str, env: dict[str, str]) -> tuple[float, float] | None:
    """Dollars per million tokens (input, output), only if the reader has set them."""
    key = model.upper().replace("-", "_").replace(".", "_")
    raw_in, raw_out = env.get(f"RATE_{key}_INPUT"), env.get(f"RATE_{key}_OUTPUT")
    if raw_in and raw_out:
        return float(raw_in), float(raw_out)
    return None


def report_spend() -> None:
    """Summarise what any model calls cost: tokens always, dollars when rates are set."""
    if not os.path.exists(USAGE):
        return
    totals: dict[str, list[int]] = {}
    with open(USAGE) as f:
        for line in f:
            r = json.loads(line)
            t = totals.setdefault(r["model"], [0, 0, 0])
            t[0] += r["prompt_tokens"]
            t[1] += r["completion_tokens"]
            t[2] += 1
    if not totals:
        return

    env = {**_env_file(), **os.environ}
    ceiling = float(env.get("BOOK_SPEND_CEILING_USD", "2.00"))
    print("\n  model usage")
    dollars, priced = 0.0, True
    for model, (pin, pout, calls) in sorted(totals.items()):
        rates = _rate(model, env)
        if rates:
            cost = (pin * rates[0] + pout * rates[1]) / 1_000_000
            dollars += cost
            money = f"  ${cost:.4f}"
        else:
            priced = False
            money = "  (no rate set)"
        print(f"    {model:22} {calls:4} calls  {pin:>7,} in  {pout:>7,} out{money}")
    if priced:
        print(f"    {'total':22} {'':4}         {'':>7}     {'':>7}      ${dollars:.4f}")
        if dollars > ceiling:
            print(f"    WARNING: over the ${ceiling:.2f} ceiling in .env")
    else:
        print("    Set RATE_<MODEL>_INPUT / _OUTPUT in .env to see dollars.")


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("chapter", nargs="?", help="two-digit chapter, e.g. 02")
    ap.add_argument("--all", action="store_true", help="ignore the cache")
    ap.add_argument("--check", action="store_true",
                    help="fail if anything is stale; with --twice, also prove determinism")
    ap.add_argument("--twice", action="store_true",
                    help="run each listing twice and fail if the output differs")
    args = ap.parse_args()

    cache = {}
    if os.path.exists(REPORT):
        with open(REPORT) as f:
            cache = json.load(f).get("listings", {})

    listings = discover(args.chapter)
    if not listings:
        print("no listings found")
        return 0

    if args.all and os.path.exists(USAGE):
        os.remove(USAGE)

    stale, unstable, exempt = [], [], []
    ran, skipped, failures = 0, 0, []
    total_time = 0.0

    for path in listings:
        spec = directives(path)
        if spec["skip"]:
            skipped += 1
            continue
        if spec["compile_only"]:
            # Proved to be Python, never run: see the directive's reason.
            with open(os.path.join(CODE, path)) as f:
                try:
                    compile(f.read(), path, "exec")
                except SyntaxError as e:
                    print(f"  FAIL  {path:34} does not compile: {e}")
                    failures.append(path)
            skipped += 1
            continue
        if spec["nondeterministic"]:
            exempt.append((path, spec["nondeterministic"]))

        h = source_digest(path)
        outfile = os.path.join(CODE, path[:-3] + ".out")
        moved = changed_dependencies(cache.get(path, {}))
        fresh = (cache.get(path, {}).get("hash") == h and os.path.exists(outfile)
                 and not moved)

        if args.check:
            if not fresh:
                stale.append(path + (f"  (imports changed: {moved[0]})" if moved else ""))
            elif args.twice and not spec["nondeterministic"]:
                problem = check_twice(path, spec)
                if problem:
                    unstable.append((path, problem))
            continue
        if fresh and not args.all:
            continue

        before = cache.get(path, {})
        result = run_one(path, spec, args.twice)
        cache[path] = {"hash": h, **result}
        ran += 1
        total_time += result["seconds"]
        mark = "ok  " if result["ok"] else "FAIL"
        note = "  (expected to fail)" if spec["expect_fail"] else ""
        print(f"  {mark}  {path:34} {result['seconds']:6.2f}s  "
              f"{result['bytes']:>6} bytes{note}")
        if moved and before.get("hash") == h:
            print(f"        rerun: {moved[0]} changed"
                  + (f", and {len(moved) - 1} more" if len(moved) > 1 else ""))
        if result.get("nondeterministic"):
            print(f"        not deterministic: {result['nondeterministic']}")
        # Same source, different output: something the listing does not control changed —
        # the data, a library, the machine. Not always a bug, never something to ignore.
        elif (before.get("hash") == h and before.get("out_hash")
              and before["out_hash"] != result["out_hash"] and not spec["nondeterministic"]):
            print("        note: output changed although the listing did not")
        if not result["ok"]:
            failures.append(path)

    if args.check:
        for p in stale:
            print(f"  STALE     {p}")
        for p, problem in unstable:
            print(f"  UNSTABLE  {p}: {problem}")
        for p, reason in exempt:
            print(f"  exempt    {p}: {reason}")
        checked = len(listings) - skipped
        print(f"\n{len(stale)} stale of {checked}"
              + (f", {len(unstable)} not reproducible" if args.twice else ""))
        return 1 if stale or unstable else 0

    # Several runs can share one repository (a chapter's listings while another chapter's
    # run), and each read the report when it started. Writing back the whole cache would
    # drop whatever the others recorded meanwhile, and their listings would then look
    # stale. So re-read the report under a lock and replace only this run's listings.
    import fcntl
    with open(REPORT + ".lock", "w") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        latest = {}
        if os.path.exists(REPORT):
            with open(REPORT) as f:
                latest = json.load(f).get("listings", {})
        for path in listings:
            if path in cache:
                latest[path] = cache[path]
        with open(REPORT, "w") as f:
            json.dump({"generated": time.strftime("%Y-%m-%dT%H:%M:%S"),
                       "listings": latest}, f, indent=2, sort_keys=True)

    print(f"\n  ran {ran}{' (twice each)' if args.twice else ''}, "
          f"cached {len(listings) - ran - skipped}, skipped {skipped}, "
          f"{total_time:.1f}s total")
    report_spend()
    if failures:
        print(f"  FAILURES: {', '.join(failures)}")
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
