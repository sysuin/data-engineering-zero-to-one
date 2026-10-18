# Contributing to the code of *Data Engineering from Zero to One*

Thank you for looking. This repository is the companion to a book, which shapes what a
good change is here. Read this before opening a pull request.

## What this repository is

**Groundwork** is the data platform of Meridian Supply Co., a fictional mid-size
distributor, and the worked project of *Data Engineering from Zero to One*. It takes six messy operational
sources and builds, chapter by chapter, a tested, orchestrated, governed warehouse that
reconciles to the cent with the one the earlier books in the series used.

## Rules the code keeps

**Each milestone's code is kept as the book showed it.** The listings in `code/NN/` are
the book's figures in another form. Do not refactor an early chapter's code to match a
later one; the difference between them is the lesson. A bug in a listing is a bug in the
book, so report it as an issue rather than fixing the listing alone, and the fix will be
made in both places.

**No output was typed by a human.** `code/_runner.py` runs every listing and saves its real
output beside it as a `.out` file. If you change a listing, run it again:

```
python3 code/_runner.py 07
```

**Every listing is deterministic.** Sources are simulated to a clock the listing sets;
nothing reads the wall clock; every printed result is ordered; money is summed in integer
cents. `make verify` runs every listing twice and fails if one character differs. A listing
that cannot repeat (a timing) says so on its first lines with `# nondeterministic: <reason>`.

**Every number is reconciled.** A count or a total a listing prints is printed beside a
second source that checks it, computed in the same listing.

**Books 1 and 2's datasets do not change.** `code/meridian/generate.py` and
`generate_ml.py` are carried over byte for byte, and their output must stay byte-identical.
This book's sources are a third generator that reads their output and never writes to it.

**No secret goes in a file git can see.** Frontline's token comes from `.env`, which is
ignored; `.env.example` holds the demo value the local server accepts.

## Before you open a pull request

```
make lint          ruff, and no captured output stale
make test          the fast tests, no generated data needed
make test-all      every test, after make data
make verify        every listing run twice, and compared
```

CI runs the first two on every push, and the rest nightly.

## Good to know

- **Disk.** Tradeline's database is about 200 MB, and each working copy another 200 MB.
  Listings share one sandbox copy in `data/sandbox/`; `make clean` removes it.
- **DuckDB reads SQLite through an extension** it downloads the first time it is used.
  After that it works offline.
- **The planted problems in the sources are verified, not assumed.**
  `code/meridian/verify_sources.py` proves each one present; a problem it does not prove is
  a chapter whose lesson may not happen.
