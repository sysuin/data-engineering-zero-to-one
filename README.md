# Data Engineering from Zero to One: the code

The companion code for *Data Engineering from Zero to One: Build Reliable Data Pipelines and Warehouses with SQL and Python* by Sunny Singh,
Book 3 of the Zero to One Series.

It holds **Groundwork**, the data platform the book builds across twenty-six chapters, and
every listing in the book with the output it printed. Groundwork belongs to Meridian Supply
Co., a fictional distributor whose data lives where real companies' does: an orders
database, a CRM's nightly exports, a support desk behind an API, suppliers' price files, a
customer portal's clickstream and a finance ledger. Each is generated here, with the mess
real systems add. Groundwork loads them so that any load can run twice safely, models them
into a warehouse with facts and slowly changing dimensions, tests it, runs it nightly with
backfills, and proves it right by reconciling it with the ledger to the cent.

Nothing in the book's output was typed by hand. Each listing was executed and its real
output saved beside it, so you can compare what you get with what the book shows. Every
listing is deterministic: run it twice and it prints the same thing, to the last digit.

## Start

You need Python 3.12 and about 2 GB of free disk. No cloud account, no API key.

```bash
make setup                   # a virtualenv in .venv, the libraries, and code/ on Python's path
cp .env.example .env         # the support desk API's demo token, and the pseudonym key
make data                    # Meridian and its sources, generated and verified (about 2 min)
make preflight               # checks Python, the libraries and the sources, in order
```

`make data` needs no network: every dataset is generated from fixed seeds, byte-identical
on every machine. Appendix B of the book walks through each step on macOS, Windows and
Linux. DuckDB downloads its SQLite extension the first time a listing uses it; after that
everything works offline.

## Running the listings

Each chapter's listings are in `code/NN/`, numbered in the order the chapter runs them, with
the captured output beside each one as a `.out` file. Run one from the repository's root,
because every path in the book is written from there:

```bash
.venv/bin/python code/07/01_full_refresh_arithmetic.py
```

Or let the runner run a chapter and compare its output with what the book printed:

```bash
.venv/bin/python code/_runner.py 07                 # re-run chapter 7, capture its output
.venv/bin/python code/_runner.py --check --twice    # every capture current, every run repeatable
```

`requirements.txt` gives lower bounds; Appendix C of the book lists the exact versions the
printed output came from. A newer library can change the wording of an error or the order
of rows a listing does not sort, but it should never change a total. If one does, please
open an issue. Listings that time something say so on their first lines, and their times
will differ from the book's.

## Running Groundwork

```bash
make ingest DAY=2025-12-31      # land every source into raw for one day
make transform                  # build staging and marts from raw
make run DAY=2025-12-31         # the nightly pipeline: ingest, transform, test, publish
make backfill FROM=2025-01-01 TO=2025-12-31
make reconcile                  # prove the warehouse against the ledger, Book 1 and Book 2
make help                       # everything else
```

The warehouse is a DuckDB file at `data/groundwork/warehouse.duckdb`.

## Where things are

```
code/NN/            the listings for chapter NN, each with its captured .out
code/groundwork/    Groundwork: ingestion, contracts, models, orchestration, observability
code/meridian/      the generators for Meridian's data and its six operational sources
dbt/groundwork/     the dbt project, from Chapter 14
docs/               Groundwork's source inventory, model, contracts and runbook
tests/              the test suite, in four layers: unit, contract, integration, diff
```

## Licence

MIT; see [LICENSE](LICENSE). The licence covers this code and the data it generates, not
the book.
