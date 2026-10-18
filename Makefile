# Groundwork — the commands you will actually type.
#
# A Makefile is the cheapest documentation there is: it is the only kind that stops
# working when it goes out of date. Everything below is what the README describes, and
# `make help` is what a new person runs first.
#
# Groundwork is built across the book, so some of its commands arrive later than others.
# Those say which chapter brings them rather than failing with a missing file.

.DEFAULT_GOAL := help
.PHONY: help setup preflight data sources listings verify test test-all lint \
        clean ingest transform run backfill reconcile diff test-push test-unit \
        test-contract test-integration test-diff warehouse demo

# The project's own Python once `make setup` has made it, so no command depends on the
# environment being switched on; the system's python3 only until then.
PY := $(if $(wildcard .venv/bin/python),.venv/bin/python,python3)
export PYTHONPATH := $(CURDIR)/code

# `make run` and `make backfill` take the day from DAY (and FROM/TO): the logical date of
# the run, not today's date. Meridian's records stop at the end of 2025.
DAY ?= 2025-12-31
FROM ?= 2022-01-01
TO ?= 2025-12-31
ARGS ?=
# `make diff` compares the warehouse built from BASE's dbt project with the one built from
# the working tree's, both from the same raw layer.
BASE ?= main

help:  ## show this list
	@grep -E '^[a-z-]+:.*?## ' $(MAKEFILE_LIST) \
	  | awk 'BEGIN{FS=":.*?## "}{printf "  \033[1m%-17s\033[0m %s\n", $$1, $$2}'

# An existing .venv is kept, whatever Python made it: to start again with another
# Python, delete .venv and run, for example, make setup PY=python3.12.
setup:  ## create the virtualenv and install everything
	@if [ -x .venv/bin/python ]; then \
	  echo "Keeping the existing .venv ($$(.venv/bin/python --version))."; \
	else echo "$(PY) -m venv .venv"; $(PY) -m venv .venv; fi
	.venv/bin/pip install --upgrade pip
	.venv/bin/pip install -r requirements.txt
	@# One line in site-packages puts code/ on Python's path, so `from groundwork.config
	@# import ...` works from any folder without setting PYTHONPATH.
	.venv/bin/python code/_path.py
	@echo "Now: make data, then make preflight."

preflight:  ## check Python, the libraries and the sources before you start
	$(PY) code/_preflight.py

data:  ## generate Meridian and its sources from their seeds, and verify them (about 2 min)
	$(PY) code/meridian/generate.py
	$(PY) code/meridian/verify.py
	$(PY) code/meridian/generate_ml.py
	$(PY) code/meridian/verify_ml.py
	$(PY) code/meridian/generate_sources.py
	$(PY) code/meridian/verify_sources.py

sources:  ## regenerate only the operational sources, and verify them
	$(PY) code/meridian/generate_sources.py
	$(PY) code/meridian/verify_sources.py

listings:  ## run every listing whose source changed, and capture its output
	$(PY) code/_runner.py

verify:  ## the code guarantee: nothing stale, and every listing prints the same thing twice
	$(PY) code/_runner.py --check --twice

test:  ## the fast tests, no generated data needed
	$(PY) -m pytest tests/ -q -m "not slow"

test-all:  ## every test, slow ones too: needs make data
	$(PY) -m pytest tests/ -q

# The suite in its four layers (Chapter 23; tests/conftest.py says which test is in which).
# A slow test in a layer is skipped, saying why, until make data has run.
test-push:  ## what CI runs on every push: unit and contract tests, no generated data
	$(PY) -m pytest tests/ -q -m "(unit or contract) and not slow"

test-unit:  ## one layer: a function or a model, on rows the test writes
	$(PY) -m pytest tests/ -q -m unit

test-contract:  ## one layer: the promises kept at each boundary
	$(PY) -m pytest tests/ -q -m contract

test-integration:  ## one layer: stages run together, ingest then dbt on a tiny Meridian
	$(PY) -m pytest tests/ -q -m integration

test-diff:  ## one layer: two builds, or two ways of building, that must agree
	$(PY) -m pytest tests/ -q -m diff

diff:  ## build BASE's dbt project and this one from the same raw, and compare every table
	$(PY) -m groundwork.diff --against $(BASE) $(ARGS)

lint:  ## the checks CI runs
	$(PY) -m ruff check code/ tests/
	$(PY) code/_runner.py --check

ingest:  ## land every source into raw for DAY
	@if [ -f code/groundwork/ingest/__main__.py ]; then $(PY) -m groundwork.ingest --day $(DAY) $(ARGS); \
	else echo "Ingestion arrives in Chapters 6 to 10."; fi

transform:  ## build staging and marts from raw
	@if [ -f code/groundwork/transform/__main__.py ]; then $(PY) -m groundwork.transform $(ARGS); \
	else echo "The transformation layer arrives in Chapters 11 to 13."; fi

run:  ## the nightly pipeline for DAY: ingest, transform, test, publish
	@if [ -f code/groundwork/nightly.py ]; then $(PY) -m groundwork.nightly run --day $(DAY) $(ARGS); \
	elif [ -f code/groundwork/orchestrate/__main__.py ]; then $(PY) -m groundwork.orchestrate run --day $(DAY) $(ARGS); \
	else echo "Orchestration arrives in Chapter 17."; fi

backfill:  ## rerun the pipeline for every day from FROM to TO
	@if [ -f code/groundwork/orchestrate/__main__.py ]; then $(PY) -m groundwork.orchestrate backfill --from $(FROM) --to $(TO) $(ARGS); \
	else echo "Backfills arrive in Chapter 18."; fi

reconcile:  ## prove the warehouse against the ledger, Book 1 and Book 2
	@if [ -f code/groundwork/reconcile.py ]; then $(PY) -m groundwork.reconcile $(ARGS); \
	else echo "Reconciliation arrives in Chapter 15."; fi
	@if [ -f code/groundwork/reconcile_books.py ]; then $(PY) -m groundwork.reconcile_books; \
	else echo "Books 1 and 2 are reconciled in Chapter 26."; fi

# `make warehouse` builds Groundwork from nothing in FOLDER (default data/groundwork), for
# the newest night the sources have. It refuses a folder that already holds a warehouse.
FOLDER ?= data/groundwork

warehouse:  ## build Groundwork from nothing: the v1.0 night on an empty FOLDER
	$(PY) -m groundwork.nightly build --folder $(FOLDER)

demo:  ## Groundwork in a minute: the night, the proof, one November, the refusals
	$(PY) -m groundwork.demo --warehouse $(FOLDER)/warehouse.duckdb

clean:  ## remove build output, caches and the sandbox
	rm -rf .pytest_cache/ .ruff_cache/ data/sandbox/
	find . -name __pycache__ -type d -prune -exec rm -rf {} +
