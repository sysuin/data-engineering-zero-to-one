# Groundwork runbook

What to do when Groundwork tells you something is wrong. Open this first.

Groundwork runs two things every day, on two separate cron lines, both in UTC:

```text
0 6 * * *  python -m groundwork.observe run     the night for yesterday
0 7 * * *  python -m groundwork.observe watch   reads what the night recorded
```

The watch is not part of the night. It runs whether the night succeeded, failed or never
started, and it reads only the audit tables. Every alert it opens is a row in `audit.alerts`;
every message it sends is a row in `audit.notifications`.

## How alerts reach you

| Severity | Means | Reaches you |
|---|---|---|
| page | someone must act today, before the dashboards are read | the on-call pager; outside 08:00-20:00 New York it waits for 08:00 |
| ticket | someone must look today, in working hours | the owner's queue |
| copy | the fault may be in a source another team runs | that team's contact |
| resolved | the rule stopped firing at the latest watch | the owner |

One condition is one alert. If the same file fails every night, you are paged once; later
watches count it (`audit.alerts.times`) and send nothing. An alert resolves by itself at the
first watch where its rule no longer fires. Nothing in Groundwork wakes anyone at night: nothing
it produces is read before 08:00 New York.

## First five minutes, whatever the alert

```text
python -m groundwork.orchestrate history --day D     every try of the night
python -m groundwork.observe signals --day D         the four signals
python -m groundwork.observe slo                     what is left of the budget
```

`D` is the logical date: the run for D starts at 06:00 UTC on D+1. Read the first failed task's
detail; it is the first line of the real error.

## The rules

### night_failed (page, data engineering)

The latest run of the day that is due failed, or no run exists.

- `history` shows which task failed and which never ran (`upstream_failed`).
- A task that failed with `fatal:` will fail the same way on a rerun. Mend the cause first
  (a contract, a model, a file), then rerun: `python -m groundwork.observe run --day D --rerun`.
- Any other failure (a dropped connection, a desk that answered 503) may pass on a rerun.
- No run at all: check that the 06:00 cron line ran and that no lock file is held
  (`warehouse.duckdb.nightly.lock`) by a run that died.

### held_rows (page for a broken batch; ticket for parked rows)

A contract kept rows out of raw. A **broken** batch is a whole file refused: its header or most
of its rows break the contract. It is tried again every night until the file or the contract
changes. **Parked** rows are single rows in `audit.quarantine`, with the reason.

- Broken: `python -m groundwork.contracts.check suppliers.price_lists` and the file ledger's note
  (`audit.file_ledger`, status `failed`) say what changed. Ask the sender's team (copied on the
  alert) whether the change is permanent and what the new column means. If it is, change the
  contract additively and ship the reader that knows the new shape in the same change
  (`docs/groundwork-contracts.md`). Do not edit the contract to make tonight pass.
- Parked: read the rows in `audit.quarantine`; correct and release, ask for a resend, or reject
  with a reason. More than half of a batch parked is a page: that is a file that changed.

### signal (page at error, ticket at warn)

One of the four signals is outside its band or limit for the day: freshness (days behind),
volume (rows), schema (a format a sender has not used before), distribution (a share, a median,
or the share of empty values). `signals --day D` prints them all with their bands. An error, such
as supplier rows with no cost, also stopped publish through the monitor task.

### checks (page at error, ticket at warn)

Chapter 15's checks recorded an error or a warning at the day's judged moment, or the
reconciliation did not tie. The owner is the check's own (finance for the ledger). `python -m
groundwork.quality --at <D+1>T07:00 --no-record` repeats them.

### stuck_load (page)

A load has been `running` for more than two hours: it died without saying so. Find its process;
if there is none, rerun the night.

### budget_spent (ticket)

The freshness SLO, *each day published by 07:00 UTC the next morning on 95% of days in any 30*,
has no budget left (1.5 late days a window). Stop risky changes for the rest of the window and
spend the time on what spent the budget: `slo` and `audit.alerts` show which days and why.

## Before you change a column

```text
python -m groundwork.observe impact tradeline.order_lines.unit_price_cents
python -m groundwork.observe lineage marts.fct_order_lines.revenue
```

`impact` lists every column computed from the one you name, the relations and tests that read
them, and the source's contacts. It follows values and the rows a column chooses.

## After an incident: the postmortem

Within a few days, write what happened, what it cost, why, and what will change. Blameless: say
what people did and why it made sense at the time, and look for causes in the system. Every
action has an owner. Keep postmortems here, newest first.

### Supplier price lists refused for 14 nights, July 2024 (replayed)

A replay of Groundwork as Chapter 17 left it, with the price lists' contract as first written,
over July 2024 (`code/21/08_replay_incident.py`).

**Impact.** Nothing published for logical days 2 to 15 July; marts and dashboards stayed at 1 July.
No wrong number was shown. The freshness budget was spent by the morning after 3 July; July ended
53.3% on time.

**Timeline (UTC).**

| When | What |
|---|---|
| 2 Jul 18:48 | Pemberton's July list arrives; its cost column is renamed "Unit Cost USD" |
| 3 Jul 06:00 | run for 2 Jul: `ingest_suppliers` fatal, "header renamed?"; 5 tasks never run; recorded, nobody told |
| 4 Jul onwards | the file fails again every night |
| 5 Jul 12:00 | run for 4 Jul also fails at the Relay sensor: no export, 37 pokes |
| 16 Jul | a buyer asks why the margin report has no July |
| 16 Jul 15:00 | contract v2 and the new reader shipped; rerun of 15 Jul succeeds |

**Causes.** A sender changed a file's header without notice, and the contract refused it, as
designed. Nothing alerted on a failed run or on rows held back, so the failure was recorded and
not seen; a stale dashboard looks like a current one. One cost column stopped every mart, because
the DAG builds nothing until every ingest succeeds.

**Went well.** Nothing wrong was published. The error's first line named the file and the column.

**Actions.**

| Action | Owner | State |
|---|---|---|
| Watch at 07:00 UTC on its own cron line | data engineering | done (v0.8) |
| Alert on held rows, broken batches included | data engineering | done (v0.8) |
| Freshness SLO with a ticket when the budget is spent | data engineering | done (v0.8) |
| Publish the marts a failure did not touch | data engineering | open |
| Decide what a night with no Relay export should do | data engineering, sales operations | open |
| Ask senders to announce format changes | procurement | open |

With the watch, the same July has the page at 12:00 UTC (08:00 New York) on 3 July, the fix at
15:00 the same day, and two late days in the month, one of them Relay's.
