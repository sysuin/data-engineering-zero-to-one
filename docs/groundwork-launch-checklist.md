# Groundwork launch checklist

Checked by `code/26/07_checklist.py` against the build from nothing (nightly.2026-01-04.1).
A line marked *by hand* is one no script can tick. Rerun the
listing after changing anything below.

| Area | Line | Result | Evidence |
|---|---|---|---|
| Run | One command builds Groundwork from nothing | yes | 20 tasks, all first try |
| Run | Every batch held to its contract | yes | 0 broken, 1 row parked |
| Run | The cron line runs v1.0's night | no | names v0.8's night |
| Run | Published by 07:00 UTC (the freshness SLO) | yes | published 06:04, due 07:00 |
| Run | A failed night reaches a person by 08:00 | no | audit.notifications only |
| Run | Raw, landed and the snapshot backed up | no | no job copies them |
| Prove | The ledger ties, every region and month | yes | 247 lines, 0 differ |
| Prove | Books 1 and 2 reconcile | yes | 17 tables, 12 quarters, 0 differ |
| Prove | Chapter 15's checks: no errors | yes | 26 checks, 0 errors |
| Prove | A change is diffed before it merges | yes | diff on every pull request |
| Prove | make test passes | by hand | a person ticks it |
| Serve | The stream has read everything that arrived | yes | lag 0 records |
| Serve | Every session points at an account version | yes | 0 sessions without a key |
| Govern | No role reads what it should not | yes | 4 roles, 0 problems |
| Govern | Every column classified by a rule | no | 4 columns await their owner |
| Govern | The pseudonym key is a real secret | no | the example's value |
| Govern | Retention's owners agree; --retain is on | by hand | a person ticks it |
| Own | Every source and check has an owner | yes | 17 contracts, 8 rules |
| Own | Each owner has read their contract | by hand | a person ticks it |
| Own | Someone else builds it from a clean clone | by hand | a person ticks it |

20 lines: 11 yes, 5 no, 4 by hand.
