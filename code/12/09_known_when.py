# A value that arrives after the fact: when each cancellation reason was
# typed, and what a monthly report run on the fifth could have known.
import duckdb

from groundwork.config import TODAY

from _dims import sandbox, tidy

con = duckdb.connect(str(sandbox("known")))
con.execute("""
CREATE TEMP TABLE ended AS
WITH c AS (
    SELECT * FROM staging.stg_relay__contracts
    WHERE snapshot_date = (SELECT max(snapshot_date)
                           FROM staging.stg_relay__contracts)
), typed AS (
    SELECT record_id AS contract_id, field_name, changed_at
    FROM staging.stg_relay__field_history WHERE record_type = 'contract'
)
SELECT c.contract_id, c.end_date, c.cancellation_reason,
       o.changed_at AS outcome_at, r.changed_at AS reason_at
FROM c
JOIN typed AS o ON o.contract_id = c.contract_id
               AND o.field_name = 'outcome'
LEFT JOIN typed AS r ON r.contract_id = c.contract_id
                    AND r.field_name = 'cancellation_reason'
WHERE c.outcome = 'not_renewed'""")
n, typed, lo, mid, hi = con.execute("""
    SELECT count(*), count(reason_at),
           min(date_diff('day', end_date, CAST(reason_at AS DATE))),
           median(date_diff('day', end_date, CAST(reason_at AS DATE))),
           max(date_diff('day', end_date, CAST(reason_at AS DATE)))
    FROM ended""").fetchone()
print(f"contracts not renewed {n:,}; with a reason today {typed:,}")
print(f"reason typed after the contract ended: {lo} to {hi} days, "
      f"median {mid:.0f}")
(soon,) = con.execute("SELECT count(*) FROM ended WHERE "
                      "date_diff('day', end_date, CAST(outcome_at "
                      "AS DATE)) <= 1").fetchone()
print(f"outcome recorded within a day of the end: {soon:,} of {n:,}")

print("\nreasons by month ended, as the report run on the 5th of "
      "the next\nmonth saw them, and as they stand today:")
print(f"{'ended in':<10}{'not renewed':>12}{'known on the 5th':>18}"
      f"{'known today':>13}")
for month, total, then, now in con.execute("""
    SELECT strftime(end_date, '%Y-%m'), count(*),
           count(*) FILTER (reason_at < date_trunc('month', end_date)
                            + INTERVAL 1 MONTH + INTERVAL 4 DAY),
           count(cancellation_reason)
    FROM ended WHERE end_date >= DATE '2025-07-01'
      AND date_trunc('month', end_date) + INTERVAL 1 MONTH
          + INTERVAL 4 DAY < ?          -- reports already run
    GROUP BY 1 ORDER BY 1""", [TODAY]).fetchall():
    print(f"{month:<10}{total:>12,}{then:>18,}{now:>13,}")
tidy("known")
