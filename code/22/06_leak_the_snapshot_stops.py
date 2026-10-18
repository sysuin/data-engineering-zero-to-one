# The same features built from today's tables: what they say on the
# morning of each mark, against what the warehouse knew then.
# timeout: 300
import json
from pathlib import Path

import duckdb

from groundwork.serve import features

from _serve import tidy, warehouse

con = duckdb.connect(str(warehouse()))
features.build(con)
# The quick way: every contract column, and the account as it is now.
con.execute("""
CREATE TEMP TABLE today AS
SELECT c.*, s.as_of, s.not_renewed, a.owner, a.segment
FROM marts.fct_contracts AS c
JOIN features.renewal_snapshot AS s USING (contract_id)
JOIN marts.dim_account AS a
  ON a.account_id = s.account_id AND a.is_current
WHERE s.end_date <= DATE '2025-11-30'""")
# When Relay recorded each contract field: its audit, by moment.
con.execute("""
CREATE TEMP TABLE recorded AS
SELECT record_id AS contract_id, field_name,
       min(changed_at) AS recorded_at
FROM staging.stg_relay__field_history
WHERE record_type = 'contract' GROUP BY ALL""")
n, leavers = con.execute("SELECT count(*), count(*) FILTER "
                         "(not_renewed) FROM today").fetchone()
print(f"contracts ended Jan 2023 to Nov 2025: {n:,}, "
      f"not renewed {leavers:,}")
print(f"{'filled in on the morning of the mark':<38}{'today':>9}"
      f"{'then':>8}")
for col in ("outcome", "notice_date", "cancellation_reason"):
    now, then = con.execute(f"""
        SELECT count(t.{col if col != 'notice_date' else
                        'notice_date_key'}),
               count(*) FILTER (r.recorded_at < t.as_of)
        FROM today AS t LEFT JOIN recorded AS r
          ON r.contract_id = t.contract_id AND r.field_name = ?""",
        [col]).fetchone()
    print(f"  {col:<36}{now:>9,}{then:>8,}")
lo, mid, hi = con.execute("""
    SELECT min(d), median(d), max(d) FROM (
        SELECT date_diff('day', t.as_of, CAST(r.recorded_at AS DATE))
            AS d
        FROM today AS t JOIN recorded AS r USING (contract_id)
        WHERE r.field_name = 'cancellation_reason')""").fetchone()
print(f"reasons typed {lo} to {hi} days after the mark, median "
      f"{mid:.0f}")


TODAY = "SELECT * FROM today"
MARK = """SELECT s.* FROM features.renewal_snapshot AS s
          JOIN today USING (contract_id)"""
out = {}
print(f"{'a one-line rule for leaving':<30}{'flags':>7}{'right':>7}"
      f"{'found':>7}")
for label, rows, rule in (
        ("reason is filled in (today)", TODAY,
         "cancellation_reason IS NOT NULL"),
        ("on the Retention desk (today)", TODAY,
         "owner = 'Retention desk'"),
        ("on the Retention desk (mark)", MARK,
         "owner = 'Retention desk'")):
    hit, right = con.execute(f"""
        SELECT count(*) FILTER ({rule}),
               count(*) FILTER (({rule}) AND not_renewed)
        FROM ({rows})""").fetchone()
    out[label] = [hit, right, leavers]
    print(f"  {label:<28}{hit:>7,}{right:>7,}"
          f"{right / leavers:>7.0%}")
(moved,) = con.execute("""
    SELECT count(*) FROM today JOIN features.renewal_snapshot AS s
    USING (contract_id) WHERE today.segment <> s.segment""").fetchone()
print(f"segment today differs from the mark's: {moved:,} rows")
# Contract 7332's dates, for the figure: what was known when.
mark, end, reason_at, handed = con.execute("""
    SELECT s.as_of, s.end_date,
           (SELECT min(recorded_at) FROM recorded AS r
            WHERE r.contract_id = 7332
              AND r.field_name = 'cancellation_reason'),
           (SELECT min(valid_from) FROM marts.dim_account AS a
            WHERE a.account_id = s.account_id AND a.valid_from > s.as_of
              AND a.owner = 'Retention desk')
    FROM features.renewal_snapshot AS s
    WHERE s.contract_id = 7332""").fetchone()
out["7332"] = {"mark": str(mark), "end": str(end),
               "reason_at": str(reason_at), "handed": str(handed)}
Path(__file__).with_suffix(".json").write_text(json.dumps(out))
con.close()
tidy("warehouse.duckdb")
