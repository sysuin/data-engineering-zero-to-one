# Exercise 3: a feature from a column recorded after the mark passes
# the cut test; the audit's recorded times catch it.
# timeout: 300
import duckdb

from groundwork.serve import features

from _serve import tidy, warehouse

con = duckdb.connect(str(warehouse()))
leaky = features.SNAPSHOT.replace(
    "       r.term_months, r.legacy_terms, r.discount_pct,",
    "       r.term_months, r.legacy_terms, r.discount_pct,\n"
    "       date_diff('day', r.mark, r.notice) AS days_to_notice,"
).replace("c.outcome\n    FROM marts.fct_contracts",
          "c.outcome,\n           strptime(CAST(c.notice_date_key AS "
          "VARCHAR), '%Y%m%d')::DATE AS notice\n"
          "    FROM marts.fct_contracts")
features.SNAPSHOT = leaky
features.build(con)
problems = features.check(con, cohorts=12)
print(f"check(): {problems or 'every promise kept'}")
(n,) = con.execute("""SELECT count(days_to_notice)
                      FROM features.renewal_snapshot""").fetchone()
late = con.execute("""
    SELECT count(*) FROM features.renewal_snapshot AS s
    JOIN staging.stg_relay__field_history AS h
      ON h.record_type = 'contract' AND h.record_id = s.contract_id
     AND h.field_name = 'notice_date'
    WHERE s.days_to_notice IS NOT NULL
      AND h.changed_at >= s.as_of""").fetchone()[0]
print(f"rows with days_to_notice {n:,}; recorded on or after the mark "
      f"{late:,}")
con.close()
tidy("warehouse.duckdb")
