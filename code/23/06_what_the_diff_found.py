# The rows the diff named, followed: which contracts, what a reader
# of the discount now sees, and what Foresight's features now say.
# timeout: 900
import json

import duckdb
from _sandbox import CHANGE, built

from groundwork import diff
from groundwork.serve import features

main, _ = built("main")
branch, _ = built("branch", CHANGE)
for path in (main, branch):
    con = duckdb.connect(str(path))
    features.build(con)                  # Chapter 22's feature table
    con.close()

con = duckdb.connect()
con.execute(f"ATTACH '{main}' AS a (READ_ONLY)")
con.execute(f"ATTACH '{branch}' AS b (READ_ONLY)")
(changed, legacy, all_legacy) = con.execute("""
    SELECT count(*), count(*) FILTER (WHERE x.legacy_terms),
           (SELECT count(*) FROM a.marts.fct_contracts
            WHERE legacy_terms)
    FROM a.marts.fct_contracts AS x JOIN b.marts.fct_contracts AS y
        USING (contract_id)
    WHERE x.discount_pct IS DISTINCT FROM y.discount_pct""").fetchone()
print(f"contracts whose discount changed: {changed:,}; on legacy "
      f"terms: {legacy:,}\n  of {all_legacy:,} legacy contracts")

print("\ncontracts in force on 31 Dec 2025:")
for db, who in (("a", "main"), ("b", "branch")):
    n, avg, zero = con.execute(f"""
        SELECT count(*), avg(discount_pct),
               count(*) FILTER (WHERE discount_pct = 0)
        FROM {db}.marts.fct_contract_month
        WHERE month_end = '2025-12-31'""").fetchone()
    print(f"  {who:<7} {n:,} in force, mean discount {avg:.2f}%, "
          f"{zero:,} at 0%")

print("\ncontracts ended 2023 to 2025, by whether a discount is "
      "recorded:")
for db, who in (("a", "main"), ("b", "branch")):
    for label, cond in (("none recorded", "discount_pct IS NULL"),
                        ("recorded", "discount_pct IS NOT NULL")):
        n, gone = con.execute(f"""
            SELECT count(*), count(*) FILTER (WHERE outcome =
                                                   'not_renewed')
            FROM {db}.marts.fct_contracts
            WHERE {cond} AND outcome IS NOT NULL
              AND end_date BETWEEN '2023-01-01' AND '2025-12-31'
            """).fetchone()
        rate = f"{100 * gone / n:.1f}% not renewed" if n else ""
        line = f"  {who:<7} {label:<14} {n:>6,} contracts  {rate}"
        print(line.rstrip())
con.close()

key = {"features.renewal_snapshot": ("contract_id",)}
(f,) = diff.compare(main, branch, schemas=("features",), key_of=key)
print(f"\nfeatures.renewal_snapshot, Foresight's rows: {f.rows_a:,}; "
      f"changed {f.changed:,}\n  in {', '.join(f.columns)}")
with open("code/23/06_what_the_diff_found.json", "w") as out:
    json.dump({"changed": changed, "legacy": legacy,
               "features_changed": f.changed}, out)
