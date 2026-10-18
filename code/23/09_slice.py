# A slice of production for test data: thirty accounts drawn at random
# against twenty chosen, case by case, for what each holds.
# timeout: 600
import random

import duckdb
from _sandbox import built

from groundwork.config import SEED

main, _ = built("main")
con = duckdb.connect(str(main), read_only=True)
CASES = {        # a case: the accounts that have it, by Relay's id
    "orders under an old CRM id": """
        SELECT account_id FROM intermediate.int_account_identity
        WHERE matched_by IN ('it_map', 'rule')""",
    "  ...one IT's map missed": """
        SELECT account_id FROM intermediate.int_account_identity
        WHERE matched_by = 'rule'""",
    "a cancelled order": """
        SELECT i.account_id FROM staging.stg_tradeline__orders AS o
        JOIN intermediate.int_account_identity AS i
            ON i.crm_id = o.account_id WHERE o.status = 'cancelled'""",
    "an order on the depot's day before": """
        SELECT a.account_id FROM marts.fct_orders AS f
        JOIN marts.dim_account AS a USING (account_key)
        WHERE CAST(f.placed_at AS DATE) <> f.order_date""",
    "a contract on legacy terms": """
        SELECT a.account_id FROM marts.fct_contracts AS c
        JOIN marts.dim_account AS a USING (account_key)
        WHERE c.legacy_terms""",
    "a contract not renewed": """
        SELECT a.account_id FROM marts.fct_contracts AS c
        JOIN marts.dim_account AS a USING (account_key)
        WHERE c.outcome = 'not_renewed'""",
    "handed to the Retention desk": """
        SELECT account_id FROM marts.dim_account
        WHERE is_current AND owner = 'Retention desk'""",
    "a segment that changed": """
        SELECT account_id FROM marts.dim_account
        GROUP BY account_id HAVING count(DISTINCT segment) > 1""",
    "a depot in the West": """
        SELECT a.account_id FROM marts.dim_account AS a
        JOIN marts.dim_region AS r USING (region_key)
        WHERE a.is_current AND r.region_name = 'West'""",
}
having = {case: {a for (a,) in con.execute(sql).fetchall()
                 if a is not None and a > 0}
          for case, sql in CASES.items()}
accounts = sorted(a for (a,) in con.execute(
    "SELECT account_id FROM marts.dim_account WHERE is_current "
    "AND account_id > 0").fetchall())

drawn = set(random.Random(SEED).sample(accounts, 30))
chosen: set[int] = set()
for case, ids in sorted(having.items(), key=lambda kv: len(kv[1])):
    for a in sorted(ids - chosen)[:2]:   # two of each, unless some of
        if len(ids & chosen) < 2:        # those already chosen have it
            chosen.add(a)

print(f"{'case':<36}{'accounts':>9}{'drawn':>7}{'chosen':>8}")
for case, ids in having.items():
    print(f"{case:<36}{len(ids):>9,}{len(ids & drawn):>7}"
          f"{len(ids & chosen):>8}")
missed = sum(not (ids & drawn) for ids in having.values())
print(f"{'accounts in the slice':<36}{len(accounts):>9,}{len(drawn):>7}"
      f"{len(chosen):>8}")
for name, ids in (("drawn", drawn), ("chosen", chosen)):
    (n,) = con.execute(
        "SELECT count(*) FROM marts.fct_orders JOIN marts.dim_account "
        "USING (account_key) WHERE account_id IN (SELECT unnest(?))",
        [sorted(ids)]).fetchone()
    print(f"orders, {name}: {n:,}")
con.close()
print(f"cases the drawn slice misses: {missed} of {len(having)}")
