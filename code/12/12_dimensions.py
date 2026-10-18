# The Build in a sandbox: every dimension made by the runner and
# dates.py, held to marts.sql, each count beside a second source.
import sqlite3
import sys
from datetime import date

from groundwork import dimensions
from groundwork.config import BOOK2_WAREHOUSE, ROOT

from _dims import built, tidy

sys.path.insert(0, str(ROOT / "code" / "meridian"))
import verify_sources  # noqa: E402  Appendix F's four rules

con = built("build")
one = lambda sql: con.execute(sql).fetchone()[0]          # noqa: E731
latest = ("(SELECT max(snapshot_date) "
          "FROM staging.stg_relay__accounts)")
checks = [
    ("dim_date days", one("SELECT count(*) FROM marts.dim_date"),
     "the calendar, 2021-2026",
     (date(2026, 12, 31) - date(2021, 1, 1)).days + 1),
    ("dim_region regions", one("SELECT count(*) FROM marts.dim_region "
                               "WHERE region_id > 0"),
     "regions.csv", one("SELECT count(*) FROM staging."
                        "stg_reference__regions")),
    ("dim_supplier suppliers", one("SELECT count(*) FROM marts.dim_"
                                   "supplier WHERE supplier_id > 0"),
     "suppliers.csv", one("SELECT count(*) FROM staging."
                          "stg_reference__suppliers")),
    ("dim_product SKUs", one("SELECT count(DISTINCT sku) FROM marts."
                             "dim_product WHERE sku <> 'UNKNOWN'"),
     "the catalogue", one("SELECT count(*) FROM staging."
                          "stg_catalogue__products")),
    ("  list rows at their cost", one("""
        SELECT count(*) FROM staging.stg_suppliers__price_lists AS p
        JOIN marts.dim_product AS v ON v.sku = p.sku
         AND p.effective_date >= v.valid_from
         AND (p.effective_date < v.valid_to OR v.valid_to IS NULL)
        WHERE v.unit_cost * 100 = p.unit_cost_cents"""),
     "price-list rows", one("SELECT count(*) FROM staging."
                            "stg_suppliers__price_lists")),
    ("dim_account current", one("SELECT count(*) FROM marts."
                                "dim_account WHERE is_current "
                                "AND account_id > 0"),
     "today's export", one("SELECT count(*) FROM staging."
                           f"stg_relay__accounts WHERE snapshot_date = "
                           f"{latest}")),
    ("ids resolved", one("SELECT count(account_id) FROM "
                         "intermediate.int_account_identity"),
     "ids ever exported", one("SELECT count(DISTINCT account_id) FROM "
                              "staging.stg_relay__accounts")),
]
b2 = sqlite3.connect(f"file:{BOOK2_WAREHOUSE}?mode=ro", uri=True)
history = b2.execute("SELECT * FROM account_history ORDER BY "
                     "account_id, valid_from, rowid").fetchall()
expected, _ = verify_sources.book2_history_as_a_crm_saw_it(
    history, b2.execute("SELECT account_id, segment, account_manager "
                        "FROM accounts").fetchall())
ours = set(con.execute("""
    WITH m AS (
        SELECT *, (segment, owner) IS DISTINCT FROM
               lag((segment, owner)) OVER (
                   PARTITION BY account_id ORDER BY valid_from) AS new
        FROM marts.dim_account WHERE account_id > 0)
    SELECT account_id, strftime(valid_from, '%Y-%m-%d'),
           strftime(lead(valid_from) OVER (PARTITION BY account_id
                    ORDER BY valid_from) - 1, '%Y-%m-%d'),
           segment, owner
    FROM m WHERE new""").fetchall())
checks.append(("segment and owner history", len(ours),
               "Book 2, four rules", len(expected)))

print(f"{'built':<26}{'rows':>7}  {'second source':<25}{'rows':>6}")
for name, n, other, m in checks:
    mark = "=" if n == m else "!"
    print(f"{name:<26}{n:>7,}  {other:<25}{m:>6,} {mark}")
print(f"Book 2's history {len(history):,} rows; ours and the four "
      f"rules' differ by {len(ours ^ expected)}")
problems = dimensions.check(con)
print("dimensions.check:", "every promise kept" if not problems
      else problems)
tidy("build")
