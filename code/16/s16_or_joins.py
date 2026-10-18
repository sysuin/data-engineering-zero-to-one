# Exercise 2: Chapter 12's OR-form range joins, inner and left.
from _plans import connect, explain, name, operators

con = connect()
JOINS = {
    "01 lines to owner": """
        SELECT count(*) FROM marts.fct_order_lines AS f
        JOIN marts.dim_account AS a USING (account_key)
        {kind} JOIN marts.dim_account AS v
          ON v.account_id = a.account_id
         AND f.order_date >= v.valid_from
         AND (f.order_date < v.valid_to OR v.valid_to IS NULL)""",
    "08 nights to version": """
        SELECT count(*) FROM staging.stg_relay__accounts AS s
        {kind} JOIN marts.dim_account AS v
          ON v.account_id = s.account_id
         AND s.snapshot_date >= v.valid_from
         AND (s.snapshot_date < v.valid_to OR v.valid_to IS NULL)""",
    "12 list rows to cost": """
        SELECT count(*) FROM staging.stg_suppliers__price_lists AS p
        {kind} JOIN marts.dim_product AS v ON v.sku = p.sku
         AND p.effective_date >= v.valid_from
         AND (p.effective_date < v.valid_to OR v.valid_to IS NULL)""",
}


def shape(plan):
    """The join that applies the OR, and whether a FILTER does it."""
    ops = [name(n) for _, n in operators(plan)]
    if "BLOCKWISE_NL_JOIN" in ops:
        return "BLOCKWISE_NL_JOIN"
    return "HASH_JOIN + FILTER" if "FILTER" in ops else "HASH_JOIN"


print(f"{'Chapter 12 listing':<22}{'inner, as written':<22}LEFT JOIN")
for label, sql in JOINS.items():
    found = [shape(explain(con, sql.format(kind=kind))["physical_plan"])
             for kind in ("", "LEFT")]
    print(f"{label:<22}{found[0]:<22}{found[1]}")
