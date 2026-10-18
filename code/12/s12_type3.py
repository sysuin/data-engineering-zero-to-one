# Exercise 4: a Type 3 column, each account's previous manager, made
# from dim_account's versions; and what it can and cannot answer.
from _dims import built, tidy

con = built("s_type3")
rows = con.execute("""
    WITH owners AS (
        SELECT account_id, valid_from, owner,
               (owner IS DISTINCT FROM lag(owner) OVER w) AS changed
        FROM marts.dim_account WHERE account_id > 0
        WINDOW w AS (PARTITION BY account_id ORDER BY valid_from)
    ), changes AS (
        SELECT account_id, valid_from, owner,
               lag(owner) OVER (PARTITION BY account_id
                                ORDER BY valid_from) AS previous
        FROM owners WHERE changed
    )
    SELECT account_id, owner, previous FROM changes
    QUALIFY valid_from = max(valid_from) OVER (PARTITION BY account_id)
""").fetchall()
desk = [r for r in rows if r[1] == "Retention desk"]
print(f"accounts {len(rows):,}; with a previous manager "
      f"{sum(r[2] is not None for r in rows):,}")
print(f"on the Retention desk today {len(desk):,}; previous manager "
      f"known for {sum(r[2] is not None for r in desk):,}")
(more,) = con.execute("""
    SELECT count(*) FROM (
        SELECT account_id FROM marts.dim_account WHERE account_id > 0
        GROUP BY 1 HAVING count(DISTINCT owner) > 2)""").fetchone()
print(f"accounts with three or more managers, which one column of "
      f"'previous'\ncannot describe: {more:,}")
tidy("s_type3")
