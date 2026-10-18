# Exercise 3: two guards against a join that multiplies rows.
from _plans import analyze, connect, scans

con = connect()
# (a) A singular test: rows returned mean the join changed the count.
GUARD = """
    SELECT f.n AS lines, j.n AS joined FROM
      (SELECT count(*) AS n FROM marts.fct_order_lines) AS f,
      (SELECT count(*) AS n FROM marts.fct_order_lines
       JOIN marts.dim_account USING (account_key)) AS j
    WHERE f.n <> j.n"""
# (b) Before the join: is the key unique on the side it should be?
UNIQUE = """
    SELECT {key}, count(*) FROM marts.dim_account
    GROUP BY {key} HAVING count(*) > 1"""
for label, sql in (("(a) join on account_key", GUARD),
                   ("(b) account_key unique", UNIQUE.format(
                       key="account_key")),
                   ("(b) account_id unique", UNIQUE.format(
                       key="account_id"))):
    rows = con.execute(sql).fetchall()
    read = sum(s["operator_rows_scanned"] for s in scans(
        analyze(con, sql)))
    print(f"{label:<26}{len(rows):>6} rows back{read:>12,} rows read")
