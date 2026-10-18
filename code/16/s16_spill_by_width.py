# Exercise 4: does a narrow sort fit where the wide one did not?
import duckdb
from _plans import HERE, tidy, warehouse

NARROW = ("SELECT order_date, order_id, line_no "
          "FROM marts.fct_order_lines "
          "ORDER BY order_date, order_id, line_no")
REJOIN = """
    SELECT f.* FROM (SELECT order_id, line_no, row_number() OVER (
                         ORDER BY order_date, order_id, line_no) AS rn
                     FROM marts.fct_order_lines) AS k
    JOIN marts.fct_order_lines AS f USING (order_id, line_no)
    ORDER BY k.rn"""
WIDE = ("SELECT * FROM marts.fct_order_lines "
        "ORDER BY order_date, order_id, line_no")


def fits(sql, limit="250MB"):
    """Run in a fresh connection held to the limit, no spilling."""
    con = duckdb.connect(config={"threads": 4, "memory_limit": limit,
                                 "temp_directory": ""})
    con.execute(f"ATTACH '{warehouse()}' AS warehouse (READ_ONLY)")
    con.execute("USE warehouse")
    try:
        con.execute(f"COPY ({sql}) TO '{HERE / 'result.parquet'}'")
        return "finished"
    except duckdb.OutOfMemoryException:
        return "out of memory"
    finally:
        con.close()


print("250MB, nowhere to spill")
for label, sql in (("the three sort columns", NARROW),
                   ("three, then every column", REJOIN),
                   ("every column", WIDE)):
    print(f"  {label:<26}{fits(sql)}")
tidy("result.parquet", "tmp_result.parquet")
