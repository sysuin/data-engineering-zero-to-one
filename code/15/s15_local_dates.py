# Exercise 4: a test that recomputes the depot's day from placed_at and
# the region's calendar, run on the build and on a copy dated by UTC.
from _sandbox import connect, tidy, work

TEST = """
    SELECT count(*) FROM marts.fct_order_lines AS f
    JOIN marts.dim_region AS r USING (region_key)
    WHERE f.order_date <> CAST((f.placed_at AT TIME ZONE 'UTC')
                               AT TIME ZONE r.timezone AS DATE)"""
path = work("local_dates")
con = connect(path, read_only=False)
print("lines on the wrong day, as built:",
      con.execute(TEST).fetchone()[0])
con.execute("""UPDATE marts.fct_order_lines
               SET order_date = CAST(placed_at AS DATE)""")
print("lines on the wrong day, dated by UTC:",
      f"{con.execute(TEST).fetchone()[0]:,}")
con.close()
tidy("local_dates")
