# A monthly job appends each month's orders and lines. The lines task
# times out after it has committed, and the scheduler runs it again.
from _monthly import report, save, scheduler, stage, warehouse


def load_orders(con, month):
    con.execute("INSERT INTO orders FROM new_orders")
    return "appended"


def load_lines(con, month):
    (n,) = con.execute("INSERT INTO order_lines FROM new_lines"
                       ).fetchone()
    return f"appended {n:,}"


con = warehouse()
print("1 November, October's run:")
stage(con, "2025-10")
scheduler(con, load_orders, "2025-10")
scheduler(con, load_lines, "2025-10")
print("1 December, November's run:")
stage(con, "2025-11")
scheduler(con, load_orders, "2025-11")
scheduler(con, load_lines, "2025-11", slow_first=True)
report(con)
save(con, __file__)

(dupes,) = con.execute("""SELECT count(*) FROM (SELECT order_id, line_no
    FROM order_lines GROUP BY ALL HAVING count(*) > 1)""").fetchone()
print(f"order lines held twice: {dupes:,}")
print("had the whole job been retried, orders too:")
load_orders(con, "2025-11")
report(con)
