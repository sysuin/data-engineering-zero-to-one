# The same night, loaded by replacing the month: delete its rows, then
# insert the batch, in one transaction.
from _monthly import report, save, scheduler, stage, warehouse


def replace_month(con, table, batch, month):
    con.execute("BEGIN")
    (gone,) = con.execute(f"DELETE FROM {table} WHERE month = ?",
                          [month]).fetchone()
    (n,) = con.execute(f"INSERT INTO {table} FROM {batch}").fetchone()
    con.execute("COMMIT")
    return f"deleted {gone:,}, inserted {n:,}"


def load_orders(con, month):
    return replace_month(con, "orders", "new_orders", month)


def load_lines(con, month):
    return replace_month(con, "order_lines", "new_lines", month)


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
print("the whole job again:")
scheduler(con, load_orders, "2025-11")
scheduler(con, load_lines, "2025-11")
report(con)
