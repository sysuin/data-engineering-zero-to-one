# The same night, loaded by merging on the key: insert lines that are
# new, update lines that changed, leave the rest alone.
from _monthly import report, save, scheduler, stage, warehouse

MERGE = """
    MERGE INTO {table} AS t USING {batch} AS s
    ON {on}
    WHEN MATCHED AND ({changed}) THEN UPDATE SET {set}
    WHEN NOT MATCHED THEN INSERT BY NAME
    RETURNING merge_action"""


def merge(con, table, batch, key, columns):
    sql = MERGE.format(
        table=table, batch=batch,
        on=" AND ".join(f"t.{k} = s.{k}" for k in key),
        changed=" OR ".join(f"t.{c} IS DISTINCT FROM s.{c}"
                            for c in columns),
        set=", ".join(f"{c} = s.{c}" for c in columns))
    actions = [a for (a,) in con.execute(sql).fetchall()]
    return (f"inserted {actions.count('INSERT'):,}, "
            f"updated {actions.count('UPDATE'):,}")


def load_orders(con, month):
    return merge(con, "orders", "new_orders", ["id"],
                 ["placed_at", "status"])


def load_lines(con, month):
    return merge(con, "order_lines", "new_lines",
                 ["order_id", "line_no"], ["qty", "unit_price_cents"])


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
