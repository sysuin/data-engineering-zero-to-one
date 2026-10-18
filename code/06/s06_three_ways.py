# Exercise 6.1: the appending monthly job made idempotent three ways,
# and what each does when the batch changes before the retry.
from _monthly import report, scheduler, stage, warehouse

LINES = ["order_id", "line_no"]


def delete_insert(con, table, batch, key, month):
    con.execute("BEGIN")
    con.execute(f"DELETE FROM {table} WHERE month = ?", [month])
    (n,) = con.execute(f"INSERT INTO {table} FROM {batch}").fetchone()
    con.execute("COMMIT")
    return f"replaced with {n:,}"


def merge(con, table, batch, key, month):
    cols = [c for (c,) in con.execute(
        f"SELECT name FROM pragma_table_info('{table}')").fetchall()
        if c not in key + ["month"]]
    acts = con.execute(f"""
        MERGE INTO {table} AS t USING {batch} AS s
        ON {' AND '.join(f't.{k} = s.{k}' for k in key)}
        WHEN MATCHED AND ({' OR '.join(
            f't.{c} IS DISTINCT FROM s.{c}' for c in cols)})
        THEN UPDATE SET {', '.join(f'{c} = s.{c}' for c in cols)}
        WHEN NOT MATCHED THEN INSERT BY NAME
        RETURNING merge_action""").fetchall()
    return f"changed {len(acts):,}"


def insert_new(con, table, batch, key, month):
    on = " AND ".join(f"t.{k} = s.{k}" for k in key)
    (n,) = con.execute(f"""INSERT INTO {table} SELECT * FROM {batch} s
        WHERE NOT EXISTS (SELECT 1 FROM {table} t WHERE {on})
        """).fetchone()
    return f"inserted {n:,}"


for way in (delete_insert, merge, insert_new):
    print(f"{way.__name__}:")
    con = warehouse()
    stage(con, "2025-11")

    def load_orders(con, month):
        return way(con, "orders", "new_orders", ["id"], month)

    def load_lines(con, month):
        return way(con, "order_lines", "new_lines", LINES, month)

    scheduler(con, load_orders, "2025-11")
    scheduler(con, load_lines, "2025-11", slow_first=True)
    report(con)
    # Before a retry, the source corrects one line's quantity.
    con.execute("UPDATE new_lines SET qty = qty + 1 WHERE order_id = "
                "458977 AND line_no = 1")
    scheduler(con, load_lines, "2025-11")
    report(con)
    con.close()
