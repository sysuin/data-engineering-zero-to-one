# fct_order_lines built by the runner: its rows set beside the lines
# they came from, and one order as the fact table holds it.
# timeout: 600
from _facts import built

con = built()
one = lambda sql: con.execute(sql).fetchone()[0]          # noqa: E731
staged = one("""SELECT count(*) FROM staging.stg_tradeline__order_lines
                JOIN staging.stg_tradeline__orders USING (order_id)
                WHERE status = 'placed'""")
facts = one("SELECT count(*) FROM marts.fct_order_lines")
print(f"placed lines in staging {staged:>10,}")
print(f"rows in fct_order_lines {facts:>10,}")
print(f"key (order_id, line_no) unique: "
      f"{one('SELECT count(DISTINCT (order_id, line_no)) '
             'FROM marts.fct_order_lines') == facts}")

head = con.execute("""
    SELECT order_id, order_number, date_key, account_key, region_key,
           channel, placed_at, order_date
    FROM marts.fct_order_lines WHERE order_number = 60603
    LIMIT 1""").fetchone()
print(f"\norder {head[1]} (Tradeline id {head[0]}), {head[5]}")
print(f"  placed_at {head[6]} UTC, order_date {head[7]}")
print(f"  date_key {head[2]}  account_key {head[3]}")
print(f"  region_key {head[4]}")
print(f"{'line':>4} {'product_key':<17}{'qty':>4}{'price':>8}"
      f"{'cost':>8}{'revenue':>9}{'margin':>9}")
for r in con.execute("""
        SELECT line_no, product_key, qty, unit_price, unit_cost,
               revenue, gross_margin
        FROM marts.fct_order_lines WHERE order_number = 60603
        ORDER BY line_no""").fetchall():
    print(f"{r[0]:>4} {r[1]:<17}{r[2]:>4}{r[3]:>8}{r[4]:>8}"
          f"{r[5]:>9}{r[6]:>9}")
