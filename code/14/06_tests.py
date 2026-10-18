# dbt's tests on the marts: all passing on the build, then four
# promises broken by hand in a copy, and what dbt test says of each.
# timeout: 1200
from collections import Counter

from _dbt import built, connect, copy, dbt, tidy


def kind(node):
    meta = getattr(node, "test_metadata", None)
    return meta.name if meta else "singular"


path = copy("tests", source=built())
res = dbt("test", path=path)
print(f"dbt test on the build: {len(res.result.results)} tests")
by = Counter((kind(r.node), str(r.status)) for r in res.result.results)
for (k, status), n in sorted(by.items()):
    print(f"  {k:<18}{n:>3} {status}")

con = connect(path, read_only=False)
con.execute("""INSERT INTO marts.fct_orders
               FROM marts.fct_orders WHERE order_number = 60603""")
con.execute("""UPDATE marts.fct_order_lines SET channel = 'Fax'
               WHERE order_number = 60603""")
con.execute("""UPDATE marts.fct_order_lines
               SET account_key = '0000000000000000'
               WHERE order_number = 60603 AND line_no = 1""")
con.execute("""UPDATE marts.dim_account SET segment = 'Franchise'
               WHERE account_id = 1130 AND is_current""")
con.close()

res = dbt("test", "--select", "marts", path=path, check=False)
print(f"\nafter four changes: dbt test succeeded: {res.success}")
print(", ".join(f"{k} {v}" for k, v in sorted(Counter(
    str(r.status) for r in res.result.results).items())))
for r in sorted(res.result.results, key=lambda r: r.node.name):
    if str(r.status) != "pass":
        model = (r.node.attached_node or "").split(".")[-1]
        column = r.node.column_name or ""
        if "||" in column:
            column = "(" + column.replace(" || '-' || ", ", ") + ")"
        print(f"  {str(r.status):<5}{r.failures:>3}  {kind(r.node):<16}"
              f"{model}.{column}")
tidy("tests")
