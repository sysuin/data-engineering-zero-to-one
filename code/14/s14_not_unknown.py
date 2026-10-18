# A generic test of Groundwork's own, in a copy of the project: no fact
# row may point at a dimension's unknown member. Run on the marts.
# timeout: 900
import shutil

import yaml

from _dbt import HERE, PROJECT, built, connect, copy, dbt, tidy

project = HERE / "project"
shutil.rmtree(project, ignore_errors=True)
shutil.copytree(PROJECT, project, ignore=shutil.ignore_patterns(
    "target", "logs"))
(project / "macros" / "test_not_unknown.sql").write_text("""\
-- Rows whose key is the unknown member's: counted, not hidden.
{% test not_unknown(model, column_name, dimension, where_unknown) %}
SELECT f.{{ column_name }}
FROM {{ model }} AS f
JOIN {{ dimension }} AS d USING ({{ column_name }})
WHERE d.{{ where_unknown }}
{% endtest %}
""")
marts = project / "models" / "marts" / "_marts.yml"
spec = yaml.safe_load(marts.read_text())
WHERE = {"account_key": ("dim_account", "account_id = -1"),
         "product_key": ("dim_product", "sku = 'UNKNOWN'")}
for model, columns in (("fct_order_lines", ["account_key"]),
                       ("fct_tickets", ["account_key", "product_key"])):
    m = next(m for m in spec["models"] if m["name"] == model)
    for col in columns:
        dim, where = WHERE[col]
        entry = next((c for c in m.setdefault("columns", [])
                      if c["name"] == col), None)
        if entry is None:
            entry = {"name": col}
            m["columns"].append(entry)
        entry.setdefault("data_tests", []).append({"not_unknown": {
            "arguments": {"dimension": f"ref('{dim}')",
                          "where_unknown": where},
            "config": {"severity": "warn"}}})
marts.write_text(yaml.safe_dump(spec, sort_keys=False))
path = copy("unknown", source=built())
res = dbt("test", "--select", "test_name:not_unknown", path=path,
          project=project)
for r in sorted(res.result.results, key=lambda r: r.node.name):
    model = r.node.attached_node.split(".")[-1]
    print(f"{model + "." + r.node.column_name:<30}{str(r.status):<6}"
          f"{r.failures:>6} on the unknown member")

con = connect(path)
skus, launched, first, last = con.execute("""
    SELECT count(DISTINCT t.sku), max(c.launched_date),
           min(CAST(t.created_at AS DATE)),
           max(CAST(t.created_at AS DATE))
    FROM marts.fct_tickets AS f
    JOIN marts.dim_product AS p USING (product_key)
    JOIN staging.stg_frontline__tickets AS t USING (ticket_id)
    JOIN staging.stg_catalogue__products AS c ON c.sku = t.sku
    WHERE p.sku = 'UNKNOWN'""").fetchone()
print(f"\nthose tickets name {skus} SKUs launched {launched}, and were "
      f"opened\nfrom {first} to {last}")
con.close()
shutil.rmtree(project)
tidy("unknown")
