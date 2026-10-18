# A wide view for people, its columns documented in the catalogue,
# and the same pivot asked of the star and of the view.
# timeout: 300
import textwrap

import duckdb

from groundwork.serve import metrics

from _serve import tidy, warehouse

con = duckdb.connect(str(warehouse()))
metrics.publish(con)
(doc,) = con.execute("SELECT comment FROM duckdb_views() "
                     "WHERE view_name = 'sales'").fetchone()
print(textwrap.fill(f"metrics.sales: {doc}", 68))
for name, typ, doc in con.execute("""
        SELECT column_name, data_type, comment FROM duckdb_columns()
        WHERE schema_name = 'metrics' AND table_name = 'sales'
        ORDER BY column_index""").fetchall():
    print(f"  {name:<16}{typ:<14}{doc}")

STAR = """
    SELECT a.segment, p.category, sum(f.revenue)
    FROM marts.fct_order_lines AS f
    JOIN marts.dim_region AS r USING (region_key)
    JOIN marts.dim_account AS a USING (account_key)
    JOIN marts.dim_product AS p USING (product_key)
    WHERE r.region_name = 'Northeast' AND year(f.order_date) = 2025
    GROUP BY ALL ORDER BY ALL"""
WIDE = """
    SELECT segment, category, sum(revenue) FROM metrics.sales
    WHERE region = 'Northeast' AND year(order_date) = 2025
    GROUP BY ALL ORDER BY ALL"""
star, wide = con.execute(STAR).fetchall(), con.execute(WIDE).fetchall()
print(f"Northeast 2025 by segment and category: {len(star)} cells "
      f"from the star,\n  {len(wide)} from the view, "
      f"{sum(a != b for a, b in zip(star, wide))} differ; total "
      f"${sum(r[2] for r in wide):,}")
con.close()
tidy("warehouse.duckdb")
