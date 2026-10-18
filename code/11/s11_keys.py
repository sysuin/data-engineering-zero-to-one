# Exercise 1: every staging model's grain, checked as a unique key
# against the real warehouse.
import duckdb

from groundwork.config import WAREHOUSE

KEYS = {
    "stg_tradeline__orders": "order_id",
    "stg_tradeline__order_lines": "order_id, line_no",
    "stg_frontline__tickets": "ticket_id",
    "stg_frontline__ticket_fields": "ticket_id, field_id",
    "stg_frontline__ticket_tags": "ticket_id, position",
    "stg_relay__accounts": "account_id, snapshot_date",
    "stg_relay__contracts": "contract_id, snapshot_date",
    "stg_relay__field_history": "changed_at, record_type, record_id, "
                                "field_name",
    "stg_relay__migration_map": "old_crm_id",
    "stg_suppliers__price_lists": "file_sha256, sku",
    "stg_catalogue__products": "sku",
    "stg_ledger__gl_revenue": "period_month, region_code, description",
    "stg_reference__regions": "region_id",
    "stg_reference__suppliers": "supplier_id",
    "stg_reference__price_events": "effective_date, scope",
}
con = duckdb.connect(str(WAREHOUSE), read_only=True)
for model, key in KEYS.items():
    rows, keys = con.execute(
        f"SELECT count(*), count(DISTINCT ({key})) "
        f"FROM staging.{model}").fetchone()
    print(f"{model:<30}{rows:>10,}{rows - keys:>6} repeated")
