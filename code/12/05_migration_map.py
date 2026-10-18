# IT's migration map beside the rule's matches, and the orders and
# tickets that point at old ids, resolved with the map and without it.
from _dims import built, tidy

con = built("map")
print(con.execute("""
    SELECT 'arrived ' || strftime(arrived_at, '%-d %B %Y %H:%M')
           || ' UTC, ' || rows || ' rows, matched_by '
    FROM audit.file_ledger WHERE table_name = 'migration_map'
""").fetchone()[0] + ", ".join(r[0] for r in con.execute(
    "SELECT DISTINCT matched_by FROM staging.stg_relay__migration_map"
).fetchall()))
for how, n in con.execute(
        "SELECT matched_by, count(*) "
        "FROM intermediate.int_account_identity "
        "GROUP BY ALL ORDER BY 2 DESC").fetchall():
    print(f"  int_account_identity: {how:<6} {n:>6,} ids")

uses = """
    SELECT '{what}' AS what, count(*) AS n,
           count(*) FILTER (i.matched_by = 'rule') AS map_misses,
           count(DISTINCT x.account_id) AS ids,
           max({when}) AS last
    FROM {table} AS x
    JOIN intermediate.int_account_identity AS i
        ON i.crm_id = x.account_id
    WHERE i.matched_by <> 'relay' {only}"""
rows = con.execute(
    uses.format(what="orders", table="staging.stg_tradeline__orders",
                when="placed_at", only="AND x.status = 'placed'")
    + " UNION ALL " +
    uses.format(what="tickets", table="staging.stg_frontline__tickets",
                when="created_at", only="")).fetchall()
print(f"\n{'on old ids':<10}{'rows':>8}{'ids':>9}{'last':>18}"
      f"{'map alone':>12}")
for what, n, misses, ids, last in rows:
    print(f"{what:<10}{n:>8,}{ids:>9,}{f'{last:%Y-%m-%d %H:%M}':>18}"
          f"{misses:>8,} lost")
(cents,) = con.execute("""
    SELECT sum(l.qty * l.unit_price_cents)
    FROM staging.stg_tradeline__orders AS o
    JOIN staging.stg_tradeline__order_lines AS l USING (order_id)
    JOIN intermediate.int_account_identity AS i
        ON i.crm_id = o.account_id
    WHERE i.matched_by = 'rule' AND o.status = 'placed'""").fetchone()
print(f"placed sales on the ids the map misses: ${cents / 100:,.2f}")
(n,) = con.execute("SELECT count(*) "
                   "FROM intermediate.int_account_identity "
                   "WHERE account_id IS NULL").fetchone()
print(f"ids unresolved with the map and the seed together: {n}")
tidy("map")
