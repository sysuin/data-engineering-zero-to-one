# Exercise 5: tickets joined to the account version valid on the day
# they were opened, against today's row.
from _dims import built, tidy

con = built("s_tickets")
rows = con.execute("""
    WITH t AS (
        SELECT f.ticket_id, i.account_id,
               CAST(timezone(r.timezone, timezone('UTC', f.created_at))
                    AS DATE) AS opened
        FROM staging.stg_frontline__tickets AS f
        JOIN intermediate.int_account_identity AS i
            ON i.crm_id = f.account_id
        JOIN marts.dim_account AS a
            ON a.account_id = i.account_id AND a.is_current
        JOIN marts.dim_region AS r USING (region_key)
    )
    SELECT count(*), count(v.account_id),
           count(*) FILTER (now.owner = 'Retention desk'),
           count(*) FILTER (v.owner = 'Retention desk')
    FROM t
    JOIN marts.dim_account AS now
        ON now.account_id = t.account_id AND now.is_current
    LEFT JOIN marts.dim_account AS v
        ON v.account_id = t.account_id AND t.opened >= v.valid_from
       AND (t.opened < v.valid_to OR v.valid_to IS NULL)""").fetchone()
n, matched, today, then = rows
print(f"tickets with an account {n:,}; joined to a version {matched:,}")
print(f"opened for the Retention desk: today's row says {today:,}, "
      f"the\nversion valid that day says {then:,}")
tidy("s_tickets")
