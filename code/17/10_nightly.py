# timeout: 3600
# Groundwork v0.7's night for 4 January 2026, every real task, in a
# sandbox copy; then what the warehouse holds after it.
import duckdb
from _sandbox import env, tidy, work

from groundwork.orchestrate import runner
from groundwork.orchestrate.dag import nightly

MARTS = ("dim_account", "dim_product", "fct_order_lines", "fct_orders",
         "fct_tickets", "fct_contract_month")


def counts(path):
    con = duckdb.connect(str(path), read_only=True)
    got = {t: con.execute(f"SELECT count(*) FROM marts.{t}")
           .fetchone()[0] for t in MARTS}
    got["snapshot, current"] = con.execute(
        "SELECT count(*) FROM snapshots.snp_relay__accounts "
        "WHERE dbt_valid_to IS NULL").fetchone()[0]
    got["loads"] = {r[0] for r in con.execute(
        "SELECT load_id FROM audit.load_log").fetchall()}
    con.close()
    return got


if __name__ == "__main__":
    copy = work("10")
    wh = copy / "warehouse.duckdb"
    before = counts(wh)
    r = runner.run(nightly(), "2026-01-04", env=env(copy))
    print(r.report())
    print(f"dbt was told: night={r.day}")
    print(f"  since={r.since or 'none: no build in the history yet'}")

    after = counts(wh)
    new = sorted(after.pop("loads") - before.pop("loads"))
    con = duckdb.connect(str(wh), read_only=True)
    print(f"\nloads this run wrote to audit.load_log: {len(new)}")
    for source, n, ok in con.execute("""
        SELECT source, count(*), count(*) FILTER (status = 'succeeded')
        FROM audit.load_log WHERE list_contains(?, load_id)
        GROUP BY 1 ORDER BY 1""", [new]).fetchall():
        print(f"  {source:<10} {n:>2} loads, {ok:>2} succeeded")
    (pub,) = con.execute("SELECT run_id FROM audit.published WHERE "
                         "logical_date = DATE '2026-01-04'").fetchone()
    print(f"audit.published: 2026-01-04 by {pub}")
    for table, bad, said in (("quality_checks", "error", "errors"),
                             ("reconciliations", "differs", "differ")):
        at, n, k = con.execute(f"""
            SELECT max(run_at), count(*), count(*) FILTER (status = ?)
            FROM audit.{table}
            WHERE run_at = (SELECT max(run_at) FROM audit.{table})""",
                               [bad]).fetchone()
        print(f"audit.{table}: {n} rows at {at:%d %b %H:%M}, "
              f"{k} {said}")
    con.close()
    print(f"\n{'relation':<22}{'before':>11}{'after':>11}")
    for t in before:
        print(f"  {t:<20}{before[t]:>11,}{after[t]:>11,}")
    logs = sorted(p.name for p in (copy / "logs" / r.run_id).iterdir())
    print(f"logs/{r.run_id}/: {len(logs)} files, one a try")
    tidy("10")
