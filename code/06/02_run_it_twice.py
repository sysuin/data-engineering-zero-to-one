# The test every pipeline must pass: run it twice, and compare what the
# warehouse holds after each run.
import duckdb
from _first_pipeline import WAREHOUSE, dollars, extract, run, transform


def fingerprint(con):
    """Each table's rows and a hash of its contents, and November."""
    fp = {t: con.execute(f"SELECT count(*), sum(hash(t)) FROM {t} t")
          .fetchone() for t in ("orders", "order_lines")}
    fp["november"] = transform(con)
    return fp


def run_twice(pipeline):
    """Run a pipeline twice on an empty warehouse; fingerprint both."""
    WAREHOUSE.unlink(missing_ok=True)
    con = duckdb.connect(str(WAREHOUSE))
    pipeline(con)
    first = fingerprint(con)
    pipeline(con)
    second = fingerprint(con)
    con.close()
    return first, second


def replace(con):
    """The same pipeline, loading by replacing each table whole."""
    for name, rows in extract().items():
        con.execute(f"CREATE OR REPLACE TABLE {name} AS FROM rows")
    return transform(con)


for name, pipeline in (("append", run), ("replace", replace)):
    first, second = run_twice(pipeline)
    print(f"{name}: {'passes' if first == second else 'FAILS'}")
    print(f"  {'':<18}{'after run 1':>18}{'after run 2':>18}")
    for t in ("orders", "order_lines"):
        print(f"  {t + ' rows':<18}{first[t][0]:>18,}"
              f"{second[t][0]:>18,}")
    print(f"  {'November':<18}{dollars(first['november'][2]):>18}"
          f"{dollars(second['november'][2]):>18}")
    print(f"  {'  its orders':<18}{first['november'][0]:>18,}"
          f"{second['november'][0]:>18,}")
