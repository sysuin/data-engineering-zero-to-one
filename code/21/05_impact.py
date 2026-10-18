# Impact analysis: what a change to one source column would reach,
# following the values and then the rows a column chooses.
import textwrap

from _sandbox import compiled

from groundwork.observe import lineage, nightly

manifest, schema = compiled()
cols = lineage.columns(manifest, schema)
tables = lineage.tables(nightly(), manifest)


def plural(n, word):
    return f"{n} {word}{'s' * (n != 1)}"


def report(column):
    print(column)
    for rows in (False, True):
        i = lineage.impact(cols, tables, column, manifest, rows=rows)
        how = "values and rows" if rows else "values only"
        print(f"  {how}: {plural(len(i['columns']), 'column')} in "
              f"{plural(len(i['relations']), 'relation')}, "
              f"{plural(len(i['tests']), 'test')}")
        for r, names in i["by_relation"].items():
            if r.startswith("marts."):
                print(textwrap.fill(f"{r}: {', '.join(names)}", 66,
                                    initial_indent="    ",
                                    subsequent_indent="      "))
        if not rows:
            print(textwrap.fill("read by " + ", ".join(
                t.removeprefix("test:") for t in i["tests"]), 66,
                initial_indent="    ", subsequent_indent="      "))
    o = i["owners"]
    print(f"  tell: {o['producer']},\n        {o['contact']}")


report("tradeline.order_lines.unit_price_cents")
print()
report("suppliers.price_lists.cost")
