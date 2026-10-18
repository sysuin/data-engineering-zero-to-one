# What ref() and source() turn into: a model's SQL before and after the
# runner compiles it, and the graph's edges read out of the text.
from groundwork.transform import runner

models = runner.discover()
model = models["stg_catalogue__products"]
print(f"{model.path.name}: materialised {model.materialised}")
print(f"  refs    {list(model.refs)}")
print(f"  sources {[f'{s}.{t}' for s, t in model.sources]}")

written = model.sql.splitlines()
compiled = runner.compile_sql(model, models).splitlines()
print("\nThe lines compiling changed:")
for before, after in zip(written, compiled):
    if before != after:
        print(f"  {before}\n  -> {after}")

typo = runner.Model("stg_typo", "staging", model.path,
                    "SELECT * FROM {{ ref('stg_catalogue__product') }}",
                    "view", ("stg_catalogue__product",), ())
try:
    runner.compile_sql(typo, models)
except KeyError as e:
    print(f"\nA model with a typo in its ref:\n  KeyError: {e.args[0]}")
