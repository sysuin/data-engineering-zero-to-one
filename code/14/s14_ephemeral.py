# int_order_lines made ephemeral, in a copy of the project: what dbt
# compiles fct_order_lines to, and who else was reading the view.
# timeout: 900
import shutil
import subprocess

from _dbt import HERE, PROJECT, built, copy, dbt, tidy

project = HERE / "project"
shutil.rmtree(project, ignore_errors=True)
shutil.copytree(PROJECT, project, ignore=shutil.ignore_patterns(
    "target", "logs"))
model = project / "models" / "intermediate" / "int_order_lines.sql"
model.write_text(model.read_text().replace(
    "materialized='view'", "materialized='ephemeral'"))

path = copy("ephemeral", source=built())
res = dbt("compile", "--select", "fct_order_lines", path=path,
          project=project)
sql = res.result.results[0].node.compiled_code
print("fct_order_lines, compiled with int_order_lines ephemeral:")
print(f"  reads intermediate.int_order_lines: "
      f"{'\"intermediate\".\"int_order_lines\"' in sql}")
print(f"  carries it as a CTE named: "
      f"{[w for w in sql.split() if w.startswith('__dbt__cte__')][0]}")
print(f"  compiled lines: {len(sql.splitlines())}")
res = dbt("ls", "--select", "config.materialized:ephemeral",
          "--output", "name", path=path, project=project)
print(f"ephemeral models: {res.result}")

# Groundwork's Python as Chapter 14 leaves it; later chapters add
# modules (Chapter 23's diff.py reads the view too).
CHAPTER_14 = ["config.py", "dates.py", "ddl", "profile.py",
              "storage.py", "loadlog.py", "ingest", "contracts",
              "transform", "dimensions.py", "identity.py", "facts.py"]
readers = subprocess.run(
    ["grep", "-rl", "intermediate.int_order_lines",
     *[f"code/groundwork/{m}" for m in CHAPTER_14]],
    capture_output=True, text=True).stdout.split()
print("\noutside dbt, read by:", ", ".join(sorted(
    r for r in readers if r.endswith(".py"))))
shutil.rmtree(project)
tidy("ephemeral")
