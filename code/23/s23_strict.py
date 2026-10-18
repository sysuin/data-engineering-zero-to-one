# Exercise 4: Chapter 16's faster fct_order_lines, built beside main and
# held to --strict: a change that promises to change nothing.
# timeout: 900
import shutil

from _sandbox import built, tidy

from groundwork import diff

faster = open("code/16/models/fct_order_lines.sql").read()
current = open("dbt/groundwork/models/marts/fct_order_lines.sql").read()
main, _ = built("main")
fast, ran = built("faster", ("models/marts/fct_order_lines.sql",
                             current, faster))
print(f"dbt build: {ran.get('success', 0)} built, {ran.get('pass', 0)}"
      f" tests passed")
code = diff.main([str(main), str(fast), "--schema", "marts",
                  "--strict"])
print(f"--strict exit code: {code}")
tidy("faster")
shutil.rmtree("data/sandbox/ch23/faster", ignore_errors=True)
