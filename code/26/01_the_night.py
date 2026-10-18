# Groundwork v1.0's night: Chapter 17's DAG with six tasks added, and
# what each waits for.
import textwrap

from groundwork.nightly import nightly
from groundwork.orchestrate.dag import nightly as chapter17

dag, before = nightly(), chapter17()
new = set(dag.tasks) - set(before.tasks)
levels = dag.levels()
ingests = {n for n in dag.tasks if n.startswith("ingest_")}
print(f"{dag.name}: {len(before.tasks)} tasks in Chapter 17, "
      f"{len(dag.tasks)} now")
print(f"{'level':<7}{'task':<18}waits for")
for name in dag.order():
    ups = set(dag.tasks[name].upstream)
    said = sorted(ups - ingests) + (["every ingest"] if ups >= ingests
                                    and len(ingests) > 1 else
                                    sorted(ups & ingests))
    mark = "+" if name in new else " "
    head = f"  {levels[name]:<4}{mark} {name:<17}"
    print(textwrap.fill(", ".join(said) or "-", 66,
                        initial_indent=head,
                        subsequent_indent=" " * len(head)))
print(f"+ added at v1.0: {len(new)}; levels "
      f"{max(levels.values()) + 1}, edges {len(dag.edges())}")
