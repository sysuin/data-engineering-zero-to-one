# Where the time goes in 03_map_reduce.py, with and without a combiner.
# nondeterministic: task timings differ from run to run
from importlib import import_module

mr = import_module("03_map_reduce")      # the job in 03_map_reduce.py

PHASES = [("read the input", ["read_s"], "maps"),
          ("map (key, partition)", ["map_s"], "maps"),
          ("shuffle: write", ["write_s"], "maps"),
          ("shuffle: fetch", ["fetch_s"], "reds"),
          ("reduce (sum)", ["reduce_s"], "reds")]


def profile(combine):
    maps, reds = mr.run(4, 4, combine=combine)
    tasks = {"maps": maps, "reds": reds}
    secs = {name: sum(t[k] for t in tasks[side] for k in keys)
            for name, keys, side in PHASES}
    records = sum(r["records_in"] for r in reds)
    mb = sum(sum(m["bytes"]) for m in maps) / 1e6
    return secs, records, mb


if __name__ == "__main__":
    plain, combined = profile(False), profile(True)
    print(f"{'task-seconds':<24}{'no combiner':>14}{'combiner':>12}")
    for name, _, _ in PHASES:
        print(f"{name:<24}{plain[0][name]:>14.2f}"
              f"{combined[0][name]:>12.2f}")
    print(f"{'records shuffled':<24}{plain[1]:>14,}{combined[1]:>12,}")
    print(f"{'MB shuffled':<24}{plain[2]:>14.1f}{combined[2]:>12.1f}")
    for label, (secs, _, _) in (("no combiner", plain),
                                ("combiner", combined)):
        shuffle = secs["shuffle: write"] + secs["shuffle: fetch"]
        work = sum(secs.values()) - secs["read the input"]
        print(f"{label}: shuffle is {shuffle / work:.0%} of the work "
              "after the read")
    secs = plain[0]
    shuffle = secs["shuffle: write"] + secs["shuffle: fetch"]
    compute = secs["map (key, partition)"] + secs["reduce (sum)"]
    print(f"without a combiner, shuffle > map and reduce together: "
          f"{shuffle > compute}")
    print(f"records shuffled fell {plain[1] / combined[1]:.0f}-fold")
