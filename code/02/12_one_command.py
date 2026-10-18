# Copy one night of Relay's export into Parquet: a script, not a cell.
import argparse
import sys

import duckdb

from groundwork.config import LANDING, SANDBOX

RELAY = LANDING / "relay"


def latest_night():
    """The newest export folder in the landing zone."""
    return max(p.name for p in RELAY.glob("20*") if p.is_dir())


def copy_night(night, dest):
    """Write each CSV of one night as Parquet; return row counts."""
    folder = RELAY / night
    if not folder.is_dir():
        raise FileNotFoundError(f"no Relay export for {night} "
                                f"in {folder}")
    dest.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect()
    counts = {}
    for csv in sorted(folder.glob("*.csv")):
        target = dest / f"{csv.stem}.parquet"
        source = f"read_csv('{csv}', all_varchar = true)"   # as sent
        con.execute(f"COPY (SELECT * FROM {source}) TO '{target}' "
                    "(FORMAT parquet)")
        (read,) = con.execute(
            f"SELECT count(*) FROM {source}").fetchone()
        (wrote,) = con.execute(
            f"SELECT count(*) FROM '{target}'").fetchone()
        counts[csv.name] = (read, wrote)
    con.close()
    return counts


def main(argv=None):
    parser = argparse.ArgumentParser(
        description="Copy a night of Relay's export to Parquet.")
    parser.add_argument("--night", default=latest_night(),
                        help="YYYY-MM-DD; default: the newest on disk")
    args = parser.parse_args(argv)

    dest = SANDBOX / "ch02" / "relay_parquet" / args.night
    counts = copy_night(args.night, dest)
    ok = True
    for name, (read, wrote) in counts.items():
        ok = ok and read == wrote
        print(f"relay {args.night} {name:13} read {read:>6,} "
              f"wrote {wrote:>6,}")
    print("reconciled" if ok else "MISMATCH")
    return 0 if ok else 1


if __name__ == "__main__":          # run as a command, not on import
    sys.exit(main())
