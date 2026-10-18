# Pemberton's June 2024 list, changed in each of the ways a producer
# changes a file: the variants listings 04 and 07 feed to readers.
from groundwork.config import LANDING

JUNE = (LANDING / "suppliers" / "pemberton-mills"
        / "PEMBERTON-PRICES-202406.csv")


def _edit(header, rows, kind):
    if kind == "added":                      # a new column at the end
        return header + ["Pack Size"], [r + ["12"] for r in rows]
    if kind == "removed":                    # the description dropped
        return ([h for h in header if h != "Item"],
                [[v for h, v in zip(header, r) if h != "Item"]
                 for r in rows])
    if kind == "renamed":                    # what July 2024 did
        return ([("Unit Cost USD" if h == "Cost (USD)" else h)
                 for h in header], rows)
    if kind == "reordered":                  # first two swapped
        return ([header[1], header[0], *header[2:]],
                [[r[1], r[0], *r[2:]] for r in rows])
    if kind == "retyped":                    # the cost as text
        return header, [[*r[:2], "$" + r[2], *r[3:]] for r in rows]
    return header, rows


KINDS = ["as sent", "added", "removed", "renamed", "reordered",
         "retyped"]


def variant(kind: str) -> bytes:
    """June's list with one kind of change, as CSV bytes."""
    lines = JUNE.read_text().splitlines()
    header, rows = lines[0].split(","), [x.split(",") for x in lines[1:]]
    header, rows = _edit(header, rows, kind)
    return ("\n".join(",".join(r) for r in [header, *rows])
            + "\n").encode()


def first_contract():
    """The price-list contract as first written (version 1), before
    Pemberton's July 2024 header was agreed."""
    from dataclasses import replace

    from groundwork import contracts
    c = contracts.load("suppliers.price_lists")
    return replace(c, version=1, changes=c.changes[:1],
                   formats={k: v for k, v in c.formats.items()
                            if k != "pemberton-2024"})
