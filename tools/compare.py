"""A/B compare ranking variants over a set of queries.

Define named variants (a rank-profile + feature overrides), run the same
queries through each, and print the top-N side by side so you can see how an
equation or weight change reorders results. This is the core loop for refining
search: change `doc.sd` (and redeploy) or change a variant's features here,
then eyeball the deltas.

Variants are read from a JSON file (--variants) or default to a small built-in
set. Queries come from --query (repeatable) or a --queries file (one per line).

Example:
  python tools/compare.py --query vitor --query "alex gleason" --topn 5
  python tools/compare.py --queries tools/queries.sample.txt --variants my.json
"""
import argparse
import json
import os
import sys

_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(_ROOT, "brainstorm_server"))

from app.utils.observer import default_observer_pubkey  # noqa: E402

import searchlib  # noqa: E402

# A starter panel: production default vs. a few weight tweaks. Edit freely or
# pass your own via --variants (same JSON shape: {name: {ranking, features}}).
DEFAULT_VARIANTS = {
    "prod": {
        "ranking": "name_and_quality_score_only",
        "features": {"w_gram": 5.0, "w_about": 0.5},
    },
    "more_gram": {
        "ranking": "name_and_quality_score_only",
        "features": {"w_gram": 15.0, "w_about": 0.5},
    },
    "less_about": {
        "ranking": "name_and_quality_score_only",
        "features": {"w_gram": 5.0, "w_about": 0.1},
    },
    "search_rank": {
        "ranking": "search_rank",
        "features": {},
    },
}


def _short(row: dict) -> str:
    label = (row["display_name"] or row["name"] or "?")[:22]
    us = row["user_score"]
    us_s = f"{us:.0f}" if isinstance(us, (int, float)) else "-"
    return f"{label} (q={us_s})"


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--query", action="append", default=[])
    ap.add_argument("--queries", default=None, help="file with one query per line")
    ap.add_argument("--variants", default=None, help="JSON {name:{ranking,features}}")
    ap.add_argument("--observer", default=None)
    ap.add_argument("--topn", type=int, default=5)
    args = ap.parse_args()

    observer = args.observer or default_observer_pubkey()

    queries = list(args.query)
    if args.queries:
        with open(args.queries) as fh:
            queries += [
                ln.strip()
                for ln in fh
                if ln.strip() and not ln.lstrip().startswith("#")
            ]
    if not queries:
        ap.error("provide at least one --query or a --queries file")

    if args.variants:
        with open(args.variants) as fh:
            variants = json.load(fh)
    else:
        variants = DEFAULT_VARIANTS

    names = list(variants)
    for q in queries:
        print("=" * 100)
        print(f"QUERY: {q!r}   (observer {observer[:12]}…)")
        print("=" * 100)
        columns: dict[str, list[str]] = {}
        for name in names:
            v = variants[name]
            root = searchlib.raw_search(
                q, observer, v.get("ranking", "name_and_quality_score_only"),
                v.get("features", {}), hits=args.topn,
            )
            rows = searchlib.hits_table(root)
            columns[name] = [_short(r) for r in rows[: args.topn]]

        width = 34
        header = "".join(f"{n[:width-2]:<{width}}" for n in names)
        print(f"{'rank':<6}{header}")
        for i in range(args.topn):
            line = f"{i+1:<6}"
            for n in names:
                cell = columns[n][i] if i < len(columns[n]) else ""
                line += f"{cell[:width-2]:<{width}}"
            print(line)
        print()


if __name__ == "__main__":
    main()
