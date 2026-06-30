"""Run a single search against the local Vespa and print the ranked results.

This mirrors what the production `/byText` endpoint does (same YQL, same
`name_and_quality_score_only` rank-profile, same observer one-hot tensor), and
additionally prints the per-hit match-features so you can see *why* a result
ranks where it does — the quantities your equations combine.

Examples:
  python tools/search.py "vitor"
  python tools/search.py "vitor pamplona" --observer <hex> --hits 20
  python tools/search.py "alex" --ranking search_rank
  python tools/search.py "alex" --feature w_gram=10 --feature w_about=0.2
"""
import argparse
import os
import sys

_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(_ROOT, "brainstorm_server"))

from app.utils.observer import default_observer_pubkey  # noqa: E402

import searchlib  # noqa: E402


def _parse_features(items: list[str]) -> dict:
    out: dict = {}
    for it in items or []:
        if "=" not in it:
            raise SystemExit(f"--feature must be key=value, got {it!r}")
        k, v = it.split("=", 1)
        try:
            out[k] = float(v)
        except ValueError:
            out[k] = v
    return out


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("query")
    ap.add_argument("--observer", default=None)
    ap.add_argument("--ranking", default="name_and_quality_score_only")
    ap.add_argument("--hits", type=int, default=25)
    ap.add_argument("--feature", action="append", default=[],
                    help="override a ranking.features.query(...) input, e.g. "
                         "--feature w_gram=10")
    ap.add_argument("--yql", action="store_true", help="print the YQL and exit")
    args = ap.parse_args()

    observer = args.observer or default_observer_pubkey()
    features = _parse_features(args.feature)

    if args.yql:
        print(searchlib.build_params(args.query, observer, args.ranking,
                                      features, args.hits)["yql"])
        return

    root = searchlib.raw_search(args.query, observer, args.ranking,
                                features, args.hits)
    total = root.get("fields", {}).get("totalCount")
    rows = searchlib.hits_table(root)

    print(f"query={args.query!r}  ranking={args.ranking}  "
          f"observer={observer[:12]}…  matched={total}  shown={len(rows)}")
    print("-" * 100)
    hdr = f"{'#':>2}  {'relevance':>11}  {'uscore':>6}  {'name / display_name':<32}  nip05"
    print(hdr)
    print("-" * 100)
    for r in rows:
        label = (r["display_name"] or r["name"] or "")[:32]
        us = r["user_score"]
        us_s = f"{us:.0f}" if isinstance(us, (int, float)) else "-"
        rel = r["relevance"]
        rel_s = f"{rel:.2f}" if isinstance(rel, (int, float)) else str(rel)
        print(f"{r['rank']:>2}  {rel_s:>11}  {us_s:>6}  {label:<32}  {r['nip05']}")


if __name__ == "__main__":
    main()
