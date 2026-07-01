"""Experiment-friendly search wrapper.

This is the layer you iterate on when tuning *queries* and *ranking features*
without editing the vendored upstream `vespa.py`. It reuses upstream's exact
YQL builder (`_build_yql`) so the candidate set is identical to production, but
lets you swap the rank-profile and override any `ranking.features.query(...)`
input from the command line.

When the change you want is to the *equations themselves* (the rank-profile
math), edit `vespa/schemas/doc.sd` and redeploy with `tools/deploy.sh`.
When the change is to *query construction* and you intend to upstream it, edit
`brainstorm_server/app/core/vespa.py` directly.
"""
import os
import sys

import httpx

# Make the vendored upstream package importable: `app.core.vespa`.
_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(_ROOT, "brainstorm_server"))

from app.core import vespa as vespa  # noqa: E402  (upstream, vendored verbatim)

VESPA_URL = os.environ.get("VESPA_URL", "http://localhost:8080")


def build_params(
    query_text: str,
    observer: str,
    ranking: str = "name_and_quality_score_only",
    features: dict | None = None,
    hits: int = 50,
) -> dict:
    """Build Vespa /search params reusing upstream's YQL builder.

    `features` overrides/extends the `ranking.features.query(...)` map, e.g.
    {"w_gram": 8.0, "w_about": 0.3}. The observer tensor `user_q` is always set
    to the one-hot {observer:1.0} unless explicitly overridden.
    """
    words = query_text.split()[: vespa.MAX_QUERY_WORDS]
    joined = "".join(words) if len(words) >= 2 else None

    params: dict = {
        "yql": vespa._build_yql(words, joined),
        "ranking": ranking,
        "ranking.features.query(user_q)": "{" + observer + ":1.0}",
        "hits": hits,
    }
    for k, v in (features or {}).items():
        params[f"ranking.features.query({k})"] = v
    for i, w in enumerate(words):
        params[f"w{i}"] = w
    if joined:
        params["wj"] = joined
    return params


def raw_search(
    query_text: str,
    observer: str,
    ranking: str = "name_and_quality_score_only",
    features: dict | None = None,
    hits: int = 50,
    timeout: float = 30.0,
) -> dict:
    """Run a synchronous search and return the raw Vespa JSON `root`."""
    params = build_params(query_text, observer, ranking, features, hits)
    r = httpx.get(f"{VESPA_URL}/search/", params=params, timeout=timeout)
    r.raise_for_status()
    return r.json().get("root", {})


def hits_table(root: dict) -> list[dict]:
    """Flatten a Vespa `root` into a list of {rank, relevance, name, ...}."""
    rows = []
    for i, h in enumerate(root.get("children", []) or []):
        f = h.get("fields", {}) or {}
        mf = f.get("matchfeatures", {}) or {}
        rows.append(
            {
                "rank": i + 1,
                "relevance": h.get("relevance"),
                "name": f.get("name", ""),
                "display_name": f.get("display_name", ""),
                "pubkey": f.get("pubkey", ""),
                "nip05": f.get("nip05", ""),
                "user_score": mf.get("user_score"),
                "text_score": mf.get("text_score"),
                "quality_boost": mf.get("quality_boost"),
                "about": (f.get("about", "") or "")[:80],
            }
        )
    return rows
