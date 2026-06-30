# Vendored upstream query logic

These files are copied **verbatim** from
[`NosFabrica/brainstorm_server`](https://github.com/NosFabrica/brainstorm_server)
(branch `claude/vespa-search-scoring-zf2suy`) so that improvements to the search
query / ranking can be diffed and sent back upstream cleanly.

| File | Origin | Editable for experiments? |
| --- | --- | --- |
| `app/core/vespa.py` | upstream `app/core/vespa.py` | **yes** — YQL builder + ranking features. Changes here are upstreamable. |
| `app/utils/observer.py` | upstream `app/utils/observer.py` | rarely |
| `app/core/config.py` | **local shim** (not upstream) | n/a — local stand-in |
| `app/core/loggr.py` | **local shim** (not upstream) | n/a — local stand-in |

The two shims exist only so the upstream files import and run outside the full
FastAPI server. Do not include them in an upstream PR.

To experiment without touching `vespa.py`, prefer the helpers in `../tools/`
(e.g. `searchlib.raw_search`), which reuse `vespa._build_yql` and let you vary
the rank-profile and feature weights from the command line. Only edit
`vespa.py` directly when the change itself is what you want to upstream.
