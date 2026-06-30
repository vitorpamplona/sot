# vespa-search

A local replica of [Brainstorm](https://brainstorm.world/)'s Vespa-based Nostr
profile search, built so you can **experiment with the ranking equations** and
send the good ones back upstream.

It mirrors two upstream repos:

- **Index + ranking** — [`NosFabrica/brainstorm_one_click_deployment`](https://github.com/NosFabrica/brainstorm_one_click_deployment) → vendored under [`vespa-app/`](vespa-app)
- **Query construction** — [`NosFabrica/brainstorm_server`](https://github.com/NosFabrica/brainstorm_server) → vendored under [`brainstorm_server/app/core/vespa.py`](brainstorm_server/app/core/vespa.py)

Those files are copied **verbatim** so that any change you make diffs cleanly
into an upstream pull request. All the local-only scaffolding (Docker compose,
data loader, experiment CLIs) lives outside those paths and *reuses* the
upstream code instead of forking it.

## Layout

```
vespa-app/                      # VERBATIM upstream Vespa app package (the index + equations)
  schemas/doc.sd                #   ← rank-profiles live here: name_only,
  services.xml                  #     name_and_quality_score_only, search_rank
  hosts.xml
brainstorm_server/              # VERBATIM upstream query logic + minimal local shims
  app/core/vespa.py             #   ← YQL builder + ranking features (upstreamable)
  app/utils/observer.py
  app/core/{config,loggr}.py    #   ← LOCAL shims (not upstream) so the above runs standalone
docker-compose.yml              # local single-node Vespa + deploy sidecar
tools/                          # LOCAL experiment harness (not upstream)
  load_nostr.py                 #   pull kind:0 profiles + kind:3 follow graph -> Vespa
  search.py                     #   run one query, show ranked hits + match-features
  compare.py                    #   A/B ranking variants over a query set
  searchlib.py                  #   thin wrapper reusing vespa._build_yql
  deploy.sh                     #   redeploy vespa-app after editing doc.sd
requirements.txt
```

## How the production search works (what we replicated)

- Each Nostr profile (kind:0) is one Vespa `doc`: `name`, `display_name`,
  `about`, `pubkey`, plus trigram (`*_gram`) fields for substring/typo matching.
- Per-observer trust lives in a **sparse tensor** `quality_scores{observer}` on
  each doc. A query passes `query(user_q) = {observer:1.0}` and the rank-profile
  computes `user_score = sum(user_q * quality_scores)` — i.e. *this observer's*
  trust in *this* profile.
- `name_and_quality_score_only` (the production profile) scores text relevance
  first (exact-token tier `2000+` vs. gram tier `300`), then in second-phase
  adds a logistic `quality_boost` gated by text quality. `search_rank` is a
  simpler multiplicative alternative. Both are in `vespa-app/schemas/doc.sd`.
- The query (`app/core/vespa.py`) fans one user query into per-word groups of
  exact / prefix / fuzzy / trigram clauses across `name`, `display_name`,
  `about`, then over-fetches and re-filters.

## Quickstart

Requires Docker + Python 3.11+.

```bash
# 1. Bring up Vespa and deploy the vendored app package (schema + ranking)
docker compose up -d vespa
docker compose up vespa-deploy        # waits for /ApplicationStatus then exits

# 2. Install the harness deps
python3 -m venv .venv && . .venv/bin/activate
pip install -r requirements.txt

# 3. Load data from the same sources production uses:
#    - kind:0 profiles from wss://wot.grapevine.network
#    - real GrapeRank scores (NIP-85 kind:30382) from wss://nip85-staging.nosfabrica.com
python tools/load_nostr.py all --limit 5000

# 4. Search (use the observer whose scores you loaded)
python tools/search.py "vitor"
python tools/search.py "vitor" --observer <grapevine_pubkey> --hits 20
```

`search.py` prints each hit's `relevance` and the match-features
(`user_score`, `text_score`, `quality_boost`) so you can see *why* it ranks
where it does.

## The experiment loop

There are two places to change the "search equations":

1. **Ranking math** — edit `vespa-app/schemas/doc.sd` (the rank-profile
   expressions), then redeploy and re-test:

   ```bash
   tools/deploy.sh                       # ~seconds, no container restart
   python tools/compare.py --queries tools/queries.sample.txt
   ```

2. **Query construction** — edit `brainstorm_server/app/core/vespa.py`
   (the `_build_yql` family). This is the file you'd actually PR upstream.

For weight-only experiments you don't need to touch either file:
`compare.py`/`search.py` can override any `ranking.features.query(...)` input
from the command line (`--feature w_gram=15`) and switch rank-profiles
(`--ranking search_rank`). `tools/compare.py` ships a starter panel of variants.

### Quality scores

`quality_scores{observer}` is production's GrapeRank output, published by
Brainstorm as NIP-85 *trusted assertions* (kind `30382`): author = the
grapevine/observer pubkey, `d` tag = subject, `rank` tag = 0..100.
`tools/load_nostr.py nip85` pulls these from `wss://nip85-staging.nosfabrica.com`
and writes one tensor cell `quality_scores{author} = rank` per subject doc, so
every observer that published assertions gets its own ranking perspective.
Ranks are already on the 0..100 scale the `quality_boost` sigmoid expects — no
rescaling — and they feed through the upstream `vespa.upsert_score`.

> Note: search from the **observer whose scores you loaded** (`--observer
> <grapevine_pubkey>`). The server's hardcoded default observer only matters if
> that pubkey actually published assertions you've ingested.

## Upstreaming a change

Because `vespa-app/` and `app/core/vespa.py` match upstream byte-for-byte, a
plain `diff` against the upstream file is your patch. Apply it to a checkout of
the corresponding upstream repo and open the PR there. Do **not** upstream the
`tools/`, `docker-compose.yml`, or the `config.py`/`loggr.py` shims.
