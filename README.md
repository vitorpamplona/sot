# sot

A Vespa-backed Nostr profile search ranked by the Nostr **web of trust**, built
as a multi-module Kotlin project. It grew out of a local replica of
[Brainstorm](https://brainstorm.world/)'s search and still compares against it
(so ranking improvements can be sent upstream), but it's now a real project: an
indexer, a shared search core, an HTTP API, a NIP-50 search relay, and a CLI.

## Modules (one Gradle build)

```
vespa/          Vespa application package — schema + rank profiles (the "equations").
                    doc.sd is language-agnostic and still upstreamable.
query-engine/       The search core (Kotlin lib): YQL builder + Vespa search client +
                    result model. Ported from the upstream Python; has unit tests.
indexer/            Nostr -> Quartz EventStore -> Vespa. Negentropy sync (NIP-77) with
                    since-cursors; projects profiles + observer-keyed WoT scores.
http-api/           Ktor service: GET /search/byText -> query-engine.
relay/       Quartz relay server answering NIP-50 `search` REQs: rank in Vespa,
                    return the original signed kind-0 events from the indexer's store.
                    NIP-42 optional auth picks the ranking observer.
cli/                install / status / search from the terminal (reuses query-engine).
gradle/libs.versions.toml   shared versions (Quartz@JitPack, Ktor, coroutines, sqlite).

brainstorm_server/  Python — kept as an upstream reference to diff against.
tools/              Python — legacy experiment harness (search.py, compare.py, …).
```

Build everything: `gradle build`. Run a module: `gradle :cli:installDist` then
`./cli/build/install/sot/bin/sot search "vitor"`.

Quartz does the heavy Nostr lifting across modules (events, NIP-19/42/50/77, the
relay server, the EventStore); Ktor serves HTTP/WebSocket.

<details><summary>Legacy layout notes (Python reference)</summary>

The Python `brainstorm_server/app/core/vespa.py` (YQL builder) and `vespa/`
were originally vendored verbatim from
[`NosFabrica/brainstorm_server`](https://github.com/NosFabrica/brainstorm_server)
and [`NosFabrica/brainstorm_one_click_deployment`](https://github.com/NosFabrica/brainstorm_one_click_deployment)
so changes diff cleanly upstream. `query-engine` is the Kotlin port of that
query logic; `doc.sd` remains the shared, upstreamable ranking artifact.

```
docker-compose.yml   local single-node Vespa + deploy sidecar
tools/               search.py · compare.py · searchlib.py · deploy.sh · load_nostr.py (legacy loader)
requirements.txt
```
</details>

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
  simpler multiplicative alternative. Both are in `vespa/schemas/doc.sd`.
- The query (`app/core/vespa.py`) fans one user query into per-word groups of
  exact / prefix / fuzzy / trigram clauses across `name`, `display_name`,
  `about`, then over-fetches and re-filters.

## Quickstart

Requires Docker, JDK 21 + Gradle 8.5+ (for the loader), and Python 3.11+ (for
the search/experiment tools).

```bash
# 1. Bring up Vespa and deploy the vendored app package (schema + ranking)
docker compose up -d vespa
docker compose up vespa-deploy        # waits for /ApplicationStatus then exits

# 2. Load data from the same sources production uses, via the Kotlin/Quartz
#    negentropy loader (full-set sync, not capped at one relay page):
#    - kind:0 profiles    from wss://wot.grapevine.network
#    - NIP-85 kind:30382 GrapeRank scores from wss://nip85-staging.nosfabrica.com
cd indexer && gradle run --args="all --max-events 25000" && cd ..

# 3. Install the search-tool deps and search (use the observer whose scores you loaded)
python3 -m venv .venv && . .venv/bin/activate
pip install -r requirements.txt
python tools/search.py "vitor"
python tools/search.py "vitor" --observer <grapevine_pubkey> --hits 20
```

> The Python `tools/load_nostr.py` still works as a simpler REQ-based loader, but
> it's capped at ~500 events per relay page; `indexer/` supersedes it.

`search.py` prints each hit's `relevance` and the match-features
(`user_score`, `text_score`, `quality_boost`) so you can see *why* it ranks
where it does.

## The experiment loop

There are two places to change the "search equations":

1. **Ranking math** — edit `vespa/schemas/doc.sd` (the rank-profile
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
grapevine/observer pubkey, `d` tag = subject, `rank` tag = 0..100. The `nip85`
loader stage pulls these from `wss://nip85-staging.nosfabrica.com` and writes one
tensor cell `quality_scores{author} = rank` per subject doc, so every observer
that published assertions gets its own ranking perspective. Ranks are already on
the 0..100 scale the `quality_boost` sigmoid expects — no rescaling.

> Note: search from the **observer whose scores you loaded** (`--observer
> <grapevine_pubkey>`). The server's hardcoded default observer only matters if
> that pubkey actually published assertions you've ingested.

## Upstreaming a change

Because `vespa/` and `app/core/vespa.py` match upstream byte-for-byte, a
plain `diff` against the upstream file is your patch. Apply it to a checkout of
the corresponding upstream repo and open the PR there. Do **not** upstream the
`tools/`, `docker-compose.yml`, or the `config.py`/`loggr.py` shims.
