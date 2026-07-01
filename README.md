# sot

**S**earch **o**ver **T**rust — a Vespa-backed Nostr profile search ranked by
the Nostr **web of trust**, built as a multi-module Kotlin project. It grew out
of a local replica of [Brainstorm](https://brainstorm.world/)'s search and now
stands on its own: an indexer, a shared search core, an HTTP API, a NIP-50
search relay, and a CLI.

## Modules (one Gradle build)

```
vespa/          Vespa application package — schema + rank profiles (the "equations").
                doc.sd defines the fields and the ranking math.
query-engine/   The search core (Kotlin lib): YQL builder + Vespa search client +
                result model. Has unit tests.
indexer/        Nostr -> Quartz EventStore -> Vespa. Negentropy sync (NIP-77) with
                since-cursors; projects profiles + observer-keyed WoT scores.
http-api/       Ktor service: GET /search -> query-engine.
relay/          Quartz relay server answering NIP-50 `search` REQs: rank in Vespa,
                return the original signed kind-0 events from the indexer's store.
                NIP-42 optional auth picks the ranking observer.
cli/            sot: install / status / search / compare from the terminal
                (reuses query-engine).
gradle/libs.versions.toml   shared versions (Quartz@JitPack, Ktor, coroutines, sqlite).
```

Build everything: `gradle build`. Install the CLI: `gradle :cli:installDist`,
then `./cli/build/install/sot/bin/sot search "vitor"`.

Quartz does the heavy Nostr lifting across modules (events, NIP-19/42/50/77, the
relay server, the EventStore); Ktor serves HTTP/WebSocket.

## How the search works

- Each Nostr profile (kind:0) is one Vespa `doc`: `name`, `display_name`,
  `about`, `pubkey`, plus trigram (`*_gram`) fields for substring/typo matching.
- Per-observer trust lives in a **sparse tensor** `quality_scores{observer}` on
  each doc. A query passes `query(user_q) = {observer:1.0}` and the rank-profile
  computes `user_score = sum(user_q * quality_scores)` — i.e. *this observer's*
  trust in *this* profile.
- `name_and_quality_score_only` (the default profile) scores text relevance
  first (exact-token tier `2000+` vs. gram tier `300`), then in second-phase
  adds a logistic `quality_boost` gated by text quality. `search_rank` is a
  simpler multiplicative alternative. Both are in `vespa/schemas/doc.sd`.
- The query builder (`query-engine`, `Yql.kt`) fans one user query into per-word
  groups of exact / prefix / fuzzy / trigram clauses across `name`,
  `display_name`, `about`, then over-fetches and re-filters.

## Quickstart

Requires Docker, JDK 21, and a global Gradle 8.5+ install (no wrapper is
committed). The `sot` CLI wraps the Docker steps; install it once with
`gradle :cli:installDist` and put `cli/build/install/sot/bin` on your `PATH`.

```bash
# 1. Bring up Vespa and deploy the app package (schema + ranking)
sot up                                # docker compose up vespa + deploy vespa/
#   equivalently: docker compose up -d vespa && docker compose up vespa-deploy

# 2. Load data via the Kotlin/Quartz negentropy indexer (full-set sync, not
#    capped at one relay page):
#    - kind:0 profiles    from the discovered outbox relays
#    - NIP-85 kind:30382 GrapeRank scores from wss://nip85-staging.nosfabrica.com
gradle :indexer:run --args="all --max-events 25000"

# 3. Search from the terminal (use the observer whose scores you loaded)
sot search "vitor"
sot search "vitor" --observer <pubkey> --hits 20
```

`sot search` prints each hit's `relevance` and `score` (the observer's
`user_score`) so you can see *why* it ranks where it does. `sot status` shows
whether Vespa / http-api / relay are up.

### Serving search over HTTP and Nostr

The same `query-engine` core is exposed two more ways:

```bash
# HTTP JSON API on :8081  (GET /search?text=vitor&observer=<pubkey>)
gradle :http-api:run

# NIP-50 search relay on :7777  (send a `search` REQ; NIP-42 auth picks the
# observer, otherwise the default observer is used)
gradle :relay:run
```

Ports and the default observer are configurable via env: `HTTP_API_PORT`,
`RELAY_PORT`, `RELAY_URL`, `INDEXER_DB`, `DEFAULT_OBSERVER`.

## The experiment loop

Two places to change the "search equations":

1. **Ranking math** — edit `vespa/schemas/doc.sd` (the rank-profile
   expressions), then redeploy and A/B the variants:

   ```bash
   sot deploy                                   # ~seconds, no container restart
   sot compare "vitor" "alex gleason" jack      # top-N side by side per variant
   ```

2. **Query construction** — edit `query-engine`'s `Yql.kt` (the YQL builder) and
   `VespaSearch.kt` (feature weights, over-fetch).

`sot compare` ships a starter panel (`prod`, `more_gram`, `less_about`,
`search_rank`); override it with `--variants variants.json` of the shape
`{"name": {"ranking": "...", "features": {"w_gram": 15.0}}}`, and feed a query
set with `--queries queries.txt` (one per line, `#` comments ignored).

### Quality scores

`quality_scores{observer}` is a GrapeRank-style web-of-trust output, published
as NIP-85 *trusted assertions* (kind `30382`): the `d` tag is the subject, the
`rank` tag is 0..100. The indexer resolves each assertion's **observer** through
the observer's kind-10040 trust-provider list (which maps `30382:rank` to the
signing service key) and writes one tensor cell `quality_scores{observer} =
rank` per subject doc — so every observer gets its own ranking perspective.
Ranks are already on the 0..100 scale the `quality_boost` sigmoid expects — no
rescaling.

> Note: search from the **observer whose scores you loaded** (`--observer
> <pubkey>`). The default observer only matters if that pubkey actually
> published assertions you've ingested.
