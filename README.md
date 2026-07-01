# sot

**S**earch **o**ver **T**rust — Nostr profile search ranked by the Nostr **web
of trust**, backed by [Vespa](https://vespa.ai). Trust comes from
[NIP-85 Trusted Assertions](https://github.com/nostr-protocol/nips/blob/master/85.md):
each observer's ranking of a profile is a score you can search by.

## Modules (one Gradle build)

```
vespa/          Vespa app package — schema + rank profiles (the ranking math).
query-engine/   Search core (Kotlin lib): YQL builder + Vespa client. Unit-tested.
indexer/        Nostr -> Quartz EventStore -> Vespa. NIP-77 negentropy sync;
                projects profiles + observer-keyed trust scores.
http/           Ktor service: GET /search -> query-engine.
relay/          NIP-50 search relay; NIP-42 auth picks the ranking observer.
cli/            sot: status / search / compare / up / deploy.
web/            Zero-build search UI (one index.html) over the http service.
```

Quartz handles the Nostr side (events, NIP-19/42/50/77, the relay server, the
EventStore); Ktor serves HTTP/WebSocket.

## Quickstart

Needs **Docker + JDK 21**. The committed Gradle wrapper (`./gradlew`) fetches
Gradle on first use. Docker only runs Vespa — every module is plain JVM, so you
can point `VESPA_URL` at a remote Vespa and skip Docker.

```bash
./gradlew :cli:installDist                 # build the CLI
export PATH="$PWD/cli/build/install/sot/bin:$PATH"

sot up                                     # start Vespa + deploy vespa/
./gradlew :indexer:run --args="all"        # load profiles + NIP-85 scores
sot search "vitor"                         # search
sot search "vitor" --observer <pubkey>     # rank by one observer's trust
```

Search from the observer whose scores you loaded: set `DEFAULT_OBSERVER` (or pass
`--observer`) to a pubkey that has published assertions, else every score is 0.
`sot status` shows whether Vespa / http / relay are up.

## Serving search

The same core is exposed three more ways:

```bash
./gradlew :http:run    # HTTP JSON on :8081  (GET /search?text=vitor&observer=<pubkey>)
./gradlew :relay:run   # NIP-50 relay on :7777 (send a `search` REQ)
cd web && python3 -m http.server 8090       # web UI -> the http service (CORS is on)
```

Env config: `HTTP_API_PORT`, `RELAY_PORT`, `RELAY_URL`, `INDEXER_DB`, `DEFAULT_OBSERVER`.

## Tuning the ranking

Two knobs:

1. **Ranking math** — `vespa/schemas/doc.sd` (rank profiles). Redeploy, then A/B:
   ```bash
   sot deploy
   sot compare "vitor" "alex gleason" jack   # top-N per variant, side by side
   ```
2. **Query building** — `query-engine`'s `Yql.kt` and `VespaSearch.kt`.

`sot compare` ships a starter panel of variants; override with
`--variants variants.json` (`{"name": {"ranking": "...", "features": {"w_gram": 15.0}}}`).

## How ranking works

Each profile (kind:0) is a Vespa `doc` with trigram fields for typo/substring
matching. Per-observer trust is a sparse tensor `quality_scores{observer}`: a
query passes `query(user_q) = {observer:1.0}`, so `user_score = sum(user_q *
quality_scores)` is *this observer's* trust in *this* profile. Scores come from
NIP-85 Trusted Assertions (kind `30382`, `rank` 0..100), keyed by the
**observer** (the kind-10040 author) — so every observer gets its own ranking.
