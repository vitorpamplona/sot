# **S**earch **o**ver **T**rust (SoT)

Nostr search engine ranked by the user's **web of trust**.

This project pulls Nostr profiles and web-of-trust scores from the network over
[NIP-77 Negentropy](https://github.com/nostr-protocol/nips/blob/master/77.md)
and indexes them into a [Vespa](https://vespa.ai) search engine. Vespa's rank
profiles weight each result by the trust score that the searching user's
[NIP-85 Trusted Assertions](https://github.com/nostr-protocol/nips/blob/master/85.md)
provider has assigned to that profile.

We offer an HTTP API and a NIP-50 search relay for production, plus a CLI and a
simple web UI for development.

## Folders

```
vespa/          Vespa application package — schema + rank profiles (the ranking math).
query-engine/   Search core (Kotlin lib): YQL builder + Vespa client. Unit-tested.
indexer/        Nostr -> Quartz EventStore -> Vespa. NIP-77 negentropy sync;
                projects profiles + observer-keyed trust scores.
http/           HTTP service: GET /search -> query-engine.
relay/          NIP-50 search relay; NIP-42 auth picks the ranking observer.
cli/            sot: status / search / up / deploy.
web/            Search UI (one index.html) over the http service.
```

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

Search as the observer whose scores you loaded: set `DEFAULT_OBSERVER` (or pass
`--observer`) to a pubkey you've ingested trust scores for, otherwise every
trust score is 0. `sot status` shows whether Vespa / http / relay are up.

## Serving search

The same core is exposed three more ways:

```bash
./gradlew :http:run    # HTTP JSON on :8081  (GET /search?text=vitor&observer=<pubkey>)
./gradlew :relay:run   # NIP-50 relay on :7777 (send a `search` REQ)
cd web && python3 -m http.server 8090       # web UI -> the http service (CORS is on)
```

Env config: `VESPA_URL`, `EVENTS_DB`, `DEFAULT_OBSERVER`, `HTTP_PORT`,
`RELAY_PORT`, `RELAY_URL`.
