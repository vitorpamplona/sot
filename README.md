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
config/         Tiny lib: env/.env resolution + defaults (one place for host/port config).
event-store/    Tiny lib: the one place that opens the shared Quartz event store
                (no SQLite FTS + relay identity), so no caller can get it wrong.
vespa/          Everything Vespa: the application package (app/ — schema + rank
                profiles, the ranking math) plus the Kotlin lib that talks to it —
                YQL search + document writes over plain objects (Profile / scores).
                Nostr-agnostic. Unit-tested.
indexer/        Nostr -> Quartz EventStore (NIP-77 negentropy sync + NIP-65 outbox
                discovery), plus the projection that maps store events into
                vespa's objects and writes them.
http/           Library: the GET /search JSON API route.
relay/          Library: the NIP-50 relay route; NIP-42 auth picks the observer.
server/         One Ktor app on one port composing http + relay + the web UI.
web/            Search UI (one index.html), served by the server (same origin).
cli/            sot: init / status / search / up / deploy.
```

## Quickstart

Needs **Docker + JDK 21**. The committed Gradle wrapper (`./gradlew`) fetches
Gradle on first use. Docker only runs Vespa — every module is plain JVM, so you
can point `VESPA_URL` at a remote Vespa and skip Docker.

```bash
./gradlew :cli:installDist                 # build the CLI
export PATH="$PWD/cli/build/install/sot/bin:$PATH"

sot up                                     # start Vespa + deploy vespa/app
sot index                                  # load profiles + NIP-85 scores
sot search "vitor"                         # search
sot search "vitor" --observer <pubkey>     # rank by one observer's trust
```

Search as the observer whose scores you loaded: set `DEFAULT_OBSERVER` (or pass
`--observer` — hex, npub, nprofile, or a NIP-05 `name@domain`) to a pubkey you've
ingested trust scores for, otherwise every trust score is 0. `sot status` shows
whether Vespa and the server are up (plus doc/event counts).

## The server

One process on one port (`SERVER_PORT`, default `:7777`) serves everything but Vespa:

```bash
./gradlew :server:run
#   http://localhost:7777/            web UI (browser) or NIP-11 (Accept: application/nostr+json)
#   http://localhost:7777/search      JSON API   (?text=vitor&observer=<pubkey>)
#   ws://localhost:7777/              NIP-50 relay (send a `search` REQ; NIP-42 auth picks the observer)
```

## Configuration

`sot init` writes a commented `.env`; the CLI **and** the server read it (a real
environment variable still overrides any value). Keys: `VESPA_URL`,
`VESPA_CONFIG_URL`, `SERVER_PORT`, `SERVER_URL`, `RELAY_URL`, `EVENTS_DB`,
`SEED_RELAYS` (comma-separated relays `sot index` crawls), `DEFAULT_OBSERVER`,
and the NIP-11 relay identity `SERVER_NAME` /
`SERVER_DESCRIPTION` / `SERVER_ICON` / `SERVER_PUBKEY` / `SERVER_OWNER`. Docker
only runs Vespa — point `VESPA_URL` at a remote Vespa to skip it.
