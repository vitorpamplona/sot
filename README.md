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
web/            Search UI (one index.html), served by `sot serve` (same origin).
cli/            sot — the one executable: serve / init / index / search / status /
                up / down / destroy / deploy. `sot serve` composes http + relay +
                web UI + a background sync loop in a single process.
```

## Quickstart

Needs **Docker + JDK 21**. The committed Gradle wrapper (`./gradlew`) fetches
Gradle on first use. Docker only runs Vespa — every module is plain JVM, so you
can point `VESPA_URL` at a remote Vespa and skip Docker.

```bash
./gradlew :cli:installDist                 # build the CLI
export PATH="$PWD/cli/build/install/sot/bin:$PATH"

sot init                                   # write a commented .env (seed relays, observer, ports)
sot up                                     # start Vespa + deploy vespa/app
sot serve                                  # web UI + /search + NIP-50 relay + background sync loop
sot search "vitor"                         # search
sot search "vitor" --observer <pubkey>     # rank by one observer's trust
```

`sot index` runs a single full sync pass and exits — useful for the initial
backfill or bounded experiments (`--max-events N`); `sot serve` runs the same
pass every `SYNC_INTERVAL` minutes in the background, so a running server keeps
itself fresh. Don't run both against the same db at once.

Search as the observer whose scores you loaded: set `DEFAULT_OBSERVER` (or pass
`--observer` — hex, npub, nprofile, or a NIP-05 `name@domain`) to a pubkey you've
ingested trust scores for, otherwise every trust score is 0. `sot status` shows
whether Vespa and the server are up (plus doc/event counts).

## The server

`sot serve` is one process on one port (`SERVER_PORT`, default `:7777`) serving
everything but Vespa, plus the background sync:

```bash
sot serve        # refuses to start if Vespa is down; `sot serve --up` starts local Vespa first
#   http://localhost:7777/            web UI (browser) or NIP-11 (Accept: application/nostr+json)
#   http://localhost:7777/search      JSON API   (?text=vitor&observer=<pubkey>)
#   ws://localhost:7777/              NIP-50 relay (send a `search` REQ; NIP-42 auth picks the observer)
# background: an incremental sync pass every SYNC_INTERVAL minutes (default 15; 0 = serve-only)
```

## Configuration

`sot init` writes a commented `.env` (a real environment variable still overrides
any value). Keys: `VESPA_URL`,
`VESPA_CONFIG_URL`, `SERVER_PORT`, `SYNC_INTERVAL`, `SERVER_URL`, `RELAY_URL`, `EVENTS_DB`,
`SEED_RELAYS` (comma-separated relays the sync crawls), `DEFAULT_OBSERVER`,
and the NIP-11 relay identity `SERVER_NAME` /
`SERVER_DESCRIPTION` / `SERVER_ICON` / `SERVER_PUBKEY` / `SERVER_OWNER`. Docker
only runs Vespa — point `VESPA_URL` at a remote Vespa to skip it.
