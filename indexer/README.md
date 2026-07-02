# indexer — Kotlin/Quartz loader

> Depends on Quartz from JitPack (`com.github.vitorpamplona.amethyst:quartz:2cb058b323`),
> which includes the generalized `negentropySyncOrFetch` accessory. `settings.gradle.kts`
> adds the `jitpack.io` repository.

Loads Nostr data using **[Amethyst's Quartz](https://github.com/vitorpamplona/amethyst)**
into a Quartz **EventStore** (the source of truth), then projects it into Vespa.
`VespaProjection` subscribes to the store's `changes` feed, maps each Nostr event
(kind:0 → profile, kind:30382 → trust score, …) into a plain object, and writes it
via **`:vespa`**'s `VespaClient` — the schema-aware writer that knows nothing
about Nostr. So this module owns the Nostr↔Vespa mapping; `:vespa` owns the
Vespa wire format.

A plain Nostr `REQ` is capped by the relay (≈500 events on the relays we use).
Two ways to get past that, selectable with `--mode`:

- **`pages` (default)** — Quartz's `INostrClient.fetchAllPages`, which walks
  `until` cursors page by page until the set is drained. Simple, robust, and
  works on **every** relay. This is the recommended path.
- **`negentropy`** — NIP-77 set reconciliation (`NegentropyManager`). Only
  fetches ids you're missing, so it's faster for *re-syncs*, but a relay will
  refuse to reconcile a set larger than its `negentropy.maxSyncEvents` (strfry
  default 1,000,000). `nip85-staging` returns `NEG-ERR blocked: too many query
  results` for the full kind-30382 set, so use `pages` there.

Both write the same documents into Vespa.

## `sync` mode — EventStore-backed projection (recommended)

`indexer sync` is the correct, NIP-85-faithful pipeline. A local Quartz
`ObservableEventStore` (SQLite) is the **source of truth**; Vespa is a
**projection** of its change feed:

```
sync events into the store ──► store.changes (Insert/Delete) ──► VespaProjection ──► Vespa
```

- **kind 0** → upsert profile.
- **kind 10040** (`TrustProviderListEvent`) → learn `serviceKey → observer` for the
  `30382:rank` provider.
- **kind 30382** (`ContactCardEvent`) → upsert `quality_scores{OBSERVER} = rank`,
  where OBSERVER is the 10040 author — **not** the 30382 signer (a per-observer
  *service key*), per NIP-85 Trusted Assertions. Keying by the signer is wrong
  and makes `--observer <user>` queries miss.
- **kind 5** (`DeletionEvent`) → erase the targeted profile/score from Vespa —
  by address (a-tag) or by raw event id (e-tag). Id targets resolve through the
  provenance ids Vespa stores (`event_id` on the doc, `score_event_ids{observer}`
  per score cell).
- **kind 62** (`RequestToVanishEvent`) → when it covers this store's relay
  identity, blank the author's profile (the store's vanish module erases their
  events; this mirrors it into Vespa).

Phases: (1) sync 0/10040/5 from seed relays → (2) resolve rank providers from
stored 10040s → (3) sync each provider's 30382 from its relay hint.

```bash
sot index                                          # full sync: kind-10002 relay discovery + SEED_RELAYS
sot index --discover false                         # seeds only, skip the outbox crawl
sot index --max-events 25000 --max-providers 15    # bound a quick experimental run
```

**Incremental by default.** Each (relay, kind) sync prefers **NIP-77 negentropy**
seeded with what the store already holds (so only the delta transfers); if the
relay refuses it (`maxSyncEvents`) or stalls on the id-fetch, it falls back to
**`fetchAllPages(since = lastSync − slack)`**. Per-relay capability and per-kind
cursors are persisted to `<db>.state.json`, so re-runs only pull new events (a
second run over the same data does ~0 work).

**Discovery (`--discover`, on by default).** A bounded NIP-65 outbox crawl: sync
kind-10002 from the seeds, add the relays they advertise to the pool, repeat for
`--max-rounds` or until `--max-relays`, persisting the pool so re-runs don't
rediscover. `--discover false` syncs only the seeds.

Flags: `--db <path>` (SQLite, default `events.db`), `--state <path>`,
`--seeds <urls…>` (defaults to `SEED_RELAYS` from `.env`/env),
`--discover true|false`, `--max-rounds N`, `--max-relays N`,
`--max-providers N`, `--fetch-timeout secs`, `--max-events N`,
`--concurrency N` (parallel relays, default 8), `--vespa <url>`.

> Relays sync in parallel (`--concurrency`, default 8) in every phase —
> discovery, phase 1, and per-provider scores. Kinds stay sequential within a
> relay (they share its cursors and negentropy-capability memory), and store
> writes are serialized behind a mutex — the fan-out is purely network-side.
> Progress: each (relay, kind) download logs a throttled `…` heartbeat every
> ~5s while active, and every finished relay logs a `[done/total]` line.

## Requirements

- JDK 21+ (use the committed Gradle wrapper: `./gradlew` from the repo root)
- A running local Vespa with the app deployed (`sot up`, or `docker compose up
  -d vespa && sot deploy` from the repo root)

## Run

This module is a **library** — it has no `main`. The CLI is the composition
root: `sot index` (see `cli/Index.kt`) wires up the store, Vespa client, and
projection from `.env`/flags and calls into `runSync`.

```bash
sot index                     # profiles + NIP-85 scores — one indivisible FULL sync
sot index --max-events 25000  # bounded slice for a quick local experiment
```

There is deliberately no profiles-only or scores-only mode: profiles without
trust scores (or vice versa) leave the index unusable, so every run syncs both.
Flags are listed above (see the `sync` section).

## How it maps to Quartz

| concern | Quartz API used |
| --- | --- |
| relay transport | `BasicOkHttpWebSocket.Builder` (OkHttp), direct egress |
| client | `NostrClient(socketBuilder, scope)` |
| paged fetch (default) | `INostrClient.fetchAllPages(relay, filters, onEvent)` |
| negentropy sync | `INostrClient.negentropySyncOrFetch(...)` (NIP-77 + paged fallback, windows `max_sync_events`) |
| store | `ObservableEventStore(EventStore(...))` — persists events, emits a `changes` feed |
| kind:0 parsing | `MetadataEvent.contactMetaData()` → `UserMetadata` → `Profile` |
| kind:30382 parsing | `ContactCardEvent.aboutUser()` (subject) + `.rank()` (0–100) |

`VespaProjection` does the parsing above and hands ready-made objects (`Profile`,
score triples) to `:vespa`'s `VespaClient`, which owns the Vespa document-API
wire format and knows nothing about Nostr.

## Design notes

- **Cap.** `--max-events` bounds ingest per kind (0 = full sync, the default). In `pages` mode it's the
  `Filter.limit`; relays hold far more than you want locally (the grapevine
  relay reports ~3.5M kind:0).
- **Write failures are logged, not swallowed.** A down or feed-blocked Vespa
  shows up as `writeFailures=N` in the final line plus the first few errors —
  not a silent "0 upserted".
- **negentropy syncer.** `RelaySyncer` calls Quartz's `negentropySyncOrFetch`
  (which reconciles, windows around `max_sync_events`, and pages back on a
  NEG-ERR) with `filter.since` cursors keyed per (relay, kind, author) for
  incremental re-runs. A thin safety net re-pages when a relay reconciles to
  nothing on a first sync (some aggregators, e.g. purplepag.es on kind:10040)
  and marks it pages-only thereafter. The old hand-rolled `NegentropyStage` is
  gone — that logic now lives in the Quartz library.

  Legacy `--mode negentropy` internals (superseded): it enumerated the relay's full
  id set, then downloads it through at most 8 concurrent REQ subscriptions
  (refilled on EOSE — relays cap subscriptions per connection). `--max-events`
  also bounds id buffering so a huge set doesn't blow the heap. On a relay that
  refuses full reconciliation it falls back to one capped `REQ` and logs it;
  `pages` mode is the better choice there.
