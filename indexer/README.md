# indexer — Kotlin/Quartz loader

> Depends on Quartz from JitPack (`com.github.vitorpamplona.amethyst:quartz:2cb058b323`),
> which includes the generalized `negentropySyncOrFetch` accessory. `settings.gradle.kts`
> adds the `jitpack.io` repository.

Loads Nostr data using **[Amethyst's Quartz](https://github.com/vitorpamplona/amethyst)**
into a Quartz **EventStore** (the source of truth), then projects profiles and
observer-keyed web-of-trust scores into Vespa.

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
  *service key*). This is what Brainstorm itself does
  (`batch_upsert_scores(observer=observer)`); keying by the signer is wrong and
  makes `--observer <user>` queries miss.
- **kind 5** (`DeletionEvent`) → erase the profile/score from Vespa.

Phases: (1) sync 0/10040/5 from seed relays → (2) resolve rank providers from
stored 10040s → (3) sync each provider's 30382 from its relay hint.

```bash
indexer sync --max-events 25000                                # full, from seed relays
indexer sync --discover true --max-relays 200                  # discover relays via kind-10002 first
indexer sync --profiles false --max-providers 15 --fetch-timeout 25   # quick scores-only
```

**Incremental by default.** Each (relay, kind) sync prefers **NIP-77 negentropy**
seeded with what the store already holds (so only the delta transfers); if the
relay refuses it (`maxSyncEvents`) or stalls on the id-fetch, it falls back to
**`fetchAllPages(since = lastSync − slack)`**. Per-relay capability and per-kind
cursors are persisted to `<db>.state.json`, so re-runs only pull new events (a
second run over the same data does ~0 work).

**Discovery (`--discover`).** A bounded NIP-65 outbox crawl: sync kind-10002 from
the seeds, add the relays they advertise to the pool, repeat for `--max-rounds`
or until `--max-relays`, persisting the pool so re-runs don't rediscover.

Flags: `--db <path>` (SQLite, default `events.db`), `--state <path>`,
`--seeds <urls…>`, `--discover true|false`, `--max-rounds N`, `--max-relays N`,
`--max-providers N`, `--fetch-timeout secs`, `--profiles true|false`,
`--max-events N`.

> Note: per-relay syncs run sequentially today; over a large discovered pool the
> first full run is slow (each relay does a negentropy attempt + fallback).
> Parallelizing the network fetch (keeping store writes serial) is the obvious
> next speedup. The robust per-relay negentropy→pages logic is exactly what the
> Quartz feature request in `../docs/quartz-negentropy-sync-prompt.md` would fold
> into the library.

## Requirements

- JDK 21+ (use the committed Gradle wrapper: `./gradlew` from the repo root)
- A running local Vespa with the app deployed (`sot up`, or `docker compose up
  -d vespa && sot deploy` from the repo root)

## Run

```bash
# from the repo root, via the committed wrapper

# kind:0 profiles (negentropy full-set sync, capped at --max-events)
./gradlew :indexer:run --args="profiles --max-events 25000"

# kind:30382 NIP-85 GrapeRank scores
./gradlew :indexer:run --args="nip85 --max-events 25000"

# both
./gradlew :indexer:run --args="all"
```

For repeated runs it's faster to build once and use the launcher script:

```bash
./gradlew :indexer:installDist
./indexer/build/install/indexer/bin/indexer profiles --max-events 25000
```

### Flags

| flag | default | meaning |
| --- | --- | --- |
| `--mode <pages\|negentropy>` | `pages` | fetch strategy (see above) |
| `--vespa <url>` | `http://localhost:8080` (or `$VESPA_URL`) | Vespa document API base |
| `--relays <urls...>` | `wss://wot.grapevine.network` | relays for kind:0 profiles |
| `--score-relays <urls...>` | `wss://nip85-staging.nosfabrica.com` | relays for kind:30382 assertions |
| `--max-events <n>` | `25000` | per-stage ingest cap (0 = unlimited) |
| `--min-rank <n>` | `1` | skip NIP-85 assertions below this rank |
| `--batch <n>` | `500` | ids per fetch REQ |
| `--limit-secs <n>` | `240` | overall per-stage timeout |

## How it maps to Quartz

| concern | Quartz API used |
| --- | --- |
| relay transport | `BasicOkHttpWebSocket.Builder` (OkHttp), direct egress |
| client | `NostrClient(socketBuilder, scope)` |
| paged fetch (default) | `INostrClient.fetchAllPages(relay, filters, onEvent)` |
| negentropy sync | `INostrClient.negentropySyncOrFetch(...)` (NIP-77 + paged fallback, windows `max_sync_events`) |
| kind:0 parsing | `MetadataEvent.contactMetaData()` → `UserMetadata` |
| kind:30382 parsing | `ContactCardEvent.aboutUser()` (subject) + `.rank()` (0–100) |

`VespaClient` writes the *exact* document-API payloads the upstream Python
`vespa.py` uses, so the index contents are identical regardless of which loader
filled them.

## Design notes

- **Cap.** `--max-events` bounds ingest per stage. In `pages` mode it's the
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
