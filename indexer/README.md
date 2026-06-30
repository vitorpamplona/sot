# indexer — Kotlin/Quartz loader

Loads Nostr data into the local Vespa using **[Amethyst's Quartz](https://github.com/vitorpamplona/amethyst)**
library, writing events **straight into Vespa** (no intermediate event store).
This replaces the Python REQ loader (`../tools/load_nostr.py`).

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
indexer sync --max-events 25000                 # full
indexer sync --profiles false --max-providers 15 --fetch-timeout 25   # quick scores-only
```

Flags: `--db <path>` (SQLite, default `events.db`), `--seeds <urls…>`,
`--max-providers N`, `--fetch-timeout secs`, `--profiles true|false`.

> Stage B (negentropy-preferred sync + persisted `since` cursors for cheap
> re-runs) and Stage C (kind-10002 outbox relay discovery) build on this.

## Requirements

- JDK 21+
- Gradle 8.5+ on the PATH (no wrapper is committed — `services.gradle.org` isn't
  reachable from the build sandbox; run with your local `gradle`)
- A running local Vespa (`docker compose up -d vespa && docker compose up vespa-deploy`
  from the repo root)

## Run

```bash
cd indexer

# kind:0 profiles (negentropy full-set sync, capped at --max-events)
gradle run --args="profiles --max-events 25000"

# kind:30382 NIP-85 GrapeRank scores
gradle run --args="nip85 --max-events 25000"

# both
gradle run --args="all"
```

For repeated runs it's faster to build once and use the launcher script:

```bash
gradle installDist
./build/install/indexer/bin/indexer profiles --max-events 25000
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
| negentropy sync (opt-in) | `NegentropyManager.startSync(...)` + `INegentropyListener` (NIP-77) |
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
- **negentropy mode internals.** `NegentropyStage` enumerates the relay's full
  id set, then downloads it through at most 8 concurrent REQ subscriptions
  (refilled on EOSE — relays cap subscriptions per connection). `--max-events`
  also bounds id buffering so a huge set doesn't blow the heap. On a relay that
  refuses full reconciliation it falls back to one capped `REQ` and logs it;
  `pages` mode is the better choice there.
