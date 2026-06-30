# indexer — Kotlin/Quartz negentropy loader

Loads Nostr data into the local Vespa using **[Amethyst's Quartz](https://github.com/vitorpamplona/amethyst)**
library, syncing over **NIP-77 negentropy** and writing events **straight into
Vespa** (no intermediate event store). This replaces the Python REQ loader
(`../tools/load_nostr.py`).

Why negentropy: a plain Nostr `REQ` is capped by the relay (≈500 events on the
relays we use). Negentropy reconciles the relay's *entire* matching set, so the
profile index isn't limited to the first page.

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
| single-relay client | `BasicRelayClient(url, socketBuilder, listener)` |
| negentropy sync | `NegentropyManager.startSync(...)` + `INegentropyListener` (NIP-77) |
| kind:0 parsing | `MetadataEvent.contactMetaData()` → `UserMetadata` |
| kind:30382 parsing | `ContactCardEvent.aboutUser()` (subject) + `.rank()` (0–100) |

`VespaClient` writes the *exact* document-API payloads the upstream Python
`vespa.py` uses, so the index contents are identical regardless of which loader
filled them.

## Design notes / limitations

- **Bounded fetch.** Reconciled ids are downloaded through at most 8 concurrent
  REQ subscriptions, refilled on EOSE — relays cap subscriptions per connection,
  so firing thousands at once stalls after the first few.
- **Cap + memory.** `--max-events` also bounds how many ids are buffered during
  reconciliation, so syncing against a relay holding millions of events (the
  grapevine relay reports ~3.5M kind:0) doesn't blow the heap.
- **Negentropy fallback.** Some relays refuse to reconcile a very large set
  (`nip85-staging` returns "too many query results"). The stage falls back to a
  plain capped `REQ` and logs it. Paginating that fallback by `until` windows to
  pull more than one relay page is a natural next step.
