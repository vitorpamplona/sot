# CLAUDE.md

Guidance for AI agents (and humans) working in this repo. Keep it current when the
architecture changes.

## What this is

**SoT — Search over Trust.** A Nostr profile search engine ranked by the searching
user's **web of trust**. It pulls Nostr profiles and NIP-85 trust scores from relays,
indexes them into [Vespa](https://vespa.ai), and ranks results by the trust score the
*observer* (the searching user's NIP-85 provider) assigned to each profile.

Plain JVM (Kotlin 2.4, JDK 21). Docker runs only Vespa; every module is ordinary JVM,
so you can point `VESPA_URL` at a remote Vespa and skip Docker.

## Module map & dependency direction

```
config        env/.env resolution + defaults. No deps. (com.vitorpamplona.sot.config)
event-store   The ONE place that opens the shared Quartz EventStore (com.vitorpamplona.sot.store):
                openEventStore() / openObservableStore() / relayIdentity(). Applies the
                no-SQLite-FTS strategy + relay identity so no caller can get either wrong.
              Depends on: :config, quartz, androidx-sqlite.
vespa         ALL Vespa access, Nostr-agnostic (com.vitorpamplona.sot.vespa):
                app/ — the Vespa application package (schema + rank profiles).
                read  — VespaSearch + ProfileQuery (YQL recall; ranking is in the schema)
                write — VespaClient + Profile (+ Profile.indexFields()). Writes are
                ASYNC via Vespa's official feed client (HTTP/2 multiplexed, per-doc
                ordering, retries built in) and return futures; reads are plain queries.
              Depends on: kotlinx-serialization, vespa-feed-client.
indexer       Nostr -> Quartz EventStore (com.vitorpamplona.sot.indexer):
                RelaySyncer (NIP-77 negentropy + paged fallback), Discovery (NIP-65
                outbox crawl), SyncPipeline.runSync(), SyncState (cursors), Sockets.
                PLUS VespaProjection — maps store events -> Profile/score and calls VespaClient.
                PLUS SyncService — owns NostrClient/VespaClient/projection; runOnce()/runForever().
                Consumes an event store; never creates one (the composition root does).
              Depends on: :vespa, quartz, coroutines, okhttp.
http          Library: the GET /search JSON route (Route.searchApi). -> :vespa
relay         Library: NIP-50 relay route + NIP-11 info + NIP-42 auth. -> :config, :vespa
cli           `sot` — the ONE executable and composition root. `sot serve` = ONE Ktor
              app on ONE port (SERVER_PORT): web UI + /search + NIP-50 relay + the
              background SyncService loop (SYNC_INTERVAL). `sot index` = one pass.
              -> :config, :event-store, :vespa, :indexer, :http, :relay
```

Key rule: **`:vespa` never imports a Nostr/Quartz type.** The indexer maps
Nostr events into `vespa`'s plain objects (`Profile`, score triples) and calls
`VespaClient`. Dependency flows `:indexer -> :vespa`, never the reverse.

## Data flow

- **Indexing** (`sot index` once, or `sot serve`'s background loop): relays
  --(RelaySyncer)--> EventStore (SQLite, source of truth) --(`store.changes` feed)-->
  `VespaProjection` --(Profile/score)--> `VespaClient` --HTTP--> Vespa. `SyncService`
  (in :indexer) owns that composition; `sot serve` shares ONE store between it and
  the relay, so everything inserted flows to Vespa through the same feed.
- **Querying**: `VespaSearch.search()` builds YQL via `ProfileQuery` + observer-weighted
  ranking params, calls Vespa `/search/`, maps children to `SearchHit`. Used by `http`,
  `relay`, and `cli`.
- **Observer-keyed trust (NIP-85)**: kind `10040` maps a service key -> observer; kind
  `30382` (signed by the service key) carries a rank; the score is stored as
  `quality_scores{OBSERVER}` on the subject's Vespa doc, keyed by the 10040 *author*,
  not the 30382 signer. Getting this wrong makes `--observer <user>` queries miss.

## Build, run, test

Use the committed wrapper `./gradlew` (Gradle 9.6.1; the wrapper fetches it from
`downloads.gradle.org` — the direct host, since the canonical `services.gradle.org`
URL redirects to GitHub releases, which some networks block).

```bash
./gradlew build                 # compile + test + spotlessCheck (the gate)
./gradlew test                  # unit tests: every module has some; the flow tests
                                # (:indexer, :vespa) run against MockVespa — the
                                # HTTP/1.1 + h2c mock in :vespa's testFixtures
./gradlew spotlessApply         # auto-format + add license headers (run before committing)
./gradlew spotlessCheck         # verify formatting (part of `check`/`build`)

./gradlew :cli:installDist      # build the CLI
export PATH="$PWD/cli/build/install/sot/bin:$PATH"
sot up                          # docker compose up Vespa + deploy vespa/app
sot serve                       # the one-port server on :7777 (web UI + /search + relay)
                                # + a background sync pass every SYNC_INTERVAL minutes
sot index                       # ONE sync pass (initial backfill / bounded experiments)
sot search "vitor" --observer <hex|npub|nprofile|nip05>
sot status                      # Vespa/server up? doc + event counts + last-sync age
sot verify [--repair]           # anti-entropy: diff the events db against the Vespa index; --repair fixes
```

`sot` subcommands: `init | serve | index | search | status | verify | up | down | destroy | deploy`
— each in its own file under `cli/src/main/kotlin/.../cli/`. `sot destroy` wipes the
events db, sync cursors, and Vespa's data volume for a from-scratch run. Don't run
`sot index` while `sot serve` is syncing the same db (two processes, one SQLite file).

## Configuration

All config resolves **env var -> `.env` -> built-in default** via `Config` (in `:config`).
`sot init` writes a commented `.env`. Keys: `VESPA_URL`, `VESPA_CONFIG_URL`, `SERVER_PORT`,
`SYNC_INTERVAL` (minutes between `sot serve` background passes; 0 = serve-only),
`SERVER_URL`, `RELAY_URL`, `EVENTS_DB`, `SEED_RELAYS`, `DEFAULT_OBSERVER`,
`QUARTZ_LOG_LEVEL` (Quartz's stderr diagnostics; ERROR by default — set WARN/DEBUG to
debug relay behavior), the NIP-11 identity `SERVER_NAME/DESCRIPTION/ICON/PUBKEY`, and `SERVER_NSEC` — the
relay's OWN key (NIP-11 `self` + NIP-42 auth to upstream relays; `sot init` generates it). A real env var always overrides `.env`.

## Conventions

- **Formatting is enforced.** Spotless + ktlint 1.7.1. Run `./gradlew spotlessApply`
  before committing; `build` fails on unformatted code. Config: `.editorconfig`
  (`max_line_length = 5000` — ktlint won't reflow long lines/signatures), root
  `build.gradle.kts` (the spotless block), header in `.spotless/copyright.kt`.
- **License header** (MIT, `.spotless/copyright.kt`) is required on every `.kt` file;
  `spotlessApply` adds it. Don't hand-write it.
- **Comments**: `/** … */` KDoc attaches to a declaration; a floating file-overview
  should be a plain `/* … */` block comment.
- **Dependencies** live in the version catalog `gradle/libs.versions.toml` (`libs.*`).
- **Quartz** (Amethyst's Nostr library) comes from JitPack, pinned by commit in the
  catalog (`quartz = "<commit>"`); `settings.gradle.kts` adds the jitpack repo. Bumping
  it may need a JitPack build (first request can 4xx while it builds — retry).
- Package root is `com.vitorpamplona.sot`; module = subpackage.

## Nostr / Vespa reference

- NIPs used: 05 (name@domain), 11 (relay info), 19 (npub/nprofile), 42 (auth picks the
  observer), 50 (search REQ), 62 (Request to Vanish -> delete from store + Vespa), 77
  (negentropy sync), 85 (Trusted Assertions: 10040 provider lists + 30382 scores).
- Vespa schema is in `vespa/app/` (schema + rank profiles = the ranking math),
  next to the Kotlin that depends on it — change them together. The
  `quality_scores` field is a sparse tensor keyed by observer pubkey.

## Gotchas

- Always open the event store through **`:event-store`** (`openEventStore` / `openObservableStore`). It's the single place that sets the two easy-to-get-wrong knobs: SQLite full-text index **off** (opening the same DB with a different strategy corrupts it — search is Vespa's job) and the store's **relay identity** from `RELAY_URL` (a null/foreign relay silently breaks NIP-62 vanish). Don't construct `EventStore` directly.
- Event **signature verification is on by default** before storing (relays are untrusted;
  a forged kind:0/30382/10040 would poison the trust graph). The non-verifying path is a
  test-only seam.
- `sot index` does a FULL sync by default (`--max-events 0` = unlimited; relays hold
  millions of events — expect hours + GBs). Pass `--max-events N` for a bounded slice.
- More detail per area: `README.md` (top-level), `indexer/README.md`, `web/README.md`.
