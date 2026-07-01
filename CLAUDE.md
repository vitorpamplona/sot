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
vespa-engine  ALL Vespa access, Nostr-agnostic (com.vitorpamplona.sot.vespa):
                read  — VespaSearch + ProfileQuery (YQL recall; ranking is in the schema)
                write — VespaClient + Profile (+ Profile.indexFields())
              Depends on: kotlinx-serialization only.
indexer       Nostr -> Quartz EventStore (com.vitorpamplona.sot.indexer):
                RelaySyncer (NIP-77 negentropy + paged fallback), Discovery (NIP-65
                outbox crawl), SyncPipeline.runSync(), SyncState (cursors), Sockets.
                PLUS VespaProjection — maps store events -> Profile/score and calls VespaClient.
                Consumes an event store; never creates one (the composition root does).
              Depends on: :vespa-engine, quartz, coroutines, okhttp.
http          Library: the GET /search JSON route (Route.searchApi). -> :vespa-engine
relay         Library: NIP-50 relay route + NIP-11 info + NIP-42 auth. -> :config, :vespa-engine
server        Composition root: ONE Ktor app on ONE port (SERVER_PORT) = web UI +
              /search + NIP-50 relay. -> :config, :event-store, :vespa-engine, :http, :relay
cli           `sot` command. Composition root for `sot index`.
              -> :config, :event-store, :vespa-engine, :indexer
```

Key rule: **`:vespa-engine` never imports a Nostr/Quartz type.** The indexer maps
Nostr events into `vespa-engine`'s plain objects (`Profile`, score triples) and calls
`VespaClient`. Dependency flows `:indexer -> :vespa-engine`, never the reverse.

## Data flow

- **Indexing** (`sot index`): relays --(RelaySyncer)--> EventStore (SQLite, source of
  truth) --(`store.changes` feed)--> `VespaProjection` --(Profile/score)--> `VespaClient`
  --HTTP--> Vespa. The CLI (`cli/Index.kt`) is the composition root: it launches the
  projection, runs `runSync`, drains, exits.
- **Querying**: `VespaSearch.search()` builds YQL via `ProfileQuery` + observer-weighted
  ranking params, calls Vespa `/search/`, maps children to `SearchHit`. Used by `http`,
  `relay`, and `cli`.
- **Observer-keyed trust (NIP-85)**: kind `10040` maps a service key -> observer; kind
  `30382` (signed by the service key) carries a rank; the score is stored as
  `quality_scores{OBSERVER}` on the subject's Vespa doc, keyed by the 10040 *author*,
  not the 30382 signer. Getting this wrong makes `--observer <user>` queries miss.

## Build, run, test

Use the committed wrapper `./gradlew` (Gradle 8.14.3).

```bash
./gradlew build                 # compile + test + spotlessCheck (the gate)
./gradlew test                  # unit tests (ProfileQueryTest lives in :vespa-engine)
./gradlew spotlessApply         # auto-format + add license headers (run before committing)
./gradlew spotlessCheck         # verify formatting (part of `check`/`build`)

./gradlew :cli:installDist      # build the CLI
export PATH="$PWD/cli/build/install/sot/bin:$PATH"
sot up                          # docker compose up Vespa + deploy vespa-engine/app
sot index all                   # load profiles + NIP-85 scores (stages: all|profiles|nip85)
sot search "vitor" --observer <hex|npub|nprofile|nip05>
sot status                      # Vespa/server up? doc + event counts

./gradlew :server:run           # the one-port server on :7777 (web UI + /search + relay)
```

`sot` subcommands: `init | index | search | status | up | down | deploy` — each in its
own file under `cli/src/main/kotlin/.../cli/`.

## Configuration

All config resolves **env var -> `.env` -> built-in default** via `Config` (in `:config`).
`sot init` writes a commented `.env`. Keys: `VESPA_URL`, `VESPA_CONFIG_URL`, `SERVER_PORT`,
`SERVER_URL`, `RELAY_URL`, `EVENTS_DB`, `SEED_RELAYS`, `DEFAULT_OBSERVER`, and the NIP-11
identity `SERVER_NAME/DESCRIPTION/ICON/PUBKEY/OWNER`. A real env var always overrides `.env`.

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
- Vespa schema is in `vespa-engine/app/` (schema + rank profiles = the ranking math),
  next to the Kotlin that depends on it — change them together. The
  `quality_scores` field is a sparse tensor keyed by observer pubkey.

## Gotchas

- Always open the event store through **`:event-store`** (`openEventStore` / `openObservableStore`). It's the single place that sets the two easy-to-get-wrong knobs: SQLite full-text index **off** (opening the same DB with a different strategy corrupts it — search is Vespa's job) and the store's **relay identity** from `RELAY_URL` (a null/foreign relay silently breaks NIP-62 vanish). Don't construct `EventStore` directly.
- Event **signature verification is on by default** before storing (relays are untrusted;
  a forged kind:0/30382/10040 would poison the trust graph). The non-verifying path is a
  test-only seam.
- `sot index` runs a bounded slice by default (`--max-events 25000`); relays hold millions.
- More detail per area: `README.md` (top-level), `indexer/README.md`, `web/README.md`.
