# CLAUDE.md

Guidance for AI agents (and humans) working in this repo. Keep it current when the
architecture changes.

## What this is

**SoT — Search over Trust.** A search engine for [Nostr](https://nostr.com) that
ranks results by *who the searcher trusts*. Events sync in from the network and
are stored in [Vespa](https://vespa.ai); a NIP-50 search ranks each hit by the
trust score the searching user (the **observer**) has for its author.

Two design choices shape everything:

- **Vespa is the event store.** There's no SQLite and no separate index to keep
  in sync — one copy of the data. `NostrEventStore` implements Quartz's
  `IEventStore` directly on Vespa documents (via the `VespaEventStore.open` handle).
- **The relay is the API — and a separate app.** Clients speak plain NIP-01
  filters and NIP-50 search over websockets to **vespa-relay** (its own repo/app);
  there is no HTTP search endpoint, and its web UI is just another Nostr client.
  SoT (this repo) never serves clients — it only fills the store vespa-relay reads.

Plain JVM (Kotlin 2.4, JDK 21). Docker runs only Vespa; every module is ordinary
JVM, so you can point `VESPA_URL` at a remote Vespa and skip Docker.

`README.md` explains the project in plain terms; `docs/` holds the design
proposals.

## Module map & dependency direction

The store is a **reusable library** (vespa-eventstore), consumed from JitPack
(pinned by commit in `gradle/libs.versions.toml`). The relay is a **separate
application** (vespa-relay) run alongside — SoT does NOT link it. SoT is just the
crawl (`:sync`) and the CLI (`:cli`), depending only on vespa-eventstore.
Dependencies flow one way: `:cli` → `:sync` → vespa-eventstore (`:store` → `:vespa`).
Notes:

- **The store is engine-agnostic.** `NostrEventStore` enforces Nostr rules over
  any `EventIndex`; `VespaEventStore` (the handle from `VespaEventStore.open`) is
  the Vespa composition. Tests run the store over `InMemoryEventIndex`.
- **The trust projection sits UNDER the store** (it decorates the store's
  `EventIndex`), so ranking updates follow every insert and delete automatically.
- **SoT and vespa-relay share ONE Vespa, not a process.** SoT writes trust data +
  content IN; vespa-relay reads and ranks OUT. There is no in-process seam.

### The store library (vespa-eventstore, package `com.vitorpamplona.quartz.eventstore.*`)

```
vespa-eventstore  (github.com/vitorpamplona/vespa-eventstore)
  :vespa  All Vespa access (…eventstore.vespa), in sub-packages:
            doc/   — the stored shapes: EventDoc (event <-> document +
                     complete-event reconstruction), ReputationDoc/ReputationCells,
                     SearchFields.
            query/ — EventQuery -> YQL: EventYql builds the filter (plus the
                     grouping/count variants via one grouping() helper),
                     FuzzyWordGroup the per-word fuzzy recall, EventSelection the
                     document-visit selection.
            client/— the EventIndex/ReputationIndex ports (+ DocRef) and the real
                     clients (VespaEventIndex/VespaReputationIndex).
            root   — shared helpers (Concurrency, IngestStats, Patterns, VespaApp).
            app/ — the Vespa application package: event.sd (lossless NIP-01 fields
                   + per-kind search fields + rank profiles) and reputation.sd (the
                   global trust-tensor doc every event references for ranking).
            main also holds InMemoryEventIndex (the reference impl, also used in
                   production for the bulk replay snapshot). testFixtures:
                   MockVespaEngine (parses the emitted YQL back and must agree with
                   it) + InMemoryReputationIndex.
  :store  NostrEventStore : IEventStore (…eventstore.store) — the one store,
            engine-AGNOSTIC. Enforces Nostr rules on insert (supersession, kind-5
            deletion + tombstones, kind-62 vanish, NIP-40 expiry, gift-wrap
            ownership). BulkInsert is the batched fast path behind batchInsert.
            Also: Filter -> EventQuery mapping (with the NIP-50 extensions),
            SearchExtractors (each Quartz SearchableEvent kind -> the schema's
            per-kind search fields), negentropy snapshots, ObserverContext +
            OriginalFilters (per-connection ranking observer + pre-strip filters,
            carried on the coroutine context), the TrustProjection (an EventIndex
            decorator that watches 30382/10040 puts and removes and rewrites the
            reputation parent docs — observer-keyed influence_scores/
            follower_counts), and VespaEventStore.open (the front door / handle).
```

### The relay (vespa-relay — a SEPARATE APP, not a SoT dependency)

`NostrRelayServer` (package …eventstore.relay) is Quartz's protocol engine over the
store — full-filter REQs, live subscriptions, VerifyPolicy-gated publishes, NIP-45
COUNT, server-side NIP-77, the NIP-11 doc — wrapped by `serveRelay` into a runnable
app that also serves the web UI. It lives in its own repo
(github.com/vitorpamplona/vespa-relay), pointed at the same Vespa SoT fills; SoT
neither imports nor runs it.

### SoT modules (this repo, package `com.vitorpamplona.sot.*`)

```
sync     The trust-sync side (…sot.sync): RelaySyncer downloads events (NIP-77
           negentropy or paged fallback, per-scope cursors) and streams them
           through EventStreamPipeline (bounded-channel verify -> batchInsert);
           NostrAuthHandshake handles NIP-42 first contact. Identity is the
           indexer's own key: it authenticates upstream and, on first run,
           self-publishes its kind 0 / 10002 / 10086 into its own store — the stored
           10086 IS the indexer configuration. TrustSync + BlendedPass walk the
           trust chain in dependency order: seed 10040 hints -> observer 10002s ->
           observer outboxes (the authoritative 10040) -> provider 30382s +
           reconcile -> orphan sweep. The crawl's bookkeeping is a local file
           (CrawlIndex / FileCrawlIndex), NOT a Vespa doctype. SyncService runs it
           once or on a loop.
         Depends on: vespa-eventstore (:store), quartz, okhttp.
cli      `sot` — the one executable and composition root. openStack() wires
           VespaEventStore.open (TrustProjection under the store) + FileCrawlIndex,
           handed to SyncService. Commands: init (interactive setup) | serve (the
           crawl on a loop) | index (one pass) | status | up | down | destroy |
           deploy. Config resolves env -> .env -> default.
         Depends on: vespa-eventstore, :sync.
```

## Data flow

- **Ingest (SoT's job)**: relays --(RelaySyncer: verify, then batchInsert)-->
  `NostrEventStore` --(insert rules + SearchExtractors + owner/expires_at
  derivation)--> `TrustProjection(VespaEventIndex, VespaReputationIndex)` -->
  Vespa. A 30382 or 10040 write recomputes the subject's `reputation` doc (its
  trust tensors) on the way through.
- **Query (vespa-relay's job, separate app)**: REQ/COUNT --> `NostrRelayServer` -->
  `ObserverContext(observer)` --> store --> `EventYql` --> Vespa `/search/` -->
  complete events rebuilt from the stored fields (signature-valid). Search results
  keep Vespa's relevance order (NIP-50); plain filters keep recency order. This runs
  in vespa-relay, against the same Vespa SoT fills.
- **Observer-keyed trust (NIP-85)**: kind 10040 maps a service key -> observer;
  kind 30382 (signed by the service key) carries the score; scores live as
  `influence_scores{OBSERVER}` cells on the SUBJECT's reputation doc, keyed by the
  10040 *author*, not the 30382 signer. The authoritative 10040 is the one on the
  observer's own OUTBOX relay — which is why sync fetches 10002s first.

## Build, run, test

Use the committed wrapper `./gradlew` (Gradle 9.6.1). The wrapper fetches from
`downloads.gradle.org` — the direct host, since the canonical URL redirects to
GitHub releases, which some networks block. A preinstalled Gradle >= 8.14.4 works
too (`gradle …` instead of `./gradlew …`).

```bash
./gradlew build                 # compile + ALL tests + spotlessCheck (the gate)
./gradlew test                  # unit tests; the wire tests run against
                                # MockVespaEngine (testFixtures of :vespa), the
                                # sync tests against in-process Quartz relays
./gradlew spotlessApply         # auto-format + license headers (run before committing)

./gradlew :cli:installDist      # build the CLI
export PATH="$PWD/cli/build/install/sot/bin:$PATH"
sot init                        # interactive setup -> .env (--yes = all defaults)
sot up                          # docker compose up Vespa + deploy the bundled schema
sot serve                       # the crawl on a loop: trust-sync every SYNC_INTERVAL min
sot index                       # one trust-sync pass, then exit
sot status                      # Vespa up? per-kind event counts + coverage
sot destroy                     # wipe sync cursors + Vespa's data volume
```

## Configuration

All config resolves **env var -> `.env` -> built-in default** via `Config` (in
`:cli`); `sot init` writes the `.env` interactively. Keys: `VESPA_URL`,
`VESPA_CONFIG_URL`, `VESPA_PORT`/`VESPA_CONFIG_PORT` (docker port mapping),
`RELAY_URL` (the indexer's own ws url — its NIP-42 identity, its kind-10002 outbox,
the NIP-62 vanish scope), `SYNC_INTERVAL` (minutes; 0 = one pass then exit),
`SYNC_STATE` (cursor file), `SEED_RELAYS` (kind-10040 discovery hints), `HOUSE_NPUB`/
`HOUSE_RELAY` (the observer behind unauthenticated searches + where its first
10002 is synced from), `QUARTZ_LOG_LEVEL`, the indexer's kind-0 identity
`SERVER_NAME/DESCRIPTION/ICON`, and `SERVER_NSEC` (the indexer's own key;
`sot init` generates it).

Deliberately absent: no `EVENTS_DB` (Vespa is the store), no `DEFAULT_OBSERVER`
(the house account replaces it), no indexer-relay key — the identity's stored
kind-10086 IS the indexer list (supersede it from any Nostr client; the sync
reads the newest one back).

## Conventions

- **Formatting is enforced.** Spotless + ktlint 1.7.1. Run `./gradlew spotlessApply`
  before committing; `build` fails on unformatted code. Config: `.editorconfig`
  (`max_line_length = 5000`), root `build.gradle.kts` (the spotless block), header
  in `.spotless/copyright.kt` (required on every `.kt` file; `spotlessApply` adds it).
- **Comments**: `/** … */` KDoc attaches to a declaration; a floating file-overview
  should be a plain `/* … */` block comment.
- **Dependencies** live in the version catalog `gradle/libs.versions.toml` (`libs.*`).
- **Quartz** (Amethyst's Nostr library) comes from JitPack, pinned by commit in the
  catalog (`quartz = "<commit>"`). Bumping it may need a JitPack build (the first
  request can 4xx while it builds — retry).
- Package root is `com.vitorpamplona.sot`; module = subpackage.

## Nostr / Vespa reference

- NIPs across the system: 01 (filters/publishes), 05, 07+42 (client login ->
  per-user ranking, in vespa-relay; SoT uses 42 to authenticate upstream relays),
  09 (deletion + tombstones), 11, 19, 40 (never
  serve expired), 45 (COUNT), 50 (search + `sort:`/`filter:rank:`/`include:spam`
  extensions), 62 (vanish, inclusive), 65 (outbox routing in sync), 77
  (negentropy: server-side AND as the sync transport), 85 (trusted assertions),
  plus kind 10086 (IndexerRelayListEvent — the indexer configuration).
- The Vespa schemas and rank profiles live in the **vespa-eventstore** repo
  (`vespa/app/`), not here. They come from Brainstorm's ranking design
  (multiplicative `wot_mult` trust, IDF identity fields, gram safety nets),
  extended with generic tiers for non-profile kinds so a single `search` rank
  profile can rank a mixed-kind result set. In that repo, change the schema and
  `EventYql`/`FuzzyWordGroup` together; `MockVespaEngine`'s parser is the drift alarm.

## Gotchas

- **Quartz strips NIP-50 extensions before the store.** `LiveEventStore` removes
  the `sort:`/`filter:rank:`/`include:spam` tokens from every REQ's search. The
  relay backend carries the pre-strip filters in `OriginalFilters` (a
  coroutine-context element, like the observer) and the store restores them. The
  session-level test (`RelayProtocolTest`, in the vespa-relay repo) is the net for
  future Quartz bumps.
- **Signature verification happens at ingest** (RelaySyncer drops forged events;
  vespa-relay's VerifyPolicy gates publishes). The store itself NEVER verifies —
  don't insert unverified network input directly.
- **Search results are relevance-ordered** (NIP-50): the store must not re-sort a
  searching query by `created_at` (see `SearchOrderTest`). Plain filters stay
  newest-first.
- **Ranked search is trust-gated** at `DEFAULT_MIN_RANK` (Brainstorm's
  onlyRanked): a fresh install with no synced scores returns NOTHING for ranked
  searches until the observer's chain syncs — `include:spam` bypasses the gate.
  Plain no-search REQs are never gated.
- Don't run `sot index` while `sot serve` is mid-pass against the same Vespa:
  semantics stay correct (single store), but they double-download.
- **Scale prerequisites for kind-1 volume** (see `docs/v2-sync-proposal.md`):
  visit-based negentropy snapshots (`EventIndex.visitIds`) and the bulk-ingest
  fast path (`BulkInsert` behind `batchInsert`) have landed; `attribute: paged`
  on fat attributes is still deferred, and broad kind-1 sync across arbitrary
  public relays is only lightly exercised.
- More detail: `README.md` and `docs/`.
