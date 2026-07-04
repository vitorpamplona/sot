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
  in sync — one copy of the data. `VespaEventStore` implements Quartz's
  `IEventStore` directly on Vespa documents.
- **The relay is the API.** Clients speak plain NIP-01 filters and NIP-50
  search over websockets. There is no HTTP search endpoint. Even the bundled web
  UI is just another Nostr client talking to the relay.

Plain JVM (Kotlin 2.4, JDK 21). Docker runs only Vespa; every module is ordinary
JVM, so you can point `VESPA_URL` at a remote Vespa and skip Docker.

`README.md` explains the project in plain terms; `docs/` holds the design
proposals.

## Module map & dependency direction

Dependencies flow one way: `:cli` → (`:relay`, `:sync`) → `:store` → `:profile`
→ `:vespa`. Notes:

- **`:vespa` may use Quartz.** It owns all Vespa access and is free to reuse
  Quartz's Nostr primitives (Hex, event helpers) instead of re-implementing
  them. (Earlier it was Nostr-agnostic; that constraint has been dropped.)
- **The trust projection sits UNDER the store** (it decorates the store's
  `EventIndex`), so ranking updates follow every insert and delete automatically.

```
vespa    All Vespa access (com.vitorpamplona.sot.vespa), in sub-packages:
           doc/   — the stored shapes: EventDoc (event <-> document +
                    complete-event reconstruction), ProfileDoc, SearchFields.
           query/ — EventQuery -> YQL: EventYql builds the filter,
                    BrainstormWordGroup the per-word fuzzy recall, EventSelection
                    the document-visit selection.
           client/— the EventIndex/ProfileIndex ports and the real clients
                    (VespaEventIndex/VespaProfileIndex).
           root   — shared helpers (Concurrency, IngestStats, Patterns).
           app/ — the Vespa application package: event.sd (lossless NIP-01
           fields + per-kind search fields + rank profiles) and profile.sd (the
           global trust-tensor doc every event references for ranking).
           testFixtures: InMemoryEventIndex (the reference implementation) +
           MockVespaEngine (parses the emitted YQL back and must agree with it).
         Depends on: kotlinx-serialization, vespa-feed-client, quartz, (test) jetty.
store    VespaEventStore : IEventStore (com.vitorpamplona.sot.store) — the one
           store. Enforces Nostr rules on insert (supersession, kind-5 deletion
           + tombstones, kind-62 vanish, NIP-40 expiry, gift-wrap ownership).
           BulkInsert is the batched fast path behind batchInsert (same rules,
           batched I/O — the sync-scale ingest path). Also: Filter -> EventQuery
           mapping (with the NIP-50 extensions), SearchExtractors (each Quartz
           SearchableEvent kind -> the schema's per-kind search fields),
           negentropy snapshots, and ObserverContext (the per-connection
           ranking observer, carried on the coroutine context).
         Depends on: :vespa, quartz.
profile  TrustProjection (com.vitorpamplona.sot.profile): an EventIndex
           decorator that watches 30382/10040 puts and removes and rewrites the
           profile parent docs (observer-keyed quality_scores/follower_counts).
           Because it sits under the store, every deletion path updates ranking
           with no special-case code.
         Depends on: :vespa, quartz.
relay    SotRelayServer (com.vitorpamplona.sot.relay): Quartz's protocol engine
           (RelayServerBase + LiveEventStore) over the store — full-filter REQs,
           live subscriptions, VerifyPolicy-gated publishes, NIP-45 COUNT,
           server-side NIP-77, the NIP-11 doc, mounted on a Ktor websocket.
           NIP-42 auth switches the ranking observer per connection and fires
           onObserver (which enrolls the user for sync).
         Depends on: :store, quartz, ktor.
sync     The trust-sync side (com.vitorpamplona.sot.sync): RelaySyncer
           downloads events (NIP-77 negentropy or paged fallback, per-scope
           cursors) and streams them through EventStreamPipeline (bounded-channel
           verify -> batchInsert); NostrAuthHandshake handles NIP-42 first
           contact. Identity is the relay's own key: it authenticates upstream
           and, on first run, self-publishes its kind 0 / 10002 / 10086 into its
           own store — the stored 10086 IS the indexer configuration. TrustSync +
           BlendedPass walk the trust chain in dependency order: seed 10040 hints
           -> observer 10002s -> observer outboxes (the authoritative 10040) ->
           provider 30382s + reconcile -> orphan sweep. SyncService runs it once
           or on a loop and enrolls new observers.
         Depends on: :vespa via :store (tests), quartz, okhttp.
cli      `sot` — the one executable and composition root. Wires
           TrustProjection(VespaEventIndex) under VespaEventStore, shared by
           SotRelayServer and SyncService. Commands: init (interactive setup) |
           serve | index | status | up | down | destroy | deploy. Config
           resolves env -> .env -> default. Bundles web/index.html.
         Depends on: all of the above.
web      index.html — the search UI, itself a Nostr client. It opens a websocket
           to the server that served it and speaks NIP-50 REQs directly: kind
           chips are literal kinds filters, NIP-07 -> NIP-42 login makes results
           ranked by you, and every indexed kind renders as a card.
           `./gradlew :cli:uiDemo` serves it over an in-memory relay (no Vespa).
```

## Data flow

- **Ingest**: relays --(RelaySyncer: verify, then batchInsert)-->
  `VespaEventStore` --(insert rules + SearchExtractors + owner/expires_at
  derivation)--> `TrustProjection(VespaEventIndex)` --> Vespa. A 30382 or 10040
  write recomputes the subject's `profile` doc (its trust tensors) on the way
  through.
- **Query**: REQ/COUNT --> `SotRelayServer` --> `ObserverContext(observer)` -->
  store --> `EventYql` --> Vespa `/search/` --> complete events rebuilt from the
  stored fields (signature-valid). Search results keep Vespa's relevance order
  (NIP-50); plain filters keep recency order.
- **Observer-keyed trust (NIP-85)**: kind 10040 maps a service key -> observer;
  kind 30382 (signed by the service key) carries the score; scores live as
  `quality_scores{OBSERVER}` cells on the SUBJECT's profile doc, keyed by the
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
sot up                          # docker compose up Vespa + deploy vespa/app
sot serve                       # ONE port (SERVER_PORT): relay + NIP-11 + web UI
                                # + background trust sync every SYNC_INTERVAL min
sot index                       # one trust-sync pass, then exit
sot status                      # Vespa/server up? per-kind event counts
sot destroy                     # wipe sync cursors + Vespa's data volume

./gradlew :cli:uiDemo           # web-UI dev: in-memory relay + seeded demo events
```

## Configuration

All config resolves **env var -> `.env` -> built-in default** via `Config` (in
`:cli`); `sot init` writes the `.env` interactively. Keys: `VESPA_URL`,
`VESPA_CONFIG_URL`, `VESPA_PORT`/`VESPA_CONFIG_PORT` (docker port mapping),
`SERVER_PORT`, `SERVER_URL`, `RELAY_URL` (the relay's public ws url — its NIP-42
identity and its 10002), `SYNC_INTERVAL` (minutes; 0 = serve-only), `SYNC_STATE`
(cursor file), `SEED_RELAYS` (kind-10040 discovery hints), `HOUSE_NPUB`/
`HOUSE_RELAY` (the observer behind unauthenticated searches + where its first
10002 is synced from), `QUARTZ_LOG_LEVEL`, the NIP-11 identity
`SERVER_NAME/DESCRIPTION/ICON/PUBKEY`, and `SERVER_NSEC` (the relay's own key;
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

- NIPs implemented: 01 (filters/publishes), 05, 07+42 (web UI login -> per-user
  ranking + observer enrollment), 09 (deletion + tombstones), 11, 19, 40 (never
  serve expired), 45 (COUNT), 50 (search + `sort:`/`filter:rank:`/`include:spam`
  extensions), 62 (vanish, inclusive), 65 (outbox routing in sync), 77
  (negentropy: server-side AND as the sync transport), 85 (trusted assertions),
  plus kind 10086 (IndexerRelayListEvent — the indexer configuration).
- The Vespa schemas and rank profiles live in `vespa/app/`. They come from
  Brainstorm's ranking design (multiplicative `wot_mult` trust, IDF identity
  fields, gram safety nets), extended with generic tiers for non-profile kinds so
  a single `search` rank profile can rank a mixed-kind result set. Change the
  schema and `EventYql`/`BrainstormWordGroup` together; `MockVespaEngine`'s parser
  is the drift alarm.

## Gotchas

- **Quartz strips NIP-50 extensions before the store.** `LiveEventStore` removes
  the `sort:`/`filter:rank:`/`include:spam` tokens from every REQ's search. The
  relay backend carries the pre-strip filters in `OriginalFilters` (a
  coroutine-context element, like the observer) and the store restores them. The
  session-level test in `RelayProtocolTest` is the net for future Quartz bumps.
- **Signature verification happens at ingest** (RelaySyncer drops forged events;
  the relay's VerifyPolicy gates publishes). The store itself NEVER verifies —
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
