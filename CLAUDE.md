# CLAUDE.md

Guidance for AI agents (and humans) working in this repo. Keep it current when the
architecture changes.

## What this is

**SoT — Search over Trust.** A Nostr search relay ranked by the searching user's
**web of trust**: events sync from the network into [Vespa](https://vespa.ai), and
NIP-50 searches rank each hit by the trust score the *observer* (the
NIP-42-authenticated user, else the operator's house account) assigned to its
author via NIP-85.

The architecture in one line: **Vespa IS the event store, and the relay IS the
API.** There is no SQLite, no separate index to keep in sync, and no http search
API — `VespaEventStore` implements Quartz's `IEventStore` on Vespa documents,
the relay serves full NIP-01 filters + NIP-50 over websockets, and even the
bundled web UI is just another Nostr client speaking NIP-50 to it.

Plain JVM (Kotlin 2.4, JDK 21). Docker runs only Vespa; every module is ordinary
JVM, so you can point `VESPA_URL` at a remote Vespa and skip Docker.

`README.md` records the design decisions of the rewrite; `docs/` holds the
proposals it grew from.

## Module map & dependency direction

```
vespa    ALL Vespa access, Nostr-agnostic (com.vitorpamplona.sot.vespa):
           app/ — THE Vespa application package: event.sd (lossless NIP-01 fields +
           per-kind search fields + Brainstorm rank profiles) and profile.sd (the
           GLOBAL trust-tensor parent every event references). Change schema and
           Kotlin together.
           EventDoc (field mapping + complete-event reconstruction), EventQuery ->
           YQL (EventYql assembles NIP-01/NIP-50 filters + sort/filter extension
           params; BrainstormWordGroup builds the per-word fuzzy recall), the
           EventIndex/ProfileIndex ports, and the real
           clients (VespaEventIndex/VespaProfileIndex: h2c feed writes, document-API
           gets, /search/ queries). testFixtures: InMemoryEventIndex (the executable
           spec) + MockVespaEngine (parses emitted YQL back and must agree with it).
         Depends on: kotlinx-serialization, vespa-feed-client, (test) jetty.
store    VespaEventStore : IEventStore (com.vitorpamplona.sot.store) — the ONE
           store: Nostr semantics enforced on insert (supersession, kind-5 +
           tombstones, kind-62 vanish, NIP-40, gift-wrap OWNER semantics), the
           BulkInsert fast path behind batchInsert (same rules, batched I/O — the
           sync-scale ingest path), Filter -> EventQuery mapping (NIP-50 extensions
           -> rank profile + min_rank),
           SearchExtractors (every Quartz SearchableEvent kind -> the schema's
           per-kind search fields), negentropy snapshots, ObserverContext (the
           coroutine element carrying the per-connection ranking observer).
         Depends on: :vespa, quartz.
profile  TrustProjection (com.vitorpamplona.sot.profile): an EventIndex DECORATOR —
           watches 30382/10040 puts/removes and rewrites the profile parent docs
           (observer-keyed quality_scores/follower_counts). Sits UNDER the store,
           so every deletion path updates ranking with zero special code.
         Depends on: :vespa, quartz.
relay    SotRelayServer (com.vitorpamplona.sot.relay): Quartz's protocol engine
           (RelayServerBase + LiveEventStore) over the store — full-filter REQs,
           live subs, VerifyPolicy-gated publishes, NIP-45 COUNT, server-side
           NIP-77, NIP-11 doc, Ktor websocket mount. NIP-42 auth switches the
           ranking observer per connection and fires onObserver (sync enrollment).
         Depends on: :store, quartz, ktor.
sync     The trust-sync side (com.vitorpamplona.sot.sync): RelaySyncer (the
           download orchestrator: NIP-77 negentropy + paged fallback, per-scope
           cursors — delegating to EventStreamPipeline [bounded-channel verify->
           batchInsert], NostrAuthHandshake [NIP-42 first contact], CursorScope),
           Identity (the relay's own key: NIP-42 client auth upstream + first-run
           self-publish of kind 0/10002/10086 into its OWN store — the stored 10086
           IS the indexer configuration), TrustSync + BlendedPass (the scores
           plane, run as a BLENDED work-unit pipeline over relays — not strict
           phase barriers — in the dependency order: seed 10040 hints -> observer
           10002s from index relays -> outboxes [the AUTHORITATIVE 10040] ->
           provider 30382s + reconcile -> orphan sweep), SyncService
           (runOnce/runForever + enroll()).
         Depends on: :vespa via :store (tests), quartz, okhttp.
cli      `sot` — the ONE executable and composition root (wires
           TrustProjection(VespaEventIndex) under VespaEventStore, shared by
           SotRelayServer and SyncService). Commands: init (INTERACTIVE setup) |
           serve | index | status | up | down | destroy | deploy. Config resolves
           env -> .env -> default. Bundles web/index.html.
         Depends on: all of the above.
web      index.html — the search UI, itself a Nostr client: NIP-50 REQs over the
           serving relay's own websocket, NIP-07 -> NIP-42 login, kind chips as
           literal kinds filters, every indexed kind rendered as a card.
           `./gradlew :cli:uiDemo` serves it over an in-memory relay (no Vespa).
```

Key rules: **`:vespa` never imports a Nostr/Quartz type** (dependency flows
`:store -> :vespa`, never the reverse), and **the projection sits UNDER the
store** (decorating its `EventIndex`), so ranking follows every insert/delete
path automatically.

## Data flow

- **Ingest**: relays --(RelaySyncer, verify-then-batchInsert)--> `VespaEventStore`
  --(insert rules + SearchExtractors + owner/expires_at derivation)-->
  `TrustProjection(VespaEventIndex)` --> Vespa. 30382/10040 writes recompute the
  subject's `profile` parent doc (trust tensors) on the way through.
- **Query**: REQ/COUNT --> `SotRelayServer` --> `ObserverContext(observer)` -->
  store --> `EventYql` --> Vespa `/search/` (rank profiles in `vespa/app`) -->
  complete events reconstructed from doc fields (signature-valid). Search results
  keep Vespa's relevance order (NIP-50); plain filters keep recency order.
- **Observer-keyed trust (NIP-85)**: kind 10040 maps a service key -> observer;
  kind 30382 (signed by the service key) carries the rank; scores live as
  `quality_scores{OBSERVER}` cells on the SUBJECT's profile doc, keyed by the
  10040 *author*, not the 30382 signer. The authoritative 10040 is the one on the
  observer's own OUTBOX relay — sync order (10002 first) exists for exactly that.

## Build, run, test

Use the committed wrapper `./gradlew` (Gradle 9.6.1; the wrapper fetches from
`downloads.gradle.org` — the direct host, since the canonical URL redirects to
GitHub releases, which some networks block; a preinstalled Gradle >= 8.14 works
too).

```bash
./gradlew build                 # compile + ALL tests + spotlessCheck (the gate)
./gradlew test                  # unit tests; the wire tests run against
                                # MockVespaEngine (testFixtures of :vespa), the
                                # sync tests against in-process Quartz relays
./gradlew spotlessApply         # auto-format + license headers (run before committing)

./gradlew :cli:installDist      # build the CLI
export PATH="$PWD/cli/build/install/sot/bin:$PATH"
sot init                        # INTERACTIVE setup -> .env (--yes = all defaults)
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
`SERVER_PORT`, `SERVER_URL`, `RELAY_URL` (the relay's public ws url — NIP-42
identity + the identity's 10002), `SYNC_INTERVAL` (minutes; 0 = serve-only),
`SYNC_STATE` (cursor file), `SEED_RELAYS` (kind-10040 discovery hints),
`HOUSE_NPUB`/`HOUSE_RELAY` (the observer behind unauthenticated searches + where
its first 10002 is synced from), `QUARTZ_LOG_LEVEL`, the NIP-11 identity
`SERVER_NAME/DESCRIPTION/ICON/PUBKEY`, and `SERVER_NSEC` (the relay's OWN key;
`sot init` generates it). Deliberately absent: no `EVENTS_DB` (Vespa is the
store), no `DEFAULT_OBSERVER` (the house account replaces it), no indexer-relay
key — **the identity's stored kind-10086 IS the indexer list** (supersede it from
any Nostr client; the sync reads the newest back).

## Conventions

- **Formatting is enforced.** Spotless + ktlint 1.7.1. Run `./gradlew spotlessApply`
  before committing; `build` fails on unformatted code. Config: `.editorconfig`
  (`max_line_length = 5000`), root `build.gradle.kts` (the spotless block), header
  in `.spotless/copyright.kt` (required on every `.kt` file; `spotlessApply` adds it).
- **Comments**: `/** … */` KDoc attaches to a declaration; a floating file-overview
  should be a plain `/* … */` block comment.
- **Dependencies** live in the version catalog `gradle/libs.versions.toml` (`libs.*`).
- **Quartz** (Amethyst's Nostr library) comes from JitPack, pinned by commit in the
  catalog (`quartz = "<commit>"`). Bumping it may need a JitPack build (first
  request can 4xx while it builds — retry).
- Package root is `com.vitorpamplona.sot`; module = subpackage.

## Nostr / Vespa reference

- NIPs implemented: 01 (filters/publishes), 05, 07+42 (web UI login -> per-user
  ranking + observer enrollment), 09 (deletion + tombstones), 11, 19, 40 (never
  serve expired), 45 (COUNT), 50 (search + `sort:`/`filter:rank:`/`include:spam`
  extensions), 62 (vanish, inclusive), 65 (outbox routing in sync), 77
  (negentropy: server-side AND as the sync transport), 85 (trusted assertions),
  plus kind 10086 (IndexerRelayListEvent — the indexer configuration).
- The Vespa schemas + rank profiles in `vespa/app/` are ported VERBATIM from
  Brainstorm's doc.sd (multiplicative `wot_mult` trust, IDF identity fields, gram
  safety nets) with sot's generic tier extension for non-profile kinds — a single
  `search` profile ranks a mixed-kind result set. Change schema and
  `EventYql`/`BrainstormWordGroup` together; `MockVespaEngine`'s parser is the
  drift alarm.

## Gotchas

- **Validated against a real Vespa** (2026-07): `sot up` deploys `vespa/app`
  clean, and the full protocol path passed an acceptance run against the
  deployed engine — feed, recall, reconstruction, COUNT, the trust gate,
  NIP-42-ranked search through the imported tensors, `sort:rank` ordering,
  the NIP-50 extensions, and kind-5 deletion. The sync side has since driven
  a multi-million-event (~11M) load against a real provider relay
  (`nip85.nosfabrica.com`, the trust-scores plane); broad kind-1 sync across
  arbitrary public relays is still only lightly exercised. Two operational
  notes from the acceptance run:
  `services.xml` raises Vespa's feed-block resource limits (dev disks trip
  the 80% default), and **Quartz's `LiveEventStore` strips NIP-50 extensions
  before the store** — the relay backend carries the pre-strip filters in
  `OriginalFilters` (a coroutine-context element, like the observer) and the
  store restores them; the session-level test in `RelayProtocolTest` is the
  net for future Quartz bumps.
- Event **signature verification happens at ingest** (RelaySyncer drops forged
  events; the relay's VerifyPolicy gates publishes). The store itself NEVER
  verifies — don't insert unverified network input directly.
- **Search results are relevance-ordered** (NIP-50): the store must not re-sort
  a searching query by `created_at` (see `SearchOrderTest`). Plain filters stay
  newest-first.
- Every ranked search is trust-gated at `DEFAULT_MIN_RANK` (Brainstorm's
  onlyRanked): a fresh install with no synced scores returns NOTHING for ranked
  searches until the observer's chain syncs — `include:spam` bypasses the gate.
- Don't run `sot index` while `sot serve` is mid-pass against the same Vespa:
  semantics stay correct (single store), but they double-download.
- Scale prerequisites for kind-1 volume (see `docs/v2-sync-proposal.md`):
  visit-based negentropy snapshots (`EventIndex.visitIds`) and the bulk-ingest
  fast path (`BulkInsert` behind `batchInsert`) have LANDED; `attribute: paged`
  on fat attributes remains deferred.
- More detail: `README.md` (the decisions), `docs/` (the proposals).
