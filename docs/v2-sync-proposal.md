# Proposal: `:v2:sync` — outbox-driven ingest for the Vespa store

> Status: **proposal** — the bridge from v1's indexer (relay-centric hoovering
> into SQLite) to v2's model (author-centric outbox sync into the Vespa store).
> Requirements it answers: (1) constant sync of user records from their outbox
> relays, searchable kinds only; (2) constant sync of observers' scores from
> their 10040-chosen providers; (3) negentropy first, plain REQ fallback;
> (4) tracking kind-10002 outbox definitions; (5) per-kind field-priority
> indexing (Brainstorm-style kind-0 weighting; title > summary > content for
> long-form; etc.).

## The shift: from relay-centric to author-centric

v1 syncs *relays*: crawl NIP-65 to find ~200 relays, download every kind-0
they hold (millions of profiles, most of which nobody's web of trust ever
touches). v2 syncs *authors*: the set of pubkeys the trust graph actually
covers, from the relays **they** declared in their 10002. The dataset shrinks
to "people someone scored," coverage improves (an author's outbox has their
latest events by definition), and the blind Discovery crawl disappears.

Call that pubkey set the **roster**:

```
roster = subjects of every synced kind-30382
       ∪ observers (kind-10040 authors) and their service keys
       ∪ operator-seeded extras
```

## The feedback loop (steady state)

The store is the only state. Every arrow below is either "sync into the
store" or "react to the store's change feed" — `ObservableEventStore`
wrapping `VespaEventStore` gives us the reactions for free, the same feed
v1's projection consumed:

```
seed relays ──10040──▶ store ──change──▶ provider (service key, relay hint) known
provider relay ──30382──▶ store ──change──▶ subject joins the ROSTER
index relays + outboxes ──10002──▶ store ──change──▶ OUTBOX DIRECTORY updated
author's outbox relays ──searchable kinds + 5 + 62 + 10002──▶ store
```

A new score's subject automatically gets their 10002 looked up; a new 10002
automatically reassigns that author to the right relay work-sets; a changed
10040 automatically schedules the new provider. Bootstrap is just running the
loop from empty in dependency order (10040 → 30382 → roster → 10002 →
outboxes); steady state is the same loop driven by change events plus a slow
reconcile cadence.

### Requirement 4 — the 10002 directory

Kind 10002 is synced *into the store* like any other kind (it's replaceable —
the store keeps exactly the latest per author; a hand-rolled side-database
would just re-implement that). The **OutboxDirectory** component is a cached
view over `store.query(kinds=[10002], authors=roster-chunk)` exposing:

- `writeRelaysOf(pubkey)` — parsed via Quartz's `AdvertisedRelayListEvent`
- the **inverted map**: relay → roster authors who write there (the sync
  work-sets), with authors batched ~500 per filter like v1's
  `AUTHORS_PER_FILTER`
- staleness: refresh a roster member's 10002 when it's older than N days,
  from the index relays + their last-known outbox (10002s live in outboxes
  too)

Where do 10002s come from initially? `INDEX_RELAYS` — a small configured list
of aggregators that specialize in 0/3/10002 (purplepag.es-style) — plus the
seed relays. Roster members with **no** 10002 anywhere fall back to
`INDEX_RELAYS` for their searchable kinds too, so a missing relay list
degrades coverage, never drops the author.

### Requirements 1 + 2 — the two sync planes

**Scores plane** (per observer): unchanged in spirit from v1 phase 3. For
each `30382:rank` provider in a stored 10040: sync
`{kinds:[30382,5], authors:[service keys]}` from the provider's relay hint,
batched per relay. Silent removals are detected by periodic full
reconciliation of the scope (see negentropy below). This plane is
authoritative per observer — one provider relay per 10040 entry, exactly the
scope model that survived from the mirror design.

**Records plane** (per author): for each (outbox relay → author batch) unit
from the directory: sync `{kinds: SYNC_KINDS + [5, 62, 10002], authors: batch}`.
Kind 5 and 62 ride along because the v2 store *interprets* them (unlike the
old mirror design, deletion semantics are local again — an author's deletion
published only to their outbox still erases here). `SYNC_KINDS` is config,
defaulting to the curated searchable set (0, 30023, 31922/31923?, git kinds,
classifieds, …) — NOT kind 1; notes are volume without search value for this
product.

### "Constant": live subscriptions + reconcile cadence

Two mechanisms per relay, complementary:

- **Live subscriptions**: after a work-unit's first sync, keep an open REQ
  (`since = now`) for the same filter and stream inserts continuously
  (verify → `batchInsert`). Relays cap subscriptions per connection, so
  units rotate through a bounded sub pool — **prioritized by trust score**:
  authors with high aggregate scores hold live subs; the long tail is
  covered by the reconcile cadence only. Live is best-effort freshness;
  correctness never depends on it.
- **Reconcile cadence** (`SYNC_INTERVAL`): periodic delta sync per work
  unit. This is the correctness backstop: catches anything live subs missed,
  plus deletions.

### Requirement 3 — negentropy first, REQ fallback

v1's `RelaySyncer` already does exactly this — `negentropySyncOrFetch` with
per-(relay, kind, authors) cursors, per-relay capability memory (a relay that
can't reconcile is remembered and paged thereafter), streamed
verify-and-insert with backpressure, and the pages fallback that works on
every relay. **It ports almost unchanged**: it only ever talked to
`ObservableEventStore`, which now wraps `VespaEventStore`. `SyncState`
(cursors, capability flags) and `SyncProgress` come along.

The v2 upgrade, when Quartz ships the seeding API (see
`docs/negentropy-first-architecture.md`'s upstream asks): seed each
reconciliation with `store.snapshotIdsForNegentropy(filter)` — the store
serves `(id, created_at)` straight off doc attributes. That gives:

- **delta-only transfers in both directions** (today's cursor approach
  re-downloads nothing but also *detects* nothing missing);
- **`haveIds` = deletions detected natively** on the scores plane — the
  full-enumeration `reconcile()` hack retires;
- a free cross-relay dedupe: the seed is the local set for the *filter*, so
  an author's second outbox relay only sends what no relay sent before.

Until then: cursors + v1's `reconcile()` enumeration on a slow cadence for
the scores plane, exactly like today.

### Requirement 5 — per-kind field-priority indexing

Replace the single `search_text` field with three kind-agnostic **tiers** in
the event schema, all bm25-indexed:

```
search_primary     kind 0: name + display_name        30023: title
search_secondary   kind 0: nip05 + lud16 domain       30023: summary + t-tags
search_tertiary    kind 0: about                      30023: content
```

- **Extractors**: a `SearchTiers(primary, secondary, tertiary)` value +
  a per-kind extractor registry in `:v2:store` (it has Quartz's typed
  events), with `SearchableEvent.indexableContent() → tertiary` as the
  fallback for kinds without a custom extractor. The kind-0 extractor ports
  Brainstorm's current field weighting; each new kind is one small class +
  tests.
- **Ranking**: the `text` profile becomes
  `w_p·bm25(primary) + w_s·bm25(secondary) + w_t·bm25(tertiary)` with
  query-tunable weights, multiplied by the observer's WoT boost when the
  profile tensor lands (the `user_q` input is already plumbed from NIP-42).
  v1's gram fields / exact-match bonuses (`doc.sd`) port onto the primary
  tier when `:v2:profile` brings the ranking math over.
- **Upgrades are cheap**: extractors are derived data, so changing one (new
  Brainstorm weighting, a new kind) rolls out via `reindexFullTextSearch` —
  re-derive in place, **no resync**. This is why the tiers live on the doc
  rather than in query-time logic.

## Module plan for `:v2:sync`

| component | role |
| --- | --- |
| `RelaySyncer` + `SyncState` + `SyncProgress` | ported from v1 (`:indexer`) nearly verbatim — the negentropy-or-pages transport |
| `Roster` | derives the pubkey set from store queries; grows via the change feed |
| `OutboxDirectory` | 10002 tracking: per-author write relays, the inverted relay→authors map, staleness refresh, INDEX_RELAYS fallback |
| `ScopePlanner` | roster × directory × providers → (relay, filter) work units, author-batched |
| `LiveSubs` | the rotating subscription pool, trust-prioritized |
| `SyncService` | bootstrap ordering, the change-feed reaction loop, the reconcile ticker — v1's `SyncService` shape |

Config: `SEED_RELAYS` (10040 bootstrap), `INDEX_RELAYS` (10002/profile
aggregators + fallback), `SYNC_KINDS`, `SYNC_INTERVAL`, `LIVE_SUBS` (pool
size, 0 = reconcile-only), `MAX_AUTHORS_PER_FILTER`.

## Phasing (each lands green on its own)

0. **Bridge the existing dataset**: `sot2 import --from events.db` — open
   v1's SQLite store read-only, stream every event through
   `VespaEventStore.batchInsert`. The v1 indexer's years of data becomes the
   v2 bootstrap in one offline pass; no network, ~30 lines, and it exercises
   the store at real scale for the first time.
1. **Scores plane**: port RelaySyncer/SyncState; 10040 → provider → 30382
   pipeline writing to the Vespa store (v1 phases 1–3 minus Discovery).
   This alone reaches feature parity with `sot index` for trust data.
2. **Search tiers + extractor registry** (requirement 5) — independent of
   sync; can even land first. Includes the kind-0 Brainstorm port and the
   30023 extractor as the second proof.
3. **Roster + OutboxDirectory + ScopePlanner** (requirements 1, 4): the
   records plane, reconcile-cadence only.
4. **LiveSubs** (the "constant" in constant sync).
5. **Seeded negentropy** once the Quartz API lands: haveIds deletion diffs
   everywhere, retire the enumeration hack.

## Open questions (defaults proposed, flag disagreement)

- **Roster eviction**: when every score for a subject is retracted, stop
  syncing them (directory drops the author) but keep their stored events
  until an optional GC sweep. Default: keep, GC later.
- **Kind 1**: excluded from `SYNC_KINDS` by default. Text-note search is a
  different product (volume, spam) — the schema supports it if an operator
  opts in.
- **Third-party 10002 hints**: when a scored subject has no 10002, we could
  mine relay hints from the 30382's tags or p-tag hints. Default: INDEX_RELAYS
  fallback only, hints later.
- **Publishes vs sync**: the relay already accepts direct EVENT publishes
  (VerifyPolicy-gated). Accepting a kind outside `SYNC_KINDS` grows the index
  outside the plan — default: add a `KindAllowDenyPolicy` mirroring
  SYNC_KINDS + semantics kinds, so the ingest surface equals the sync surface.
