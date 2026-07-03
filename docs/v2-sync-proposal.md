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
observer enters (config │ NIP-42 auth │ seed-relay 10040 hints)
        │
index relays ──their 10002──▶ store ──change──▶ observer's OUTBOX known
observer's outbox ──10040 (authoritative) + 10002 refresh──▶ store ──change──▶ provider known
provider relay ──30382──▶ store ──change──▶ subject joins the ROSTER
index relays ──subject's 10002──▶ store ──change──▶ subject's outbox known
subject's outbox ──SYNC_KINDS + 5 + 62 + 10002──▶ store
```

**The universal rule: 10002 resolution precedes every per-author sync —
observers included.** The freshest 10040 lives on the observer's own outbox
relay, so 10040 can never come first; seed relays' 10040s are *discovery
hints* (who the observers are), never the authority. Authority needs no
special code: whatever source a 10040 arrives from, replaceable supersession
keeps the newest — the outbox pass just makes sure the newest is actually
seen. 10002 itself escapes the circularity by design: NIP-65 tells clients to
broadcast relay lists widely, which is exactly what the index relays
(purplepag.es-style) exist to hold.

Observers enter three ways: the operator's config (`DEFAULT_OBSERVER` +
extras), seed-relay 10040 hints, and — the product loop — **NIP-42
authentication on our own relay enrolls the authenticated pubkey as an
observer**, so the first person to search through their own web of trust is
also the trigger that starts syncing it.

A new score's subject automatically gets their 10002 looked up; a new 10002
reassigns that author to the right relay work-sets; a fresher 10040
supersedes in the store and re-plans the provider scope. Bootstrap is running
the loop from empty in that dependency order; steady state is the same loop
driven by change events plus a slow reconcile cadence.

### Requirement 4 — the 10002 directory is a Vespa query, not a database

Kind 10002 is synced *into the store* like any other kind, and the store IS
the directory: replaceable supersession keeps exactly the newest per author,
and kind-5 / vanish / expiry disruptions apply automatically because a 10002
is just an event — every NIP rule is enforced once, in the store, instead of
mirrored into a side structure that can drift.

The one thing today's schema can't answer directly is "who writes to relay
X": the queryable `tag_index` keeps only `"r:<url>"` and drops the r-tag's
third element, so read-only relays are indistinguishable from write relays.
Fix it the same way as `owner`/`expires_at`/`search_text` — one **derived
attribute**, computed at feed time by the store (which has Quartz to parse
NIP-65 markers and normalize URLs):

```
field write_relays type array<string> {   # kind-10002 docs only
    indexing: attribute | summary          # normalized write/both r-tags
    attribute: fast-search
}
```

Then the "directory" is two stateless queries:

- **relay → authors** (the sync work-sets):
  `where kind = 10002 and write_relays contains "<url>"`, paging `pubkey`s
  out in `AUTHORS_PER_FILTER`-sized batches;
- **the distinct relay list + author counts** (what to plan over): one Vespa
  **grouping** query — `… | all(group(write_relays) each(output(count())))`
  — array attributes group per element, so this is a single round trip.

`OutboxDirectory` shrinks to a query facade with zero state of its own;
rebuild-on-restart and cache-invalidation problems disappear by construction.
Staleness stays a sync concern, also answerable from the store: refresh a
roster member's 10002 when the stored doc's `created_at` is older than N
days, from the index relays + their last-known outbox.

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

**Provider switches invalidate scores.** When an observer's 10040 changes —
new provider, or the same provider on a different relay — the stored 30382s
synced for that observer are stale the moment the new list supersedes: they
pollute ranking and must go, and the new provider's set must sync from its
relay. Mechanics:

- **Trigger**: the change feed's 10040 insert re-plans immediately — the new
  provider scope is scheduled, the old one leaves the planner.
- **Cleanup is reference-counted, not per-observer**: one service key often
  serves many observers, so the old provider's 30382s are deleted only when
  NO stored 10040 still lists that service key for `30382:rank`. The check
  parses the stored 10040s (observer-cardinality — thousands, cheap; their
  provider tags aren't single-letter, so this is a parse, not a tag query),
  then `store.delete(kinds=[30382], authors=[orphaned service key])`.
- **The correctness backstop is stateless**: a periodic **orphan sweep** —
  referenced service keys (parsed from all stored 10040s) vs distinct 30382
  authors (one Vespa grouping query) — deletes anything unreferenced. The
  sweep catches every path to staleness (provider switch, observer removed,
  a 10040 deleted or vanished) even if a feed reaction was missed, so the
  in-flight reaction only buys promptness, never correctness.
- Ranking follows automatically: `:v2:profile`'s projection derives
  `quality_scores` from stored 30382s, so deleting the old provider's events
  plus syncing the new provider's set re-derives the observer's cells — no
  special invalidation code.

**Records plane** (per author): for each (outbox relay → author batch) unit
from the directory: sync `{kinds: SYNC_KINDS + [5, 62, 10002], authors: batch}`.
Kind 5 and 62 ride along because the v2 store *interprets* them (unlike the
old mirror design, deletion semantics are local again — an author's deletion
published only to their outbox still erases here). `SYNC_KINDS` is config,
defaulting to **kind 1 + kind 0** — notes and profiles are the product —
plus the curated searchable set (30023, git kinds, classifieds, …). Kind 1
dominates the corpus by orders of magnitude; see the scale section below for
what that commits us to.

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
search_primary     kind 0: name + display_name   kind 1: subject tag (NIP-14)   30023: title
search_secondary   kind 0: nip05 + lud16 domain  kind 1: t-tags                 30023: summary + t-tags
search_tertiary    kind 0: about                 kind 1: content                30023: content
```

Note-content lands in the tertiary tier deliberately: within a
`kinds:[1]` search only notes compete, so the tier is neutral; in a
mixed-kind search a profile whose *name* matches outranks a note that merely
mentions the term.

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

## Scale: what kind 1 from day one commits us to

Roster-scoped kind 1 is still tens of millions of docs growing continuously
(network-wide it's hundreds of millions; even a 100k-author roster posting
normally produces ~10⁷/year). Three specific consequences, each with a known
answer:

1. **Vespa attribute memory.** Attributes are RAM-resident by default, and
   notes are tag-heavy (a reply carries several e/p tags → fat `tag_index`
   arrays). Rough envelope: ~0.5–1 KB of attribute footprint per note →
   50–100 GB RAM at 10⁸ docs if we do nothing. The knobs, in order:
   `attribute: paged` (disk-backed with OS paging) on the fat, rarely-ranked
   attributes (`tag_index`, `owner`, `expires_at`); the summary store
   (`tags`, `content`, `sig`) is already on disk; a multi-node content
   cluster is the eventual answer and only touches `services.xml` +
   `redundancy`. Schema work, not code work.
2. **Store ingest throughput.** `insertLocked` runs 3 index round-trips per
   regular event (dup check, tombstone probe, vanish probe) under the single
   writer — fine for scores/profiles, a ceiling of low-thousands/s for a
   kind-1 backfill. The fix stays inside `VespaEventStore` with no interface
   change: a **bulk fast path** in `batchInsert` that batches the semantic
   checks per chunk (one `ids in (…)` dup query, one kind-5 `#e` query for
   the whole chunk, one vanish query per distinct owner), then feeds the
   survivors through the async feed client concurrently. Round-trips become
   ~4 per 5 000 events instead of 3 per event.
3. **Negentropy snapshots.** `snapshotIdsForNegentropy` currently pages
   through `/search/` capped at 10 k hits — useless against a
   million-note (relay, author-batch) scope. The **visit-based id walk**
   (document API visits streaming just `id` + `created_at`, exactly what v1's
   `visitDocs` did) stops being a nice-to-have and moves into phase 1.

None of this changes the architecture — the store interface, the relay, and
the sync planes are unaffected. It changes which optimizations are mandatory
versus optional.

## Module plan for `:v2:sync`

| component | role |
| --- | --- |
| `RelaySyncer` + `SyncState` + `SyncProgress` | ported from v1 (`:indexer`) nearly verbatim — the negentropy-or-pages transport |
| `Roster` | derives the pubkey set from store queries; grows via the change feed |
| `OutboxDirectory` | stateless query facade over the store's 10002 docs (via the derived `write_relays` attribute): relay→authors recall, grouping for the relay list, staleness checks, INDEX_RELAYS fallback |
| `ScopePlanner` | roster × directory × providers → (relay, filter) work units, author-batched |
| `LiveSubs` | the rotating subscription pool, trust-prioritized |
| `SyncService` | bootstrap ordering, the change-feed reaction loop, the reconcile ticker — v1's `SyncService` shape |

Config: `SEED_RELAYS` (10040 bootstrap), `INDEX_RELAYS` (10002/profile
aggregators + fallback), `SYNC_KINDS`, `SYNC_INTERVAL`, `LIVE_SUBS` (pool
size, 0 = reconcile-only), `MAX_AUTHORS_PER_FILTER`.

## Phasing (each lands green on its own; no v1 data migration — v2 starts fresh)

1. **Scores plane**: port RelaySyncer/SyncState; observer → 10002 →
   outbox → authoritative 10040 → provider → 30382, writing to the Vespa
   store. Reaches feature parity with `sot index` for trust data — with
   better 10040 freshness than v1, which never read the observer's outbox.
   Includes the kind-1 scale prerequisites: the visit-based negentropy
   snapshot and the store's bulk-ingest fast path.
2. **Search tiers + extractor registry** (requirement 5) — independent of
   sync; can even land first. Includes the kind-0 Brainstorm port and the
   30023 extractor as the second proof. The `write_relays` derived attribute
   lands here too (same extractor mechanism, same reindex-not-resync story).
3. **Roster + OutboxDirectory + ScopePlanner** (requirements 1, 4): the
   records plane, reconcile-cadence only.
4. **LiveSubs** (the "constant" in constant sync).
5. **Seeded negentropy** once the Quartz API lands: haveIds deletion diffs
   everywhere, retire the enumeration hack.

## Open questions (defaults proposed, flag disagreement)

- **Roster eviction**: when every score for a subject is retracted, stop
  syncing them (directory drops the author) but keep their stored events
  until an optional GC sweep. Default: keep, GC later.
- **Third-party 10002 hints**: when a scored subject has no 10002, we could
  mine relay hints from the 30382's tags or p-tag hints. Default: INDEX_RELAYS
  fallback only, hints later.
- **Publishes vs sync**: the relay already accepts direct EVENT publishes
  (VerifyPolicy-gated). Accepting a kind outside `SYNC_KINDS` grows the index
  outside the plan — default: add a `KindAllowDenyPolicy` mirroring
  SYNC_KINDS + semantics kinds, so the ingest surface equals the sync surface.
