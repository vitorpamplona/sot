# Negentropy-first architecture: Vespa as a mirror of authoritative relays

> Status: **design exploration** — not implemented. This evaluates replacing the
> local EventStore with direct relay→Vespa mirroring, and making NIP-50 the
> primary (eventually only) search interface.

## The idea in one paragraph

Today SoT keeps a local SQLite EventStore as the source of truth, re-implements
Nostr state semantics on it (replaceables, addressables, kind-5 deletions,
kind-62 vanish, silent-removal diffs), and projects its change feed into Vespa.
The proposal inverts this: **designated remote relays are the source of truth,
and Vespa is a mirror of their *current* event sets**, kept in sync by NIP-77
negentropy. The invariant per synced scope becomes:

> *An event id is in Vespa ⟺ it is currently on the authoritative relay for
> its scope.*

Every deletion style — kind 5, addressable supersession, kind 62, silent row
wipes — collapses into one observable fact: *the id is no longer in the relay's
set*. We stop interpreting deletion events entirely; the source relay already
did. The local store, its projection's deletion machinery, and the anti-entropy
`verify --repair` loop all disappear, because negentropy against the true
source **is** the anti-entropy.

## Why this is possible: seeded negentropy reports deletions natively

The crucial mechanical detail. NIP-77 reconciles two sets of
`(created_at, id)` pairs and yields two outputs:

- `needIds` — the relay has them, we don't → **fetch and index**.
- `haveIds` — we have them, the relay doesn't → **delete from Vespa**.

Today's `RelaySyncer` calls Quartz's `negentropySyncOrFetch` with an **empty**
local set: incrementality comes from persisted `since` cursors, and deletions
require the separate `reconcile()` full-enumeration + diff-against-the-store
hack. If instead we seed the sync with the `(id, created_at)` pairs Vespa
already holds for the scope, both directions come out of one protocol run, and
only the delta transfers — in either direction. That is the entire sync
algorithm:

```
local = vespa.visit(scope)                    # (event_id, created_at) pairs
need, have = negentropy(relay, filter, local)
for id in need: fetch event → verify sig → index into Vespa
for id in have: read doomed doc → undo its contribution → delete doc
```

No cursors to advance carefully, no "a timeout must never be read as
deletion" subtlety in our code (an aborted reconciliation simply applies
nothing), no four styles of deletion handling.

### What this requires from Quartz

`NegentropyManager.startSync` takes `localEvents: List<Event>` — full events —
and the high-level `negentropySyncOrFetch` doesn't expose seeding at all. Two
small upstream changes:

1. Accept `(id, created_at)` pairs (that's all the protocol hashes) instead of
   full `Event`s, so a caller without an event store can seed.
2. Surface `haveIds` from the high-level accessory (it's already produced by
   the session; it's just not returned today).

### What this requires from the source relays

The authoritative relay for a scope **must support NIP-77** (strfry does, with
`maxSyncEvents` windowing that Quartz already handles). That's a fair contract
to impose on a relay you're calling "source of truth". For a score provider
whose relay hint can't reconcile, we degrade per-scope to what we do today:
`since`-cursor pages for inserts + periodic full page enumeration for the
deletion diff. The architecture tolerates pages-only scopes; it just loses the
elegance (and cheap deletions) for those scopes only.

## Scopes: one authoritative relay per set (and how "more than one" works)

The mirror invariant is only coherent against **one** authoritative set per
scope. The scopes are exactly the sync units we already have:

| scope | filter | authoritative relay |
| --- | --- | --- |
| trust-provider lists | kind 10040 | configured `SEED_RELAYS` (each one its own scope) |
| profiles | kind 0 | configured profile source relay(s) — see below |
| scores of provider P | kind 30382, author = P's service keys | the relay hint in the 10040 (grouped per relay, as today) |

**Can we use more than one relay? Yes — as disjoint scopes, not as a union.**
Each Vespa doc carries a `scope` attribute naming the (relay, filter) pair that
owns it; a reconciliation pass only deletes within its own scope. Two relays =
two scopes = two independent mirrors that happen to land in one index.

What does *not* work is a union treated as one set: an id absent from relay A
but present on relay B is not deleted, so "not on the relay ⇒ delete" becomes
"absent from **all** relays in the union ⇒ delete", which means every source
must complete a full reconciliation in the same pass before any deletion is
safe — one timeout poisons the whole diff. If redundancy is ever needed,
model it as replicas with delete-only-on-absent-from-all, and accept that
deletions stall while any replica is unreachable. For the prototype:
**one primary relay per scope**, period.

The same-event-in-two-scopes case (a profile on both the profile source and a
provider relay) is harmless: the doc is owned by whichever scope indexed it;
if that scope drops it while another still serves it, the other scope's next
pass re-indexes it. Eventual consistency, converging on "any source has it".

### Profiles are the awkward scope — and the biggest simplification lever

Today profiles come from a ~200-relay NIP-65 outbox crawl, precisely because no
single relay has everyone. A mirror can't union those. Options:

1. **One profile aggregator relay** (purplepag.es-style) as the profile source.
   Accept its coverage as the product's coverage. Simplest; recommended start.
2. A small fixed list of profile relays as independent scopes (last-write-wins
   across scopes by `created_at` for the *parsed profile*, while each scope's
   event mirror stays exact).
3. Drop standalone profile syncing: derive the interesting pubkey set from the
   score subjects + observers, and fetch/refresh only those kind-0s.

The stated product goal — *"let people offer WoT search engines via Nostr
interfaces"* — argues for (1) with (2) as a config option: an operator points
`PROFILE_SOURCE_RELAYS` at one or a few relays they trust to hold profiles, and
`SEED_RELAYS` at where 10040s live. The discovery crawl, `--max-rounds`,
`--max-relays`, and the persisted relay pool are deleted.

## The Vespa document model

Two document types replace the single `doc` schema:

### `event` — the mirror (and the NIP-50 recall surface)

One document per **currently-live** Nostr event, `docid = event.id`:

| field | type | why |
| --- | --- | --- |
| `kind` | int attribute (fast-search) | `kinds` filter |
| `pubkey` | string attribute (fast-search) | `authors` filter |
| `created_at` | long attribute (fast-search) | `since`/`until`, negentropy seeding, recency ordering |
| `tags` | array\<string\> attribute, values encoded `"t:value"` | `#x` tag filters: `tags contains "p:<hex>"` |
| `content` | string index (bm25) | generic `search` recall for non-profile kinds |
| `raw` | string, summary-only | the **original signed event JSON**, served back on REQ |
| `scope` | string attribute (fast-search) | which (relay, filter) mirror owns this doc |

`raw` is what lets the relay module answer REQs without an EventStore: events
are signature-verified once at index time (verification stays **on** — the
source relay is authoritative for *set membership*, never for *authenticity*),
then served verbatim from the summary.

Note what we no longer sync at all: **kind 5**. A mirror doesn't interpret
deletion events; it observes their effect. Phase 3's per-provider kind-5 pull
and the projection's e-tag/a-tag resolution logic are deleted, along with the
`event_id` / `score_event_ids` provenance fields — the event doc *is* the
provenance.

### `profile` — the ranked aggregate

One document per pubkey (`docid = pubkey`), essentially today's schema: parsed
kind-0 fields, the gram fields, and the `quality_scores` sparse tensor keyed by
observer — plus `source_event_id`/`source_created_at` so a mirror deletion of a
kind-0 knows whether it's blanking the *current* profile or a stale no-op.

Why a second type instead of hanging the tensor on the kind-0 event doc:
event docs are keyed by event id, so a profile update arrives as a *new* doc
(and the old one dies in the next diff) — the tensor would be lost on every
kind-0 replace and need re-derivation. The pubkey-keyed profile doc absorbs
kind-0 churn as partial updates and keeps the ranking schema stable.

### The residual projection (small, and provenance-free)

A thin mapper remains between mirror operations and the `profile` type — but it
only handles *positive* state plus doc-granular deletion, never deletion
*semantics*:

- **index kind 0** → upsert `profile(pubkey)` if newer than `source_created_at`.
- **index 10040** → learn `service key → observer` (resolvable any time by
  querying `event` docs with `kind = 10040`, replacing the in-memory-map +
  store-fallback dance).
- **index 30382** → resolve observer, set `quality_scores{observer}` on the
  subject's profile (absent `rank` tag = remove the cell, as today).
- **delete event id** → *read the doomed `event` doc first*, undo what it
  contributed (kind 0 → blank profile if it's the current source; 30382 →
  remove that observer's tensor cell), then delete the doc.

That last rule replaces `handleDeletion`, `handleVanish`, `eraseAuthor`,
`sweepScores`, `deleteByEventId`, `findProfileByEventId`, `findScoreByEventId`,
and the paged million-cell sweep — the doomed doc tells us exactly what to
undo. Order of operations (read → undo → delete) is the one invariant to test.

### Seeding the local set from Vespa

Negentropy needs the scope's `(event_id, created_at)` pairs each pass. Source:
a `/document/v1` **visit** with `selection = scope`, returning only the two
attributes. Millions of ids per scope is tens of MB streamed — acceptable per
pass, and the serve loop can keep the set in memory between passes as a pure
cache (rebuildable, never authoritative). No sidecar state file: `SyncState`'s
cursors, negentropy-capability memory (still useful), and relay pool shrink to
a fraction of today's file — or move into a tiny Vespa doc.

## NIP-50 as the primary interface: full Filter → YQL

This is a MUST regardless of which architecture wins, and the event schema
above is what makes it possible. The mapping is mechanical:

| Filter field | YQL |
| --- | --- |
| `ids` | `id in (…)` (or direct docid gets) |
| `kinds` | `kind in (…)` |
| `authors` | `pubkey in (…)` |
| `#x: [v…]` | `(tags contains "x:v1" or tags contains "x:v2" …)` — AND across different tag letters |
| `since` / `until` | `created_at >= s and created_at <= u` (range attribute) |
| `limit` | `hits` |
| `search` | the text recall clauses, AND'ed with all of the above |

Two query modes fall out:

- **No `search` term**: pure attribute filtering, ordered by `created_at`
  desc — ordinary relay REQ semantics. (Today these filters are silently
  ignored; supporting them makes SoT a real read-relay over its mirror, nearly
  for free.)
- **With `search`**: the filter clauses become the recall *constraint* and
  ranking does the rest. For kind-0 searches, ranking is today's
  observer-weighted profile math (query the `profile` type, constrained by the
  filter's authors if present, then fetch `raw` kind-0s). For other kinds,
  first phase is bm25 over `content` × recency.

**WoT-ranking arbitrary kinds by their author's trust** — the obvious next
step once 30382 covers more than profiles — has a Vespa-native shape:
make `profile` a **global (parent) document**, give `event` a
`reference<profile>` on `pubkey`, and import `quality_scores` into the event
schema. Then any event query can rank by
`sum(query(user_q) * attribute(imported_quality_scores))`. Caveat: parent docs
are replicated to every content node and held in memory; fine on the
single-node prototype, a real capacity decision later. Don't build this in v1,
but don't preclude it — it's the strongest argument for the two-type model.

The `:vespa`-stays-Nostr-agnostic rule survives: `:vespa` gains a plain
`EventQuery(kinds, authors, tags, since, until, limit, text)` value type + the
YQL builder; `:relay` maps Quartz's `Filter` into it.

## What the codebase looks like after

```
config     unchanged (keys: −EVENTS_DB, +PROFILE_SOURCE_RELAYS; SEED_RELAYS = 10040 sources)
event-store DELETED (the module, SQLite, androidx-sqlite, the single-writer constraints)
vespa      grows: event + profile schemas, EventQuery→YQL, visit API for seeding
mirror     (renamed indexer) negentropy mirror sync + the thin projection above.
           RelaySyncer keeps streaming/verify/backpressure; loses cursors-as-correctness,
           reconcile(), the kind-5 phase, Discovery entirely.
http       unchanged surface; reads Vespa only
relay      full Filter support (not just search-term-only); serves `raw` from Vespa;
           no store dependency
cli        −verify (a mirror pass IS verify), destroy = Vespa volume only,
           `sot index`/`serve` no longer fight over a SQLite file
```

Also gone as *concepts*: the projection change-feed subscription race
(`awaitSubscribed`), `awaitIdle` draining, "don't run `index` while `serve` is
syncing", GBs of local disk, and the store-vs-index drift class of bugs that
`sot verify --repair` exists to fix.

## Risks — where this is worse

1. **The source relay becomes a trusted availability *and* integrity
   dependency for set membership.** A wiped, misconfigured, or malicious
   source relay empties our index. Mitigation (required, cheap): a
   **mass-deletion guard** — refuse to apply a diff deleting more than N% of a
   scope (or >M docs) in one pass without an explicit flag; log loudly. Note
   the local store never really protected against this either (the silent-
   removal reconcile would propagate a relay wipe too) — but the blast radius
   is now everything, not just scores.
2. **No local archive.** Disaster recovery = full re-download from relays.
   Acceptable by design (the relay is the archive), but a relay that *lost*
   data takes our copy down with it. If that ever matters, the answer is a
   second replica scope, not resurrecting the store.
3. **Kind-62 fidelity becomes the source relay's policy.** If the source
   honors a vanish, the mirror follows automatically. If it doesn't, SoT would
   keep serving data its author asked to vanish. Recommendation: keep syncing
   kind 62 (it's cheap) and honor broad/self-directed vanishes at the
   projection layer — the only deletion *semantics* we keep, on ethics grounds
   rather than consistency grounds.
4. **Quartz API changes needed** (seed pairs + `haveIds`), plus the Vespa visit
   plumbing. Real work, all upstream-able.
5. **Pages-only provider relays** lose the elegant path and keep roughly
   today's machinery for their scope. If most 10040 relay hints turn out not
   to speak NIP-77, the "simplification" partially evaporates — worth
   measuring on real 10040 data before committing.
6. **Eventual-consistency windows** widen slightly (score arrives before its
   subject's profile, tensor cell updates lag replaces). Search tolerates
   this; tests must too.

## Verdict

For what this project is trying to become — a simple, operable package anyone
can point at relays to offer a WoT search engine — the trade is good. The
EventStore earns its complexity only when relays are unreliable *and*
unconstrained; the moment "source of truth" is a named, NIP-77-capable relay
chosen by the operator (and by each observer's own 10040), that machinery is
solving a problem the contract already solved. The two genuinely new costs —
Filter→YQL and Vespa-seeded negentropy — are respectively (a) mandatory anyway
and (b) small once Quartz exposes seeding.

## Suggested phasing

1. **Filter → YQL + the `event` schema** on the current architecture (feed
   event docs from the existing store change feed). Ships the NIP-50 MUST
   independently of everything else, and de-risks the schema.
2. **Quartz: seedable negentropy + `haveIds`.** Then the Vespa-visit seeding
   and the mirror loop for the *score* scopes only (they already have
   single-relay sources — the 10040 hints). Delete `reconcile()` and the
   kind-5 provider sync.
3. **Profiles from designated source relays**; delete Discovery.
4. **Delete `:event-store`**, move REQ serving to `raw` summaries, thin out
   the projection to the four rules above.

Each phase leaves `main` shippable; phase 4 is the point of no return and the
big payoff.
