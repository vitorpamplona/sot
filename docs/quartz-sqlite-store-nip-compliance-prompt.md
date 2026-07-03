# Prompt: fix four NIP-compliance gaps in Quartz's SQLite EventStore

> Status: **open — send upstream.** Found while auditing sot's v2
> `VespaEventStore` (which reimplements the SQLite store's semantics) against
> the spec texts in `nostr-protocol/nips`. The Vespa store already ships the
> spec-correct behavior with a test per finding
> (`v2/store/src/test/.../VespaEventStoreTest.kt`); this prompt asks for the
> same fixes in Quartz. Findings verified against Quartz commit `4a66435263`.

Paste the section below to an agent working in the **Amethyst / Quartz**
repository (`github.com/vitorpamplona/amethyst`, module `quartz`).

---

## Task

The SQLite event store (`quartz/src/commonMain/kotlin/com/vitorpamplona/quartz/nip01Core/store/sqlite/`)
diverges from the normative text of NIP-09, NIP-40, NIP-50, and NIP-62 in four
places. Fix each one, keeping the existing module/trigger architecture, and add
a regression test per fix in the store's test suite. The spec quotes below are
from `nostr-protocol/nips` (master).

### 1. NIP-62 — vanish must erase up to and INCLUDING the request's `created_at`

**Spec:** relays "MUST fully delete any events from the `.pubkey`" ... "until
its `.created_at`" — an inclusive bound.

**Current code** (`RightToVanishModule.kt`, trigger
`delete_events_on_event_vanish`) deletes strictly older events only:

```sql
DELETE FROM event_headers
WHERE event_headers.created_at < NEW.created_at AND
      event_headers.pubkey_owner_hash = NEW.pubkey_hash;
```

An event published at the same second as the vanish request survives, against
the spec.

**Fix:** change `<` to `<=`, and **exclude the vanish request's own header
row** — it is inserted before the `event_vanish` row that fires this trigger,
so an unqualified `<=` would delete the request itself, and the
`ON DELETE CASCADE` from `event_vanish.event_header_row_id` would then erase
the registry row that powers the `reject_events_on_event_vanish` trigger,
silently disabling future-insert blocking:

```sql
DELETE FROM event_headers
WHERE event_headers.created_at <= NEW.created_at AND
      event_headers.pubkey_owner_hash = NEW.pubkey_hash AND
      event_headers.row_id != NEW.event_header_row_id;
```

**Test:** insert a note at `t`, a covering kind 62 at `t` → the note is gone,
the request row and its `event_vanish` registry entry survive, and a later
insert by the pubkey at `t` is still rejected with
`blocked: a request to vanish event exists`.

### 2. NIP-40 — queries must not return expired-but-unswept events

**Spec:** "Relays SHOULD NOT send expired events to clients, even if they are
stored."

**Current code:** expiry is enforced only at insert
(`blocked: Cannot insert an expired event`) and by the on-demand
`deleteExpiredEvents()` sweep. `QueryBuilder.kt` contains no expiration
condition at all, so between sweeps every query/count serves expired events.

**Fix:** add an anti-join against `event_expirations` to the WHERE clause the
`QueryBuilder` emits for `query`/`count` (NOT the negentropy snapshot — the
stored set is the honest reconciliation set, and re-inserts are rejected as
expired anyway):

```sql
AND NOT EXISTS (
    SELECT 1 FROM event_expirations x
    WHERE x.event_header_row_id = event_headers.row_id
      AND x.expiration <= unixepoch()
)
```

`event_expirations` only holds rows for events that carry the tag, so the
probe is cheap for the overwhelmingly common no-expiration case.

**Test:** insert an event expiring in the future, advance the clock past it
(or insert with a near-now expiration and wait), query WITHOUT calling
`deleteExpiredEvents()` → not returned, yet still present in a raw row count.

### 3. NIP-09 + NIP-62 — kind 5 must have no effect against kind 5 or kind 62

**Spec:** NIP-09: "Publishing a deletion request event against a deletion
request has no effect." NIP-62: "Publishing a deletion request event (Kind 5)
against a request to vanish has no effect."

**Current code:** `DeletionRequestModule.deleteSQL`'s by-id statement deletes
any kind:

```sql
DELETE FROM event_headers
WHERE id IN (...) AND pubkey_owner_hash = ?
```

so a user's kind 5 can erase their own earlier kind 5 (resurrecting deleted
events for late-arriving replays, since the tombstone that powers
`reject_deleted_events` is gone) or erase a kind 62 (weakening a vanish).
Symmetrically, the `reject_deleted_events` trigger can block a kind 5 / kind
62 INSERT if some kind 5 e-tags it.

**Fix:** two guards:

- `deleteSQL` by-id: append `AND kind NOT IN (5, 62)` (a-tag statements are
  unaffected — 5 and 62 are neither replaceable nor addressable).
- `reject_deleted_events` trigger: add `WHEN NEW.kind NOT IN (5, 62)` so
  tombstone kinds are immune to tombstones.

**Test:** kind 5 A deletes a note; kind 5 B e-tags A → A's row survives and a
replay of the note is still rejected. A kind 5 e-tagging a stored kind 62 →
the 62 survives.

### 4. NIP-50 — strip unsupported `key:value` extensions before FTS matching

**Spec:** "A query string may contain `key:value` pairs (two words separated
by colon), these are extensions, relays SHOULD ignore extensions they don't
support."

**Current code:** `QueryBuilder` passes the raw search string to
`MATCH` (`match(fts.tableName, search)` in the FTS path). A query like
`vitor language:en` makes FTS look for the literal tokens `language`/`en`
(shrinking or emptying the result set), and the colon is FTS5 query syntax —
some extension strings make `MATCH` throw instead of matching.

**Fix:** before building the MATCH expression, drop every
whitespace-delimited token matching `^\w+:\S+$` (no extensions are supported
yet). If nothing remains, the search term imposes **no text constraint** —
the filter's other fields still apply — rather than matching nothing.

**Test:** an indexed profile named `vitor` matches the search
`vitor language:en`; the search `language:en` alone with `kinds:[0]` returns
all profiles.

## Non-findings (checked, already correct)

Replaceable/addressable supersession incl. the lowest-id tie-break (NIP-01),
`since <= created_at <= until` inclusivity, ephemeral accept-without-store,
a-tag deletes bounded by the deletion's `created_at` (NIP-09), keeping kind 5s
indefinitely, gift-wrap ownership via `pubkey_owner_hash` (required by NIP-62's
"delete all NIP-59 Gift Wraps that p-tagged the `.pubkey`"), and NIP-50
relevance-ordering being left to the caller. Note the tombstone
reject-on-reinsert behavior itself is an extension beyond NIP-09's letter —
worth keeping, both stores rely on it.
