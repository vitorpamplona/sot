# SoT v2 — Vespa-native rewrite

A ground-up rewrite of SoT that grew out of
[`docs/negentropy-first-architecture.md`](../docs/negentropy-first-architecture.md)
and then pivoted (decision 1 below): full NIP-50 means storing every event's
fields anyway, so instead of mirroring remote relays to *avoid* a local store,
**Vespa itself is the event store** — one copy of the data, implementing
Quartz's `IEventStore`, with NIP-50 (full filter support) as the primary
search interface. SQLite disappears; the store/index drift class of bugs
disappears with it.

The v1 modules at the repo root stay untouched as the working reference
implementation until v2 replaces them. v2 shares the repo's Gradle build,
version catalog, and formatting rules; its modules live under `:v2:*` and its
packages under `com.vitorpamplona.sot.v2`.

```bash
./gradlew :v2:vespa:test :v2:store:test      # from the repo root
```

## Decisions

1. **Vespa is the event store, not a mirror of one.** (Revised: the original
   mirror invariant — "in Vespa ⟺ on the authoritative relay" — assumed we
   were avoiding local storage, but complete NIP-50 requires storing
   everything regardless.) `:v2:store`'s `VespaEventStore` implements Quartz's
   `IEventStore` and enforces Nostr semantics itself, mirroring the SQLite
   modules: replaceable/addressable supersession (lowest-id tie-break),
   kind-5 deletion + tombstones, kind-62 vanish, ephemeral/expired rejection,
   NIP-40 sweeps. Because semantics are local, syncing from **many relays
   works again** (supersession is commutative — no per-scope ownership
   needed), the store never trusts source-relay behavior, and it can accept
   direct publishes. What survives of negentropy-first: NIP-77 as the sync
   *transport* (delta in both directions) and as anti-entropy —
   `IEventStore.snapshotIdsForNegentropy` exists for exactly this, and the
   store serves it straight off the doc attributes.
2. **Events are stored as fields, not blobs — and reconstructed on serve.**
   One `event` document per live event (docid = event id) holding `id`,
   `pubkey`, `created_at`, `kind`, `tags`, `content`, `sig`. The signature is
   over the canonical serialization of exact field values, so a complete,
   client-verifiable event is rebuilt from the document — no duplicate raw
   JSON. The part that makes this safe is **losslessness of `tags`**: the
   document keeps the exact tag array (`tags`, summary-only JSON), while the
   queryable form (`tag_index`, `"<letter>:<value>"` pairs) is a derived,
   lossy view used only for recall.
3. **Only single-letter tags are queryable** (`tag_index` holds only them) —
   exactly the space NIP-01 filters can address (`#a`–`#Z`). Everything else
   still round-trips through `tags`. The one other derived attribute is
   `expires_at` (NIP-40), so the expiry sweep is a single range query.
4. **No per-source `scope` field.** (Revised twice: first from
   "reconciliation scopes own deletion" to "provenance only", then removed —
   the store was the only write path and always left it empty, making it dead
   weight.) If the sync module ever needs per-source provenance for
   silent-removal diffs, it can add a field it can actually populate, or
   track source→ids outside the docs.
5. **`:v2:vespa` stays Nostr-library-agnostic.** Plain values in (`EventDoc`,
   `EventQuery`), YQL/JSON out, plus the `EventIndex` port the store talks to.
   Quartz enters in `:v2:store` and above. Events are verified once at ingest
   (syncer/relay) — the store, like the SQLite one, never re-verifies.
6. **Filter semantics** (`EventQuery` → YQL): ids/authors must be 64-hex
   (invalid entries are dropped; a constraint left with no valid values can
   match nothing, so the whole query is answered with an empty result). No
   `search` term → pure attribute recall ordered by `created_at desc`, like a
   plain relay REQ. With `search` → the filter becomes the recall constraint
   and ranking takes over.
7. **Full SQLite-store feature parity, from the sources.** Every rule in
   Quartz's `sqlite/*Module.kt` set is reimplemented and tested one-to-one:
   supersession (lowest-id tie-break), owner-keyed deletion + tombstones and
   vanish (owner = **gift-wrap recipient** for kind 1059, else the author —
   SQLite's `pubkey_owner_hash`, as an `owner` attribute), vanish deleting
   strictly-older history only, ephemeral kinds **accepted without storing**
   (NIP-01 — not rejected), expired-insert rejection + NIP-40 sweeps, and
   NIP-50 search over `SearchableEvent.indexableContent()` only (the
   `search_text` field = SQLite's FTS table; raw `content` is never searched),
   with a working one-shot + resumable `reindexFullTextSearch`.
   **Where the spec and Quartz disagree, the spec wins** (audited against
   nostr-protocol/nips): NIP-62 vanish erases up to and INCLUDING the
   request's `created_at` (Quartz: strict `<`); NIP-40 expired events are
   never served, even stored-but-unswept (Quartz serves them until a sweep);
   kind 5 has no effect against a kind 5 or a kind 62 (Quartz deletes them);
   and NIP-50 `key:value` extension tokens are stripped from the search term
   instead of matched as text (Quartz feeds them to FTS). One deliberate
   extension kept from Quartz beyond NIP-09's letter: gift-wraps are
   deletable by their p-tag recipient (NIP-62 explicitly wants this for
   vanish; applying it to kind 5 is the consistent reading).
8. **Single-writer, read-your-writes.** All store writes serialize behind one
   mutex, and the `EventIndex` contract requires an acked put to be visible
   to search (Vespa's proton updates the memory index on the write path).
   That pair is what makes query-then-write semantics sound without SQL
   transactions. There are no cross-document transactions; relay semantics
   never needed them.

## Module plan

| module | status | contents |
| --- | --- | --- |
| `:v2:vespa` | **started** | `app/` (the `event` schema + query profile), `EventDoc` (field mapping + complete-event reconstruction), `EventQuery` → YQL, the `EventIndex` port, and `VespaEventIndex` — the real client (h2c feed writes, document-API gets, `/search/` queries). testFixtures: the in-memory reference + `MockVespaEngine`, which parses the emitted YQL back into an `EventQuery` and must agree with the reference. Next: a visit-based full-corpus walk; validate the YQL against a real deployed Vespa. |
| `:v2:store` | **started** | `VespaEventStore : IEventStore` — the SQLite store's semantics on Vespa, `Filter` → `EventQuery` mapping, negentropy snapshots. The whole semantics suite runs twice: in-memory AND over the wire through `VespaEventIndex` + `MockVespaEngine`. Next: `ObservableEventStore` wiring; integration test: Quartz event → doc → reconstructed JSON → Quartz parse → `verify()`. |
| `:v2:sync` | planned | Multi-relay ingest through the store (verify → `batchInsert`), NIP-77 delta sync seeded from `snapshotIdsForNegentropy`, optional per-source reconcile diffs for silent removals. |
| `:v2:relay` | planned | NIP-50 relay serving complete events reconstructed from Vespa; full filter REQs; NIP-42 picks the observer. |
| `:v2:profile` | planned | The ranked `profile` document type (pubkey-keyed, `quality_scores` tensor — port of v1's `doc.sd` ranking math) and the kind-0/30382/10040 mapping rules. |
| `:v2:cli` | planned | Composition root: `serve` / `sync` / `status`. |
