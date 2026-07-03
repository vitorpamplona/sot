# SoT v2 — negentropy-first rewrite

A ground-up rewrite of SoT on the architecture in
[`docs/negentropy-first-architecture.md`](../docs/negentropy-first-architecture.md):
**no local event store** — designated remote relays are the source of truth, and
Vespa mirrors their *current* event sets via seeded NIP-77 reconciliation.
NIP-50 (with full filter support) is the primary search interface.

The v1 modules at the repo root stay untouched as the working reference
implementation until v2 replaces them. v2 shares the repo's Gradle build,
version catalog, and formatting rules; its modules live under `:v2:*` and its
packages under `com.vitorpamplona.sot.v2`.

```bash
./gradlew :v2:vespa:test      # from the repo root
```

## Decisions

1. **The mirror invariant.** Per sync scope (one authoritative relay + one
   filter): *an event id is in Vespa ⟺ it is currently on that relay.*
   Negentropy's `needIds` drive indexing, `haveIds` drive deletion. We never
   interpret deletion events (kind 5, supersession, vanish, silent wipes) —
   the source relay already did.
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
   still round-trips through `tags`.
4. **Every document carries a `scope` attribute** naming the (relay, filter)
   mirror that owns it. Reconciliation deletes only within its own scope;
   multiple source relays coexist as disjoint scopes, never as a union.
5. **`:v2:vespa` stays Nostr-library-agnostic.** Plain values in
   (`EventDoc`, `EventQuery`), YQL/JSON out. Quartz enters only in the mirror
   module. Signature verification happens there, once, at index time — the
   source relay is authoritative for set membership, never authenticity.
6. **Filter semantics** (`EventQuery` → YQL): ids/authors must be 64-hex
   (invalid entries are dropped; a constraint left with no valid values can
   match nothing, so the whole query is answered with an empty result). No
   `search` term → pure attribute recall ordered by `created_at desc`, like a
   plain relay REQ. With `search` → the filter becomes the recall constraint
   and ranking takes over.

## Module plan

| module | status | contents |
| --- | --- | --- |
| `:v2:vespa` | **started** | `app/` (the `event` schema), `EventDoc` (field mapping + complete-event reconstruction), `EventQuery` → YQL. Next: feed/search client against MockVespa. |
| `:v2:mirror` | planned | Seeded negentropy sync (needs the Quartz `(id, created_at)` seeding + `haveIds` API), the Vespa-visit local-set source, the four-rule projection into profile ranking state, the mass-deletion guard. |
| `:v2:relay` | planned | NIP-50 relay serving complete events reconstructed from Vespa; full filter REQs; NIP-42 picks the observer. Integration test: Quartz event → `EventDoc` → reconstructed JSON → Quartz parse → `verify()`. |
| `:v2:profile` | planned | The ranked `profile` document type (pubkey-keyed, `quality_scores` tensor — port of v1's `doc.sd` ranking math) and the kind-0/30382/10040 mapping rules. |
| `:v2:cli` | planned | Composition root: `serve` / `mirror` / `status`. |
