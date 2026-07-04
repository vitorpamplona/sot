# SoT — Search over Trust

A Nostr search relay ranked by the searching user's **web of trust**: events
sync from the network into [Vespa](https://vespa.ai), and NIP-50 searches rank
each hit by the trust score the *observer* (the NIP-42-authenticated user, or
the operator's house account) assigned to its author via NIP-85.

The design grew out of
[`docs/negentropy-first-architecture.md`](docs/negentropy-first-architecture.md)
and then pivoted (decision 1 below): full NIP-50 means storing every event's
fields anyway, so instead of mirroring remote relays to *avoid* a local store,
**Vespa itself is the event store** — one copy of the data, implementing
Quartz's `IEventStore`, with NIP-50 (full filter support) as the primary
search interface. There is no SQLite and no http search API: the relay is the
API, and even the bundled web UI is just another Nostr client speaking NIP-50
to it.

The stack is **validated against a real Vespa deployment** (2026-07): the
schemas deploy clean and a full protocol acceptance run passed against the
live engine — feed, recall, reconstruction, the trust gate, NIP-42-ranked
search through the imported tensors, the NIP-50 extensions, and deletion.

```bash
./gradlew build                 # everything: compile + tests + formatting
./gradlew :cli:installDist      # the `sot` binary
cli/build/install/sot/bin/sot init     # interactive setup (--yes = defaults)
cli/build/install/sot/bin/sot up       # start local Vespa + deploy vespa/app
cli/build/install/sot/bin/sot serve    # relay + web UI + background sync
./gradlew :cli:uiDemo           # web-UI dev: in-memory relay, no Vespa
```

## Decisions

1. **Vespa is the event store, not a mirror of one.** (Revised: the original
   mirror invariant — "in Vespa ⟺ on the authoritative relay" — assumed we
   were avoiding local storage, but complete NIP-50 requires storing
   everything regardless.) `:store`'s `VespaEventStore` implements Quartz's
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
5. **`:vespa` stays Nostr-library-agnostic.** Plain values in (`EventDoc`,
   `EventQuery`), YQL/JSON out, plus the `EventIndex` port the store talks to.
   Quartz enters in `:store` and above. Events are verified once at ingest
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
   and NIP-50 `key:value` extension tokens are split off the search term
   instead of matched as text (Quartz feeds them to FTS) — with `sort:`,
   `filter:rank:…`, and `include:spam` HONORED (see decision 8); unknown
   extensions are ignored per the NIP. One deliberate
   extension kept from Quartz beyond NIP-09's letter: gift-wraps are
   deletable by their p-tag recipient (NIP-62 explicitly wants this for
   vanish; applying it to kind 5 is the consistent reading).
8. **The NIP-42 observer rides the coroutine context.** Quartz's
   `IEventStore` has no per-caller parameter, so the relay backend resolves
   the ranking observer (first authenticated pubkey, else the operator's
   default) and wraps each REQ/COUNT in `withContext(ObserverContext(...))`;
   the store reads it back and stamps `EventQuery.observer` — ranking
   context only, never recall. `EventYql` emits it as the `user_q` query
   feature, which the imported profile tensors resolve into per-author trust.
   The query side is Brainstorm's, ported from `vespa_query.py`: per-word OR
   groups (exact / prefix / length-gated fuzzy per field, trigram safety
   nets, joined + adjacent-pair variants, words out-of-band as `@w0..@w5`),
   default profile `search`. NIP-50 extensions map onto Brainstorm's API:
   `sort:rank[:asc]`/`sort:followers`/`sort:text` pick the rank profile (with
   no terms: a trust-ordered match-all), `filter:rank:gte:N`/`gt:N` set the
   `min_rank` floor, and every search is trust-gated at `DEFAULT_MIN_RANK`
   by default — Brainstorm's `onlyRanked`, whose NIP-50 inverse is
   `include:spam` (presence switches the default floor off; an explicit
   `filter:rank` floor always survives). Plain no-search REQs are never
   gated: that recall is NIP-01's, not search's.
9. **Single-writer, read-your-writes.** All store writes serialize behind one
   mutex, and the `EventIndex` contract requires an acked put to be visible
   to search (Vespa's proton updates the memory index on the write path).
   That pair is what makes query-then-write semantics sound without SQL
   transactions. There are no cross-document transactions; relay semantics
   never needed them.

## Modules

The whole stack is built and green; the store/relay/query path is validated
against a real Vespa (2026-07). Genuinely-deferred scale work is listed under
the table.

| module | contents |
| --- | --- |
| `:vespa` | `app/` (the `event` schema + query profile), `EventDoc` (field mapping + complete-event reconstruction), `EventQuery` → YQL (`EventYql` assembles the filter; `BrainstormWordGroup` builds the per-word fuzzy recall), the `EventIndex` port, and `VespaEventIndex` — the real client (h2c feed writes, document-API gets, `/search/` queries, and `visitIds` — the visit-based full-corpus walk behind negentropy snapshots). testFixtures: the in-memory reference + `MockVespaEngine`, which parses the emitted YQL back into an `EventQuery` and must agree with the reference. (YQL + schemas VALIDATED against a real deployed Vespa, 2026-07.) |
| `:store` | `VespaEventStore : IEventStore` — full Nostr semantics on Vespa (the same rules Quartz's SQLite modules enforce), `Filter` → `EventQuery` mapping, negentropy snapshots, and the `BulkInsert` fast path behind `batchInsert` (the same rules with batched I/O — the sync-scale ingest path). The whole semantics suite runs twice: in-memory AND over the wire through `VespaEventIndex` + `MockVespaEngine`; `RelayProtocolTest` covers the Quartz event → doc → reconstructed JSON → `verify()` round trip. |
| `:sync` | The SCORES plane of `docs/v2-sync-proposal.md`. `RelaySyncer` (the download orchestrator: negentropy-or-pages transport, per-scope cursors, capability memory) delegates to `EventStreamPipeline` (bounded-channel streamed verify → `batchInsert`), `NostrAuthHandshake` (NIP-42 first contact), and `CursorScope`. `Identity` — the relay's own key (`SERVER_NSEC`): NIP-42 client auth upstream, and first-run self-publish of its kind 0 / 10002 (this relay's URL) / **10086** into its OWN store — the stored 10086 IS the indexer configuration (operator supersedes it from any Nostr client; `indexRelays()` reads it back). `TrustSync` + `BlendedPass` run the author-first chain as a BLENDED work-unit pipeline over relays (not strict phase barriers), in the one order that keeps 10040s authoritative: seed-relay 10040 *hints* → observer 10002s from the index relays (+ the **house account**'s home relay) → observer **outboxes** (0/10002/10040/5/62 — the authoritative 10040 lives there) → per-provider 30382+5 with the silent-removal reconcile diff → the **orphan sweep** (30382s whose service key no stored 10040 references anymore are deleted — provider switches need no invalidation code; the `:profile` projection re-derives ranking from what remains). `SyncService` composes client + authenticator + state, `runOnce`/`runForever`, and `enroll()` — NIP-42 logins on our relay become observers on the next pass. Tests run the whole chain over `InProcessNet`: real Quartz relays (REQ + NIP-77), each backed by `VespaEventStore` over the in-memory index, signed events, verification on. |
| `:relay` | `SotRelayServer` — Quartz's protocol engine (`RelayServerBase` + `LiveEventStore`) over the Vespa store: full-filter REQs + live subscriptions, VerifyPolicy-gated EVENT publishes, NIP-45 COUNT, server-side NIP-77, NIP-11 doc, Ktor mount. NIP-42 auth switches the ranking observer per connection (`ObserverRoutingBackend`) and fires `onObserver` for sync enrollment. `:cli` is the composition root that wires it. |
| `:profile` | `app/schemas/profile.sd`: the GLOBAL parent doc (pubkey-keyed `quality_scores` + `follower_counts` tensors, Brainstorm shapes) that every event references (`author_ref`) and imports for ranking. `event.sd` carries the per-kind search fields (Brainstorm profile group for kind 0; generic tiers otherwise) and ONE kind-dispatched `search` rank profile plus Brainstorm's sort alternates (`sort_followers`, `rank_desc/asc`, `rank_filtered`, `text_relevance`) — a single query ranks every recalled kind by its own equations × the observer's trust. `TrustProjection` decorates the store's `EventIndex` and recomputes each subject's `ProfileDoc` (both tensors, observer-keyed per NIP-85) whenever a 30382/10040 doc is put or removed — so supersession, kind-5, vanish, and sweeps all update ranking with zero deletion-specific code; `rebuildAll()` bootstraps from an existing corpus, `author_ref` is stamped on every event doc at feed time. `:store`'s `SearchExtractors` decomposes every Quartz `SearchableEvent` (~80 kinds) into the per-kind fields — kind 0 into the Brainstorm profile group, titled kinds into primary/secondary/tertiary tiers, everything else via the `indexableContent()` fallback into the tertiary tier — applied at insert and re-derivable via `reindexFullTextSearch`. `EventYql` emits the NIP-50 `sort:`/`filter:rank:`/`include:spam` extensions that select profile and trust floor (decision 8). Equations are VERBATIM from the operator-provided brainstorm-k8s doc.sd (2026-07, the §12 multiplicative-trust rewrite): concave `wot_mult` multiply (not the old additive sigmoid), `text_score_cutoff` pre-multiply discard, sentinel + drop-limit gating, proximity rule, and the known-inert `match_quality` itemRawScore ladder kept for parity. |
| `web/` | `index.html` — the search UI, and itself a Nostr CLIENT: no http API, it opens a WebSocket to the server that serves it and speaks NIP-50 REQs directly. Kind chips are literal `kinds` filters (Everything / People / Notes / Articles / Media / Code & git / Live), the sort menu and "unranked too" checkbox append the NIP-50 `sort:`/`include:spam` extensions, and "Search as me" is NIP-07 → NIP-42: the extension signs the relay's challenge, the connection's ranking observer becomes YOU, and the relay enrolls you for trust sync. Every indexed kind renders as a card — profile cards from kind-0 content, note cards, and a generic card driven by exactly the tags the extractors index (title/name/subject, summary/description/alt, image) with a kind badge and an author byline enriched by a batched kind-0 REQ. Bundled into `sot serve`'s `GET /`; `gradle :cli:uiDemo` serves it over an in-memory relay (no Vespa) seeded with demo events for UI development. Smoke-tested end to end with a headless browser + a fake NIP-07 signer: cards, chips, sort, and the AUTH flow. |
| `:cli` | The composition root and the `sot` binary. `sot init` is INTERACTIVE: a sectioned walkthrough (service identity, the relay's own key, the house account, network) where every question shows its default, answers validate-and-normalize as they land (npub/nip05 → hex via Quartz's resolver, relay urls canonicalized, the chosen port cascades into the url defaults), the identity nsec is generated on the spot unless pasted, and `--yes` scripts it (all defaults, still generating a key). `serve` = ONE Ktor app: WS `/` (the `:relay` engine) + NIP-11 on GET, plus the `:sync` background loop — relay and sync share the one store (`TrustProjection(VespaEventIndex)` under `VespaEventStore`), and NIP-42 logins flow through the relay's `onObserver` hook into `SyncService.enroll`, so the first authenticated search starts that user's trust sync. `index` = one pass; `status` = engine reachability + per-kind counts; `up`/`down`/`deploy`/`destroy` manage the local Vespa (deploying `vespa/app`; destroy wipes the sync cursors + the data volume — the event store lives IN Vespa). Config is env → `.env` → default, deliberately small: no `EVENTS_DB`, no `DEFAULT_OBSERVER` (the house account, `HOUSE_NPUB`/`HOUSE_RELAY`, replaces it), no indexer-relay key (the stored 10086 rules). |

**Deferred** (see `docs/v2-sync-proposal.md`): the records plane (roster ×
outbox directory via `write_relays`), seeded negentropy (awaiting the Quartz
API), and `attribute: paged` on fat Vespa attributes for kind-1 volume. Broad
kind-1 sync across arbitrary public relays is still only lightly exercised.
