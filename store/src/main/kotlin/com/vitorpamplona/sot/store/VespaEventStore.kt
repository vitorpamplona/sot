/*
 * Copyright (c) 2026 Vitor Pamplona
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy of
 * this software and associated documentation files (the "Software"), to deal in
 * the Software without restriction, including without limitation the rights to use,
 * copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the
 * Software, and to permit persons to whom the Software is furnished to do so,
 * subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS
 * FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR
 * COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN
 * AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION
 * WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 */
package com.vitorpamplona.sot.store

import com.vitorpamplona.quartz.nip01Core.core.Event
import com.vitorpamplona.quartz.nip01Core.core.isAddressable
import com.vitorpamplona.quartz.nip01Core.core.isEphemeral
import com.vitorpamplona.quartz.nip01Core.core.isReplaceable
import com.vitorpamplona.quartz.nip01Core.relay.filters.Filter
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.NormalizedRelayUrl
import com.vitorpamplona.quartz.nip01Core.store.FtsReindexProgress
import com.vitorpamplona.quartz.nip01Core.store.IEventStore
import com.vitorpamplona.quartz.nip01Core.store.IdAndTime
import com.vitorpamplona.quartz.nip01Core.tags.dTag.dTag
import com.vitorpamplona.quartz.nip09Deletions.DeletionEvent
import com.vitorpamplona.quartz.nip40Expiration.isExpired
import com.vitorpamplona.quartz.nip50Search.SearchableEvent
import com.vitorpamplona.quartz.nip62RequestToVanish.RequestToVanishEvent
import com.vitorpamplona.sot.vespa.QUERY_FANOUT
import com.vitorpamplona.sot.vespa.client.EventIndex
import com.vitorpamplona.sot.vespa.doc.EventDoc
import com.vitorpamplona.sot.vespa.doc.SearchFields
import com.vitorpamplona.sot.vespa.mapBounded
import com.vitorpamplona.sot.vespa.query.EventQuery
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import kotlin.coroutines.coroutineContext

/**
 * Quartz [IEventStore] backed by the search engine itself: ONE copy of the
 * data, queryable with full NIP-01 filters plus NIP-50 search, and wrappable in
 * `ObservableEventStore` like any other store.
 *
 * It enforces Nostr semantics in [insertLocked]:
 *
 *  - duplicates rejected ("duplicate:");
 *  - replaceables/addressables: strictly-older versions (created_at, then
 *    LOWEST id wins ties) are deleted on insert, and an insert that lost that
 *    comparison is rejected ("replaced:");
 *  - kind 5: targets erased (e-tags by id, a-tags — including replaceable
 *    addresses — up to the deletion's created_at, same-owner only per NIP-09),
 *    the kind 5 kept as a tombstone, and covered inserts rejected ("blocked:");
 *  - kind 62 covering [relay]: the owner's strictly-older events erased, the
 *    request kept, covered inserts rejected ("blocked:");
 *  - deletion/vanish enforcement keys on the event's OWNER: the gift-wrap
 *    recipient for kind 1059, else the author. Recipients control the wraps
 *    addressed to them;
 *  - ephemeral kinds are accepted WITHOUT storing (persistence is a no-op per
 *    NIP-01; an observable wrapper still broadcasts them live); already-expired
 *    events rejected; [deleteExpiredEvents] sweeps due NIP-40 expirations via
 *    the derived `expires_at` attribute;
 *  - NIP-50: only kinds implementing [SearchableEvent] are searchable, via
 *    [SearchExtractors], which decomposes each kind's indexable content into the
 *    schema's per-kind search fields. [reindexFullTextSearch] re-derives them
 *    after extractor/Quartz upgrades.
 *
 * Correctness rests on two properties. First, all writes serialize behind one
 * [Mutex], so query-then-write is atomic against other writers in this process.
 * Second, [EventIndex] guarantees an acked put is visible to search (see its
 * contract). There are no cross-document transactions: [transaction] buffers and
 * applies sequentially without rollback, which relay semantics never needed.
 *
 * Events are NOT verified here. Verification is the ingest path's job
 * (syncer/relay), once, before insert.
 */
class VespaEventStore(
    private val index: EventIndex,
    override val relay: NormalizedRelayUrl? = null,
    private val nowSecs: () -> Long = { System.currentTimeMillis() / 1000 },
) : IEventStore {
    private val writes = Mutex()

    // The bulk fast path shares this store's exact deletion/vanish probes for
    // its guard-page fallback (the rare owner whose guard set overflows a page).
    private val bulkInsert =
        BulkInsert(index, relay) { e ->
            rejectIfDeleted(e)
            rejectIfVanished(e)
        }

    override suspend fun insert(event: Event) = writes.withLock { insertLocked(event) }

    /**
     * Batches take the BULK fast path. The per-event path costs 3–5 index
     * round-trips each (dup probe, tombstone probe, vanish probe,
     * supersession), which caps ingest in the low thousands per second — useless
     * against a million-event sync. Bulk runs the same rules with chunked
     * queries and one pipelined [EventIndex.putAll].
     *
     * Kind 5 and kind 62 keep the exact sequential path: they MUTATE the store,
     * and order against their neighbors matters (a deletion inside the batch may
     * target an event earlier in it). The batch is processed as plain-event runs
     * separated by those events; tiny runs just loop [insertLocked].
     */
    override suspend fun batchInsert(events: List<Event>): List<IEventStore.InsertOutcome> =
        writes.withLock {
            val outcomes = arrayOfNulls<IEventStore.InsertOutcome>(events.size)
            var i = 0
            while (i < events.size) {
                if (events[i] is DeletionEvent || events[i] is RequestToVanishEvent) {
                    outcomes[i] = tryInsertLocked(events[i])
                    i++
                    continue
                }
                var j = i
                while (j < events.size && events[j] !is DeletionEvent && events[j] !is RequestToVanishEvent) j++
                val run = events.subList(i, j)
                if (run.size < BULK_MIN) {
                    run.forEachIndexed { k, ev -> outcomes[i + k] = tryInsertLocked(ev) }
                } else {
                    bulkInsert.run(run).forEachIndexed { k, o -> outcomes[i + k] = o }
                }
                i = j
            }
            outcomes.map { it ?: IEventStore.InsertOutcome.Rejected(Rejections.INSERT_FAILED) }
        }

    private suspend fun tryInsertLocked(event: Event): IEventStore.InsertOutcome =
        try {
            insertLocked(event)
            IEventStore.InsertOutcome.Accepted
        } catch (e: RejectedException) {
            // Only a SEMANTIC rejection (duplicate, replaced, blocked by a
            // deletion/vanish) becomes a Rejected outcome. A transient engine
            // failure (a 5xx that outlived its retries, an IO error) must
            // PROPAGATE — swallowing it as "Rejected" would silently DROP a
            // valid event and let the sync cursor advance past it. This matches
            // the bulk path, which already throws on engine failures.
            IEventStore.InsertOutcome.Rejected(e.message ?: Rejections.INSERT_FAILED)
        }

    /** No rollback: buffered inserts apply in order; the first rejection propagates and aborts the rest. */
    override suspend fun transaction(body: IEventStore.ITransaction.() -> Unit) {
        val buffered = ArrayList<Event>()
        object : IEventStore.ITransaction {
            override fun insert(event: Event) {
                buffered += event
            }
        }.body()
        writes.withLock { buffered.forEach { insertLocked(it) } }
    }

    private suspend fun insertLocked(event: Event) {
        // Accepted but never persisted (NIP-01): an ObservableEventStore wrapper
        // still broadcasts the insert to live subscribers.
        if (event.kind.isEphemeral()) return
        if (event.isExpired()) throw RejectedException(Rejections.EXPIRED)
        if (index.get(event.id) != null) throw RejectedException(Rejections.DUPLICATE)
        rejectIfDeleted(event)
        rejectIfVanished(event)
        when (event) {
            is DeletionEvent -> applyDeletion(event)
            is RequestToVanishEvent -> applyVanish(event)
            else -> supersede(event)
        }
        index.put(event.toDoc())
    }

    // ---- queries ------------------------------------------------------------

    /** Map a filter to an [EventQuery] stamped with the current expiry cutoff (NIP-40) and, for searches, the ranking observer. */
    private fun Filter.toExpiryQuery(observer: String? = null): EventQuery? = toEventQuery()?.copy(notExpiredAt = nowSecs(), observer = observer)

    override suspend fun <T : Event> query(filter: Filter): List<T> = query(listOf(filter))

    @Suppress("UNCHECKED_CAST")
    override suspend fun <T : Event> query(filters: List<Filter>): List<T> {
        val observer = coroutineContext[ObserverContext]?.pubkey
        val queries = restoreSearches(filters).mapNotNull { it.toExpiryQuery(observer) }
        val docs = searchDocs(queries)
        // NIP-50: a searching query's results stay in the engine's RELEVANCE
        // order "instead of the usual created_at ordering" — re-sorting here
        // would undo the rank profile. Plain filters keep NIP-01 recency.
        val ranked = queries.any { it.search != null || it.ranking != null }
        val ordered = if (ranked) docs else docs.sortedWith(NEWEST_FIRST)
        return ordered.mapNotNull { Event.fromJsonOrNull(it.toEventJson()) } as List<T>
    }

    /** Search every query concurrently (bounded), deduped by id — the shared recall for query and multi-filter count. */
    private suspend fun searchDocs(queries: List<EventQuery>): List<EventDoc> =
        when (queries.size) {
            0 -> emptyList()
            1 -> index.search(queries[0])
            else -> queries.mapBounded(QUERY_FANOUT) { index.search(it) }.flatten().distinctBy { it.id }
        }

    override suspend fun <T : Event> query(
        filter: Filter,
        onEach: (T) -> Unit,
    ) = query<T>(filter).forEach(onEach)

    override suspend fun <T : Event> query(
        filters: List<Filter>,
        onEach: (T) -> Unit,
    ) = query<T>(filters).forEach(onEach)

    override suspend fun count(filter: Filter): Int = restoreSearches(listOf(filter)).single().toExpiryQuery()?.let { index.count(it) } ?: 0

    override suspend fun count(filters: List<Filter>): Int {
        // Multi-filter counts need cross-filter id dedup (engine count can't),
        // but they don't need the events materialized — recall the docs and
        // count distinct ids, skipping the per-doc Event reconstruction.
        if (filters.size == 1) return count(filters[0])
        val queries = restoreSearches(filters).mapNotNull { it.toExpiryQuery() }
        return searchDocs(queries).size
    }

    /**
     * Undo Quartz's relay-side NIP-50 extension stripping. `LiveEventStore`
     * strips `key:value` tokens from every REQ's search before the store sees
     * it. That is the right default for stores that would match them as text,
     * but THIS store honors `sort:`/`filter:rank:`/`include:spam`. The relay
     * backend carries the pre-strip filters in [OriginalFilters] — the same list
     * in the same order, only `search` differs — so each filter's original
     * search string is restored before mapping. Direct callers (no context
     * element) are untouched.
     */
    private suspend fun restoreSearches(filters: List<Filter>): List<Filter> {
        val originals = coroutineContext[OriginalFilters]?.filters ?: return filters
        if (originals.size != filters.size) return filters
        return filters.mapIndexed { i, f ->
            val original = originals[i].search
            if (original != null && original != f.search) f.copy(search = original) else f
        }
    }

    /**
     * (created_at, id) pairs straight off the docs — no Event materialization
     * and no result cap. Plain filters walk the corpus through the engine's
     * visit ([com.vitorpamplona.sot.vespa.EventIndex.visitIds]), so a negentropy
     * session (or a sync reconcile diff) sees the COMPLETE match set even when it
     * dwarfs the search page limit. Searching or limit'd filters keep the search
     * path, since their semantics live there.
     */
    override suspend fun snapshotIdsForNegentropy(
        filters: List<Filter>,
        maxEntries: Int?,
    ): List<IdAndTime> {
        val all = ArrayList<IdAndTime>()
        // A single-filter cap can stop the walk early: the caller only needs to
        // learn the set exceeds the cap, not scan a 10M corpus to prove it.
        // (Multi-filter needs the full set for cross-filter dedup, so no break.)
        val cap = maxEntries?.takeIf { filters.size == 1 }?.plus(1)
        // Exclude already-expired events (NIP-40), exactly as query/count do.
        // Otherwise the negentropy set offers ids a plain REQ would never serve,
        // and a peer keeps trying to reconcile events we refuse to return.
        for (q in filters.mapNotNull { it.toExpiryQuery() }) {
            if (q.search == null && q.limit == null) {
                index.visitIds(q) { page ->
                    page.forEach { all += IdAndTime(it.createdAt, it.id) }
                    cap == null || all.size < cap
                }
            } else {
                index.search(q).forEach { all += IdAndTime(it.createdAt, it.id) }
            }
        }
        val unique = if (filters.size > 1) all.distinctBy { it.id } else all
        return if (maxEntries != null && unique.size > maxEntries + 1) unique.subList(0, maxEntries + 1) else unique
    }

    // ---- deletes ------------------------------------------------------------

    override suspend fun delete(filter: Filter) = delete(listOf(filter))

    override suspend fun delete(filters: List<Filter>) {
        writes.withLock { filters.mapNotNull { it.toEventQuery() }.forEach { sweep(it) } }
    }

    override suspend fun deleteExpiredEvents() {
        // expiresBefore is strict (<): +1 makes "expires exactly now" due, per NIP-40.
        writes.withLock { sweep(EventQuery(expiresBefore = nowSecs() + 1)) }
    }

    /** Remove every match, page by page, until the query comes back empty. */
    private suspend fun sweep(q: EventQuery) {
        var rounds = 0
        while (rounds++ < MAX_SWEEP_ROUNDS) {
            val page = index.search(q)
            if (page.isEmpty()) return
            index.removeAll(page.map { it.id })
            // A limit'd delete is satisfied by its first page.
            if (q.limit != null) return
        }
    }

    // ---- Nostr semantics -------------------------------------------------------

    /**
     * NIP-09: a kind 5 authored by this event's OWNER, e/a-tagging it, with
     * created_at >= the event's, blocks the insert. Both target styles (e-tag
     * and a-tag) are time-guarded.
     */
    private suspend fun rejectIfDeleted(event: Event) {
        // NIP-09/NIP-62: a deletion request against a deletion request or a
        // request to vanish has no effect — they are immune to kind-5 tombstones.
        if (event is DeletionEvent || event is RequestToVanishEvent) return
        val owner = event.owner()

        suspend fun deletionExists(
            tagKey: String,
            value: String,
        ): Boolean = index.search(EventQuery(kinds = listOf(DeletionEvent.KIND), authors = listOf(owner), tags = mapOf(tagKey to listOf(value)), since = event.createdAt, limit = 1)).isNotEmpty()
        if (deletionExists("e", event.id)) throw RejectedException(Rejections.DELETED)
        val address = event.addressOrNull() ?: return
        if (deletionExists("a", address)) throw RejectedException(Rejections.DELETED)
    }

    /** NIP-62: a stored vanish request by this event's OWNER covering [relay] blocks their events up to its time. */
    private suspend fun rejectIfVanished(event: Event) {
        val vanishes = index.search(EventQuery(kinds = listOf(RequestToVanishEvent.KIND), authors = listOf(event.owner()), since = event.createdAt))
        val blocked =
            vanishes.any { doc ->
                (Event.fromJsonOrNull(doc.toEventJson()) as? RequestToVanishEvent)?.shouldVanishFrom(relay) == true
            }
        if (blocked) throw RejectedException(Rejections.VANISHED)
    }

    /**
     * Replaceable/addressable version resolution in ONE query. Fetch the stored
     * versions once. If any of them wins the (created_at, lowest id wins ties)
     * comparison, reject this insert. Otherwise delete the strictly-older ones it
     * supersedes.
     */
    private suspend fun supersede(event: Event) {
        val versions = currentVersions(event)
        if (versions.any { it.createdAt > event.createdAt || (it.createdAt == event.createdAt && it.id < event.id) }) {
            throw RejectedException(Rejections.REPLACED)
        }
        versions.forEach {
            if (it.createdAt < event.createdAt || (it.createdAt == event.createdAt && it.id > event.id)) index.remove(it.id)
        }
    }

    /**
     * The stored versions sharing this event's replaceable address. Addressables
     * compare d-tags doc-side (not via tag_index) so a missing d tag and an
     * explicit empty one meet at the same address, per NIP-01.
     */
    private suspend fun currentVersions(event: Event): List<EventDoc> {
        if (!event.kind.isReplaceable() && !event.kind.isAddressable()) return emptyList()
        if (!event.kind.isAddressable()) {
            // Replaceable: one address per (kind, author); the query is exact.
            return index.search(EventQuery(kinds = listOf(event.kind), authors = listOf(event.pubKey)))
        }
        val d = event.tags.dTag()
        // Constrain by d in the QUERY when it's present, so a prolific author's
        // OTHER addresses of this kind don't push the target version past the
        // 10k search page. Missing it would miss supersession — a real defect for
        // trust providers with tens of thousands of 30382s. The empty/missing-d
        // address can't use tag recall, so it keeps the broad (kind, author)
        // query; an author with >10k empty-d addressables of one kind is not a
        // real case. The doc-side d filter still normalizes missing == empty.
        val docs =
            if (d.isNullOrEmpty()) {
                index.search(EventQuery(kinds = listOf(event.kind), authors = listOf(event.pubKey)))
            } else {
                index.search(EventQuery(kinds = listOf(event.kind), authors = listOf(event.pubKey), tags = mapOf("d" to listOf(d))))
            }
        return docs.filter { doc -> doc.dTagOrEmpty() == d }
    }

    /**
     * NIP-09 enforcement: erase this kind 5's targets — by id when the doc's
     * OWNER is the deletion author (a recipient deletes gift-wraps sent to
     * them), by address (addressable AND replaceable kinds) up to the
     * deletion's created_at, same author only. The event itself is stored
     * after, as the tombstone.
     */
    private suspend fun applyDeletion(ev: DeletionEvent) {
        for (id in ev.deleteEventIds()) {
            val doc = index.get(id) ?: continue
            // NIP-09/NIP-62: kind 5 against a kind 5 or a kind 62 has no effect.
            if (doc.kind == DeletionEvent.KIND || doc.kind == RequestToVanishEvent.KIND) continue
            if (doc.owner == ev.pubKey) index.remove(id)
        }
        for (address in ev.deleteAddresses()) {
            if (address.pubKeyHex != ev.pubKey) continue
            if (!address.kind.isAddressable() && !address.kind.isReplaceable()) continue
            index
                .search(EventQuery(kinds = listOf(address.kind), authors = listOf(address.pubKeyHex), until = ev.createdAt))
                // Replaceable kinds have ONE address regardless of the a-tag's d part.
                .filter { !address.kind.isAddressable() || it.dTagOrEmpty() == address.dTag }
                .forEach { index.remove(it.id) }
        }
    }

    /**
     * NIP-62 enforcement: when the request covers [relay], erase the owner's
     * history "until its created_at" — INCLUSIVE, per the spec. The request
     * itself is only stored after this runs, so it survives its own sweep.
     */
    private suspend fun applyVanish(ev: RequestToVanishEvent) {
        if (!ev.shouldVanishFrom(relay)) return
        sweep(EventQuery(owners = listOf(ev.pubKey), until = ev.createdAt))
    }

    // ---- full-text reindex ----------------------------------------------------

    /**
     * Re-derive the search fields for every stored event. Which kinds are
     * searchable — and how [SearchExtractors] decomposes them — is baked into
     * this build, so docs indexed under old code can be stale (or missing from
     * search) until this runs. It also clears fields for kinds that LOST
     * searchability.
     */
    override suspend fun reindexFullTextSearch() {
        var cursor: String? = null
        do {
            val progress = reindexFullTextSearch(cursor)
            cursor = progress.cursor
        } while (!progress.done)
    }

    /**
     * Resumable batch: docs are walked in id order from [resumeFrom]
     * (exclusive). This is reference-grade paging: each call re-lists the ids
     * through [EventIndex.search]. The real Vespa client will page with a visit.
     */
    override suspend fun reindexFullTextSearch(
        resumeFrom: String?,
        batchSize: Int,
    ): FtsReindexProgress =
        writes.withLock {
            val batch =
                index
                    .search(EventQuery())
                    .sortedBy { it.id }
                    .filter { resumeFrom == null || it.id > resumeFrom }
                    .take(batchSize)
            for (doc in batch) {
                val fields = Event.fromJsonOrNull(doc.toEventJson())?.let(SearchExtractors::extract) ?: SearchFields.NONE
                if (fields != doc.search) index.put(doc.copy(search = fields))
            }
            FtsReindexProgress(cursor = batch.lastOrNull()?.id, processedThisBatch = batch.size, done = batch.size < batchSize)
        }

    override fun close() = index.close()

    private companion object {
        // Page-sized rounds; a page of results per round means a runaway sweep
        // still terminates loudly rather than spinning forever.
        const val MAX_SWEEP_ROUNDS = 10_000

        // Runs at least this long take the bulk path; smaller ones aren't
        // worth the setup and stay on the per-event path.
        const val BULK_MIN = 16
        val NEWEST_FIRST = compareByDescending(EventDoc::createdAt).thenBy(EventDoc::id)
    }
}
