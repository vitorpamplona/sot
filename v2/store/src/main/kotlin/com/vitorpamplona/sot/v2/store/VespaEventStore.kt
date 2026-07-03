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
package com.vitorpamplona.sot.v2.store

import com.vitorpamplona.quartz.nip01Core.core.Address
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
import com.vitorpamplona.quartz.nip50Search.SearchQuery
import com.vitorpamplona.quartz.nip50Search.SearchableEvent
import com.vitorpamplona.quartz.nip59Giftwrap.wraps.GiftWrapEvent
import com.vitorpamplona.quartz.nip62RequestToVanish.RequestToVanishEvent
import com.vitorpamplona.sot.v2.vespa.EventDoc
import com.vitorpamplona.sot.v2.vespa.EventIndex
import com.vitorpamplona.sot.v2.vespa.EventQuery
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock

/**
 * Quartz [IEventStore] backed by the search engine itself — the v2 replacement
 * for the SQLite store: ONE copy of the data, queryable with full NIP-01
 * filters plus NIP-50 search, wrappable in `ObservableEventStore` like any
 * other store.
 *
 * The SQLite store enforces Nostr semantics with triggers; this store enforces
 * the same rules in [insertLocked], matching sqlite ...Module.kt behavior:
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
 *  - deletion/vanish enforcement keys on the event's OWNER (SQLite's
 *    pubkey_owner_hash): the gift-wrap recipient for kind 1059, else the
 *    author — recipients control the wraps addressed to them;
 *  - ephemeral kinds are accepted WITHOUT storing (persistence is a no-op per
 *    NIP-01; an observable wrapper still broadcasts them live); already-expired
 *    events rejected; [deleteExpiredEvents] sweeps due NIP-40 expirations via
 *    the derived `expires_at` attribute;
 *  - NIP-50: only kinds implementing [SearchableEvent] are searchable, via
 *    their `indexableContent()` (the `search_text` field = SQLite's FTS
 *    table); [reindexFullTextSearch] re-derives it after Quartz upgrades.
 *
 * Correctness rests on two properties: all writes serialize behind one [Mutex]
 * (query-then-write is atomic against other writers in this process — mirror
 * of v1's single-writer rule), and [EventIndex] guarantees an acked put is
 * visible to search (see its contract). There are no cross-document
 * transactions: [transaction] buffers and applies sequentially without
 * rollback, which relay semantics never needed.
 *
 * Events are NOT verified here — like the SQLite store, verification is the
 * ingest path's job (syncer/relay), once, before insert.
 */
class VespaEventStore(
    private val index: EventIndex,
    override val relay: NormalizedRelayUrl? = null,
    private val nowSecs: () -> Long = { System.currentTimeMillis() / 1000 },
) : IEventStore {
    class RejectedException(
        message: String,
    ) : Exception(message)

    private val writes = Mutex()

    override suspend fun insert(event: Event) = writes.withLock { insertLocked(event) }

    override suspend fun batchInsert(events: List<Event>): List<IEventStore.InsertOutcome> =
        writes.withLock {
            events.map { event ->
                try {
                    insertLocked(event)
                    IEventStore.InsertOutcome.Accepted
                } catch (e: Exception) {
                    IEventStore.InsertOutcome.Rejected(e.message ?: "insert failed")
                }
            }
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
        if (event.isExpired()) throw RejectedException("blocked: Cannot insert an expired event")
        if (index.get(event.id) != null) throw RejectedException("duplicate: already have this event")
        rejectIfDeleted(event)
        rejectIfVanished(event)
        rejectIfSuperseded(event)
        when (event) {
            is DeletionEvent -> applyDeletion(event)
            is RequestToVanishEvent -> applyVanish(event)
            else -> supersedeOlder(event)
        }
        index.put(event.toDoc())
    }

    // ---- queries ------------------------------------------------------------

    override suspend fun <T : Event> query(filter: Filter): List<T> = query(listOf(filter))

    @Suppress("UNCHECKED_CAST")
    override suspend fun <T : Event> query(filters: List<Filter>): List<T> =
        filters
            .mapNotNull { it.toEventQuery()?.copy(notExpiredAt = nowSecs()) }
            .flatMap { index.search(it) }
            .distinctBy { it.id }
            .sortedWith(NEWEST_FIRST)
            .mapNotNull { Event.fromJsonOrNull(it.toEventJson()) } as List<T>

    override suspend fun <T : Event> query(
        filter: Filter,
        onEach: (T) -> Unit,
    ) = query<T>(filter).forEach(onEach)

    override suspend fun <T : Event> query(
        filters: List<Filter>,
        onEach: (T) -> Unit,
    ) = query<T>(filters).forEach(onEach)

    override suspend fun count(filter: Filter): Int = filter.toEventQuery()?.let { index.count(it.copy(notExpiredAt = nowSecs())) } ?: 0

    override suspend fun count(filters: List<Filter>): Int = if (filters.size == 1) count(filters[0]) else query<Event>(filters).size

    /** (created_at, id) pairs straight off the docs — no Event materialization. */
    override suspend fun snapshotIdsForNegentropy(
        filters: List<Filter>,
        maxEntries: Int?,
    ): List<IdAndTime> {
        val all =
            filters
                .mapNotNull { it.toEventQuery() }
                .flatMap { index.search(it) }
                .distinctBy { it.id }
                .map { IdAndTime(it.createdAt, it.id) }
        return if (maxEntries != null && all.size > maxEntries + 1) all.subList(0, maxEntries + 1) else all
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
            page.forEach { index.remove(it.id) }
            // A limit'd delete is satisfied by its first page.
            if (q.limit != null) return
        }
    }

    // ---- Nostr semantics (the sqlite ...Module.kt rules) -----------------------

    /**
     * NIP-09: a kind 5 authored by this event's OWNER, e/a-tagging it, with
     * created_at >= the event's, blocks the insert (SQLite's
     * reject_deleted_events trigger, both target styles time-guarded).
     */
    private suspend fun rejectIfDeleted(event: Event) {
        // NIP-09/NIP-62: a deletion request against a deletion request or a
        // request to vanish has no effect — they are immune to kind-5 tombstones.
        if (event is DeletionEvent || event is RequestToVanishEvent) return
        val owner = event.owner()
        val byId = index.search(EventQuery(kinds = listOf(DeletionEvent.KIND), authors = listOf(owner), tags = mapOf("e" to listOf(event.id)), since = event.createdAt, limit = 1))
        if (byId.isNotEmpty()) throw RejectedException("blocked: a deletion event exists")
        val address = event.addressOrNull() ?: return
        val byAddress = index.search(EventQuery(kinds = listOf(DeletionEvent.KIND), authors = listOf(owner), tags = mapOf("a" to listOf(address)), since = event.createdAt, limit = 1))
        if (byAddress.isNotEmpty()) throw RejectedException("blocked: a deletion event exists")
    }

    /** NIP-62: a stored vanish request by this event's OWNER covering [relay] blocks their events up to its time. */
    private suspend fun rejectIfVanished(event: Event) {
        val vanishes = index.search(EventQuery(kinds = listOf(RequestToVanishEvent.KIND), authors = listOf(event.owner()), since = event.createdAt))
        val blocked =
            vanishes.any { doc ->
                (Event.fromJsonOrNull(doc.toEventJson()) as? RequestToVanishEvent)?.shouldVanishFrom(relay) == true
            }
        if (blocked) throw RejectedException("blocked: a request to vanish event exists")
    }

    /** Reject a replaceable/addressable that LOST the version comparison (created_at, lowest id wins ties). */
    private suspend fun rejectIfSuperseded(event: Event) {
        val newer =
            currentVersions(event).any {
                it.createdAt > event.createdAt || (it.createdAt == event.createdAt && it.id < event.id)
            }
        if (newer) throw RejectedException("replaced: a newer version exists")
    }

    /** Delete the strictly-older versions this insert supersedes (the sqlite trigger's DELETE). */
    private suspend fun supersedeOlder(event: Event) {
        currentVersions(event).forEach {
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
        val sameKindAuthor = index.search(EventQuery(kinds = listOf(event.kind), authors = listOf(event.pubKey)))
        if (!event.kind.isAddressable()) return sameKindAuthor
        val d = event.tags.dTag()
        return sameKindAuthor.filter { doc -> dTagOf(doc.tags) == d }
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
                .filter { !address.kind.isAddressable() || dTagOf(it.tags) == address.dTag }
                .forEach { index.remove(it.id) }
        }
    }

    /**
     * NIP-62 enforcement: when the request covers [relay], erase the owner's
     * history "until its created_at" — INCLUSIVE, per the spec (Quartz's
     * SQLite trigger uses strict <; the spec wins). The request itself is only
     * stored after this runs, so it survives its own sweep.
     */
    private suspend fun applyVanish(ev: RequestToVanishEvent) {
        if (!ev.shouldVanishFrom(relay)) return
        sweep(EventQuery(owners = listOf(ev.pubKey), until = ev.createdAt))
    }

    // ---- full-text reindex ----------------------------------------------------

    /**
     * Re-derive `search_text` for every stored event. Which kinds implement
     * [SearchableEvent] — and what text they contribute — is baked into the
     * Quartz build, so docs indexed under old code can be stale (or missing
     * from search) until this runs; it also clears text for kinds that LOST
     * searchability, mirroring the one-shot SQLite rebuild.
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
     * (exclusive). Reference-grade paging — each call re-lists the ids through
     * [EventIndex.search]; the real Vespa client will page with a visit.
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
                val text = (Event.fromJsonOrNull(doc.toEventJson()) as? SearchableEvent)?.indexableContent()
                if (text != doc.searchText) index.put(doc.copy(searchText = text))
            }
            FtsReindexProgress(cursor = batch.lastOrNull()?.id, processedThisBatch = batch.size, done = batch.size < batchSize)
        }

    override fun close() = index.close()

    private companion object {
        // Page-sized rounds; a page of results per round means a runaway sweep
        // still terminates loudly rather than spinning forever.
        const val MAX_SWEEP_ROUNDS = 10_000
        val NEWEST_FIRST = compareByDescending(EventDoc::createdAt).thenBy(EventDoc::id)
    }
}

/**
 * Quartz Filter -> the engine's plain [EventQuery]. Null when the filter can
 * never match: NIP-01 semantics for a present-but-EMPTY list ("the event's
 * value must be in the list" — of nothing). Absent (null) lists mean no
 * constraint, which is EventQuery's empty default.
 */
internal fun Filter.toEventQuery(): EventQuery? {
    if (ids?.isEmpty() == true || authors?.isEmpty() == true || kinds?.isEmpty() == true) return null
    if (tags?.values?.any { it.isEmpty() } == true || tagsAll?.values?.any { it.isEmpty() } == true) return null
    return EventQuery(
        ids = ids.orEmpty(),
        kinds = kinds.orEmpty(),
        authors = authors.orEmpty(),
        tags = tags.orEmpty(),
        tagsAll = tagsAll.orEmpty(),
        since = since,
        until = until,
        limit = limit,
        // NIP-50: extensions are relay hints, not text ("relays SHOULD ignore
        // extensions they don't support"). Quartz's parser splits them off —
        // and, unlike a naive key:value regex, keeps `scheme://…` tokens as
        // terms. None are honored yet; an all-extensions query becomes
        // unconstrained (null), not match-nothing.
        search = SearchQuery.parse(search).terms.ifEmpty { null },
    )
}

/**
 * The event's exact stored form plus the two derived fields: [EventDoc.owner]
 * (gift-wrap recipient or author) and [EventDoc.searchText] (searchable kinds'
 * indexable text — SQLite's FTS row). Scope stays empty: provenance is the
 * syncer's concern, never semantics.
 */
internal fun Event.toDoc(): EventDoc =
    EventDoc(
        id = id,
        pubkey = pubKey,
        createdAt = createdAt,
        kind = kind,
        tags = tags.map { it.toList() },
        content = content,
        sig = sig,
        scope = "",
        owner = owner(),
        searchText = (this as? SearchableEvent)?.indexableContent(),
    )

/** The pubkey Nostr semantics key off (SQLite's pubkey_owner_hash): the gift-wrap recipient, else the author. */
internal fun Event.owner(): String = (this as? GiftWrapEvent)?.recipientPubKey() ?: pubKey

/**
 * The NIP-01 address for replaceable/addressable kinds; null for regular
 * events. Replaceables use the fixed empty d-tag regardless of stray d tags,
 * matching Quartz's BaseReplaceableEvent.FIXED_D_TAG.
 */
internal fun Event.addressOrNull(): String? =
    when {
        kind.isReplaceable() -> Address.assemble(kind, pubKey)
        kind.isAddressable() -> Address.assemble(kind, pubKey, tags.dTag())
        else -> null
    }

/** The doc-side twin of Quartz's TagArray.dTag() — EventDoc tags are plain lists, not TagArrays. */
internal fun dTagOf(tags: List<List<String>>): String = tags.firstOrNull { it.size >= 2 && it[0] == "d" }?.get(1) ?: ""
