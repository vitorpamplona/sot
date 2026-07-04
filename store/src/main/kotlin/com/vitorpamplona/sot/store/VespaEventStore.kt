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
import com.vitorpamplona.sot.vespa.EventDoc
import com.vitorpamplona.sot.vespa.EventIndex
import com.vitorpamplona.sot.vespa.EventQuery
import com.vitorpamplona.sot.vespa.EventYql
import com.vitorpamplona.sot.vespa.IngestStats
import com.vitorpamplona.sot.vespa.QUERY_FANOUT
import com.vitorpamplona.sot.vespa.SearchFields
import com.vitorpamplona.sot.vespa.mapBounded
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import kotlin.coroutines.coroutineContext

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
 *    [SearchExtractors] — every kind's indexable content decomposed into the
 *    schema's per-kind search fields (= SQLite's FTS table, tiered);
 *    [reindexFullTextSearch] re-derives them after extractor/Quartz upgrades.
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

    /**
     * Batches take the BULK fast path: the per-event path costs 3–5 index
     * round-trips each (dup probe, tombstone probe, vanish probe,
     * supersession), which caps ingest in the low thousands per second —
     * useless against a million-event sync. Bulk runs the same rules with
     * chunked queries and one pipelined [EventIndex.putAll].
     *
     * Kind 5 and kind 62 keep the exact sequential path: they MUTATE the
     * store, and order against their neighbors matters (a deletion inside the
     * batch may target an event earlier in it). The batch is processed as
     * plain-event runs separated by those events; tiny runs just loop
     * [insertLocked].
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
                    bulkInsertRun(run).forEachIndexed { k, o -> outcomes[i + k] = o }
                }
                i = j
            }
            outcomes.map { it ?: IEventStore.InsertOutcome.Rejected("insert failed") }
        }

    private suspend fun tryInsertLocked(event: Event): IEventStore.InsertOutcome =
        try {
            insertLocked(event)
            IEventStore.InsertOutcome.Accepted
        } catch (e: Exception) {
            IEventStore.InsertOutcome.Rejected(e.message ?: "insert failed")
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

    /**
     * One run of plain events (no kind 5/62), the same rules as
     * [insertLocked] with batched I/O. Stages: local checks (ephemeral,
     * expired, intra-run duplicate ids) -> one `id in (…)` duplicate query per
     * [CHECK_CHUNK] -> per-owner tombstone/vanish guards (one query each; an
     * owner with a guard set too large for one page falls back to the exact
     * per-event probes) -> per-address supersession resolved IN RUN ORDER
     * (existing versions fetched in chunked queries; losers inside the run
     * are Accepted-then-superseded, exactly as sequential inserts would end
     * up) -> one pipelined putAll of the survivors.
     */
    private suspend fun bulkInsertRun(events: List<Event>): List<IEventStore.InsertOutcome> {
        val outcome = arrayOfNulls<IEventStore.InsertOutcome>(events.size)

        fun alive() = events.indices.filter { outcome[it] == null }

        // Stage A — no I/O: ephemeral accepted-not-stored, expired rejected,
        // later copies of an id already in this run rejected as duplicates.
        val seen = HashSet<String>()
        events.forEachIndexed { i, e ->
            when {
                e.kind.isEphemeral() -> outcome[i] = IEventStore.InsertOutcome.Accepted
                e.isExpired() -> outcome[i] = IEventStore.InsertOutcome.Rejected("blocked: Cannot insert an expired event")
                !seen.add(e.id) -> outcome[i] = IEventStore.InsertOutcome.Rejected("duplicate: already have this event")
            }
        }

        // Stage B — ids already stored. The chunk queries are independent
        // reads; they fan out with BOUNDED concurrency (serialized round trips
        // starve the batch, but unbounded fan-out measurably 504s the engine's
        // summary stage).
        val stored = HashSet<String>()
        IngestStats.timed("dedup") {
            alive()
                .map { events[it].id }
                .chunked(CHECK_CHUNK)
                .mapBounded(QUERY_FANOUT) { chunk -> index.search(EventQuery(ids = chunk)) }
                .forEach { docs -> docs.forEach { stored += it.id } }
        }
        alive().forEach { i -> if (events[i].id in stored) outcome[i] = IEventStore.InsertOutcome.Rejected("duplicate: already have this event") }

        // Stage C — tombstone + vanish guards, one pass per distinct owner;
        // the guard reads fan out (bounded) across owners.
        val owners = alive().groupBy { events[it].owner() }
        val guards =
            IngestStats.timed("guards") {
                owners.keys
                    .toList()
                    .mapBounded(QUERY_FANOUT) { owner ->
                        owner to
                            Pair(
                                index.search(EventQuery(kinds = listOf(DeletionEvent.KIND), authors = listOf(owner))),
                                index.search(EventQuery(kinds = listOf(RequestToVanishEvent.KIND), authors = listOf(owner))),
                            )
                    }.toMap()
            }
        for ((owner, idxs) in owners) {
            val (tombs, vanishes) = guards.getValue(owner)
            if (tombs.size >= GUARD_PAGE || vanishes.size >= GUARD_PAGE) {
                // Guard set larger than a page: the batched view could miss one.
                // Exactness over speed — run these events through the per-event probes.
                for (i in idxs) {
                    outcome[i] =
                        try {
                            rejectIfDeleted(events[i])
                            rejectIfVanished(events[i])
                            null
                        } catch (e: Exception) {
                            IEventStore.InsertOutcome.Rejected(e.message ?: "blocked")
                        }
                }
                continue
            }
            // target -> the newest guarding tombstone's created_at.
            val byId = HashMap<String, Long>()
            val byAddress = HashMap<String, Long>()
            tombs.forEach { doc ->
                doc.tags.forEach { t ->
                    if (t.size > 1) {
                        when (t[0]) {
                            "e" -> byId.merge(t[1], doc.createdAt, ::maxOf)
                            "a" -> byAddress.merge(t[1], doc.createdAt, ::maxOf)
                        }
                    }
                }
            }
            val vanishAt =
                vanishes
                    .mapNotNull { doc -> (Event.fromJsonOrNull(doc.toEventJson()) as? RequestToVanishEvent)?.takeIf { it.shouldVanishFrom(relay) }?.createdAt }
                    .maxOrNull() ?: Long.MIN_VALUE
            for (i in idxs) {
                val e = events[i]
                val guard = maxOf(byId[e.id] ?: Long.MIN_VALUE, e.addressOrNull()?.let { byAddress[it] } ?: Long.MIN_VALUE)
                if (guard >= e.createdAt) {
                    outcome[i] = IEventStore.InsertOutcome.Rejected("blocked: a deletion event exists")
                } else if (e.createdAt <= vanishAt) {
                    outcome[i] = IEventStore.InsertOutcome.Rejected("blocked: a request to vanish event exists")
                }
            }
        }

        // Stage D — supersession per replaceable address, resolved in run order.
        val toPut = LinkedHashMap<String, Event>() // id -> event scheduled for storage
        val groups = LinkedHashMap<Triple<Int, String, String?>, MutableList<Int>>()
        alive().forEach { i ->
            val e = events[i]
            if (e.kind.isReplaceable() || e.kind.isAddressable()) {
                val d = if (e.kind.isAddressable()) e.tags.dTag() else null
                groups.getOrPut(Triple(e.kind, e.pubKey, d)) { mutableListOf() } += i
            } else {
                toPut[e.id] = e
            }
        }
        // Existing versions for every touched address, chunked: replaceables by
        // (kind, authors…); addressables by (kind, authors…, d-tags…) via
        // tag_index recall, bucketed doc-side (the d filter is exact there).
        val existing = HashMap<Triple<Int, String, String?>, MutableList<EventDoc>>()
        val addressable = groups.keys.filter { it.third != null }
        val replaceable = groups.keys.filter { it.third == null }
        val versionQueries =
            buildList {
                for ((kind, keys) in replaceable.groupBy { it.first }) {
                    keys.map { it.second }.distinct().chunked(CHECK_CHUNK).forEach { authors ->
                        add(EventQuery(kinds = listOf(kind), authors = authors))
                    }
                }
                for ((kind, keys) in addressable.groupBy { it.first }) {
                    // Small chunks on purpose: the (authors x d-tags) recall is a
                    // cross product — a dense corpus (dozens of service keys
                    // scoring the same subjects) at 500 pairs recalls tens of
                    // thousands of docs, sailing past the search page cap and
                    // SILENTLY missing existing versions.
                    keys.chunked(ADDR_CHUNK).forEach { chunk ->
                        val authors = chunk.map { it.second }.distinct()
                        val ds = chunk.mapNotNull { it.third }.distinct()
                        add(EventQuery(kinds = listOf(kind), authors = authors, tags = mapOf("d" to ds)))
                    }
                }
            }
        IngestStats
            .timed("versions") {
                versionQueries.mapBounded(QUERY_FANOUT) { q -> index.search(q) }
            }.forEach { docs ->
                docs.forEach { doc ->
                    val d = if (doc.kind.isAddressable()) dTagOf(doc.tags) else null
                    existing.getOrPut(Triple(doc.kind, doc.pubkey, d)) { mutableListOf() } += doc
                }
            }
        val removeFromStore = ArrayList<String>()
        for ((key, idxs) in groups) {
            val versions = existing[key].orEmpty()
            // The run competes against the store's best; every stored version
            // strictly older than the final winner is swept, like supersedeOlder.
            var bestDocId: String? = versions.maxWithOrNull(compareBy<EventDoc> { it.createdAt }.thenByDescending { it.id })?.id
            var bestAt = versions.maxOfOrNull { it.createdAt } ?: Long.MIN_VALUE
            var bestId = versions.filter { it.createdAt == bestAt }.minOfOrNull { it.id }
            var bestInRun: Int? = null
            for (i in idxs) {
                val e = events[i]
                val lost = bestId != null && (bestAt > e.createdAt || (bestAt == e.createdAt && bestId!! < e.id))
                if (lost) {
                    outcome[i] = IEventStore.InsertOutcome.Rejected("replaced: a newer version exists")
                } else {
                    // The previous best is superseded: an in-run best stays
                    // Accepted but never lands; a stored best is removed.
                    bestInRun?.let { toPut.remove(events[it].id) }
                    bestDocId?.let { removeFromStore += it }
                    bestDocId = null
                    bestInRun = i
                    bestAt = e.createdAt
                    bestId = e.id
                    toPut[e.id] = e
                }
            }
            // Older stored versions beyond the single best also fall (drift repair).
            versions.forEach { doc -> if (doc.id != bestDocId && doc.id !in removeFromStore) removeFromStore += doc.id }
        }
        removeFromStore.distinct().forEach { index.remove(it) }

        // Stage E — one pipelined write for everything that survived. (Timing
        // is booked by the layers below: the projection decorator splits it
        // into write / proj.fetch / proj.write.)
        index.putAll(toPut.values.map { it.toDoc() })
        alive().forEach { i -> outcome[i] = IEventStore.InsertOutcome.Accepted }
        return outcome.map { it ?: IEventStore.InsertOutcome.Rejected("insert failed") }
    }

    // ---- queries ------------------------------------------------------------

    override suspend fun <T : Event> query(filter: Filter): List<T> = query(listOf(filter))

    @Suppress("UNCHECKED_CAST")
    override suspend fun <T : Event> query(filters: List<Filter>): List<T> {
        val observer = coroutineContext[ObserverContext]?.pubkey
        val queries = restoreSearches(filters).mapNotNull { it.toEventQuery()?.copy(notExpiredAt = nowSecs(), observer = observer) }
        val docs = queries.flatMap { index.search(it) }.distinctBy { it.id }
        // NIP-50: a searching query's results stay in the engine's RELEVANCE
        // order "instead of the usual created_at ordering" — re-sorting here
        // would undo the rank profile. Plain filters keep NIP-01 recency.
        val ranked = queries.any { it.search != null || it.ranking != null }
        val ordered = if (ranked) docs else docs.sortedWith(NEWEST_FIRST)
        return ordered.mapNotNull { Event.fromJsonOrNull(it.toEventJson()) } as List<T>
    }

    override suspend fun <T : Event> query(
        filter: Filter,
        onEach: (T) -> Unit,
    ) = query<T>(filter).forEach(onEach)

    override suspend fun <T : Event> query(
        filters: List<Filter>,
        onEach: (T) -> Unit,
    ) = query<T>(filters).forEach(onEach)

    override suspend fun count(filter: Filter): Int = restoreSearches(listOf(filter)).single().toEventQuery()?.let { index.count(it.copy(notExpiredAt = nowSecs())) } ?: 0

    override suspend fun count(filters: List<Filter>): Int = if (filters.size == 1) count(filters[0]) else query<Event>(filters).size

    /**
     * Undo Quartz's relay-side NIP-50 extension stripping: `LiveEventStore`
     * strips `key:value` tokens from every REQ's search before the store sees
     * it — the right default for stores that would match them as text, but
     * THIS store honors `sort:`/`filter:rank:`/`include:spam`. The relay
     * backend carries the pre-strip filters in [OriginalFilters] (same list,
     * same order — only `search` differs), and each filter's original search
     * string is restored before mapping. Direct callers (no context element)
     * are untouched.
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
     * (created_at, id) pairs straight off the docs — no Event materialization,
     * and no result cap: plain filters walk the corpus through the engine's
     * visit ([com.vitorpamplona.sot.vespa.EventIndex.visitIds]), so a
     * negentropy session (or a sync reconcile diff) sees the COMPLETE match
     * set even when it dwarfs the search page limit. Searching or limit'd
     * filters keep the search path — their semantics live there.
     */
    override suspend fun snapshotIdsForNegentropy(
        filters: List<Filter>,
        maxEntries: Int?,
    ): List<IdAndTime> {
        val all = ArrayList<IdAndTime>()
        for (q in filters.mapNotNull { it.toEventQuery() }) {
            if (q.search == null && q.limit == null) {
                index.visitIds(q) { page -> page.forEach { all += IdAndTime(it.createdAt, it.id) } }
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
     * Re-derive the search fields for every stored event. Which kinds are
     * searchable — and how [SearchExtractors] decomposes them — is baked into
     * this build, so docs indexed under old code can be stale (or missing
     * from search) until this runs; it also clears fields for kinds that LOST
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

        // Ids/authors/d-tags per check query — well under the engine's page cap.
        const val CHECK_CHUNK = 500

        // (author, d) pairs per addressable version-query: the recall is a
        // cross product, and a dense corpus (~50 services scoring the same
        // subjects) must stay under the engine's 10k search page.
        const val ADDR_CHUNK = 100

        // A tombstone/vanish guard set this big may have been page-capped by
        // the engine; those owners fall back to the exact per-event probes.
        const val GUARD_PAGE = 10_000
        val NEWEST_FIRST = compareByDescending(EventDoc::createdAt).thenBy(EventDoc::id)
    }
}

/**
 * Quartz Filter -> the engine's plain [EventQuery]. Null when the filter can
 * never match: NIP-01 semantics for a present-but-EMPTY list ("the event's
 * value must be in the list" — of nothing). Absent (null) lists mean no
 * constraint, which is EventQuery's empty default.
 *
 * NIP-50 extensions are relay hints, not text — Quartz's parser splits them
 * off (and, unlike a naive key:value regex, keeps `scheme://…` tokens as
 * terms). The honored set maps onto Brainstorm's search API:
 *
 *  - `sort:rank[:desc]` / `sort:rank:asc` / `sort:followers` / `sort:text`
 *    pick the rank profile; with no terms that's a trust-ordered match-all.
 *  - `filter:rank:gte:N` / `filter:rank:gt:N` set the observer trust floor
 *    (rank_filtered when no sort chose a profile — text order, gated).
 *  - `include:spam` switches OFF the default trust floor: every ranked query
 *    is otherwise gated at [DEFAULT_MIN_RANK] (Brainstorm's onlyRanked
 *    default — include:spam is its NIP-50 inverse). Plain filter REQs (no
 *    terms, no sort) are never gated: that recall is NIP-01's, not search's.
 *
 * Unknown extensions stay ignored; an all-extensions query becomes
 * unconstrained (null terms), not match-nothing.
 */
internal fun Filter.toEventQuery(): EventQuery? {
    if (ids?.isEmpty() == true || authors?.isEmpty() == true || kinds?.isEmpty() == true) return null
    if (tags?.values?.any { it.isEmpty() } == true || tagsAll?.values?.any { it.isEmpty() } == true) return null
    val parsed = SearchQuery.parse(search)
    val terms = parsed.terms.ifEmpty { null }
    val sort = parsed.extensions["sort"]?.let(::rankProfileOf)
    val floor = parsed.extensions["filter"]?.let(::rankFloorOf)
    val ranked = terms != null || sort != null
    return EventQuery(
        ids = ids.orEmpty(),
        kinds = kinds.orEmpty(),
        authors = authors.orEmpty(),
        tags = tags.orEmpty(),
        tagsAll = tagsAll.orEmpty(),
        since = since,
        until = until,
        limit = limit,
        search = terms,
        ranking = sort ?: floor?.let { EventYql.RANK_FILTERED },
        minRank = floor ?: if (ranked && !parsed.includeSpam) DEFAULT_MIN_RANK else null,
    )
}

/**
 * The default observer trust floor for search (Brainstorm passes min_rank=2
 * on the 0..100 rank scale): hits whose author the observer's provider
 * doesn't rank are spam-filtered out unless the query says `include:spam`.
 */
const val DEFAULT_MIN_RANK = 2.0

/** `sort:` value -> rank profile; null (ignored) for values we don't recognize. */
private fun rankProfileOf(value: String): String? =
    when (value) {
        "rank", "rank:desc" -> EventYql.RANK_DESC
        "rank:asc" -> EventYql.RANK_ASC
        "followers" -> EventYql.RANK_FOLLOWERS
        "text" -> EventYql.RANK_TEXT
        else -> null
    }

/** `filter:` value (`rank:gte:N` / `rank:gt:N`) -> the min_rank floor; null when unrecognized. */
private fun rankFloorOf(value: String): Double? {
    val parts = value.split(':')
    if (parts.size != 3 || parts[0] != "rank") return null
    val n = parts[2].toDoubleOrNull() ?: return null
    return when (parts[1]) {
        "gte" -> n

        // Scores are integers (0..100): strictly-greater = the next rank up.
        "gt" -> n + 1.0

        else -> null
    }
}

/**
 * The event's exact stored form plus the two derived fields: [EventDoc.owner]
 * (gift-wrap recipient or author) and [EventDoc.search] (the kind-specific
 * decomposition from [SearchExtractors] — SQLite's FTS row, tiered).
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
        owner = owner(),
        search = SearchExtractors.extract(this),
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
