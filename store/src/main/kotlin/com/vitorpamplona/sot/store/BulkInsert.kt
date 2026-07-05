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
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.NormalizedRelayUrl
import com.vitorpamplona.quartz.nip01Core.store.IEventStore
import com.vitorpamplona.quartz.nip01Core.tags.dTag.dTag
import com.vitorpamplona.quartz.nip09Deletions.DeletionEvent
import com.vitorpamplona.quartz.nip40Expiration.isExpired
import com.vitorpamplona.quartz.nip62RequestToVanish.RequestToVanishEvent
import com.vitorpamplona.sot.vespa.IngestStats
import com.vitorpamplona.sot.vespa.QUERY_FANOUT
import com.vitorpamplona.sot.vespa.SkipDerivedRecompute
import com.vitorpamplona.sot.vespa.client.EventIndex
import com.vitorpamplona.sot.vespa.doc.EventDoc
import com.vitorpamplona.sot.vespa.mapBounded
import com.vitorpamplona.sot.vespa.query.EventQuery
import kotlinx.coroutines.withContext

/** A SEMANTIC insert rejection (duplicate, replaced, or blocked). Transient engine failures are NOT this; they propagate. */
class RejectedException(
    message: String,
) : Exception(message)

/** The insert-rejection reasons, shared by the per-event and bulk paths so the two can never drift. */
internal object Rejections {
    const val EXPIRED = "blocked: Cannot insert an expired event"
    const val DUPLICATE = "duplicate: already have this event"
    const val DELETED = "blocked: a deletion event exists"
    const val VANISHED = "blocked: a request to vanish event exists"
    const val REPLACED = "replaced: a newer version exists"
    const val INSERT_FAILED = "insert failed"
}

/**
 * The bulk insert fast path for one run of plain events (no kind 5/62). It
 * enforces the same Nostr rules as the per-event [VespaEventStore] path, but
 * with BATCHED I/O. The per-event path costs 3–5 engine round trips per event,
 * which is useless against a million-event sync. Stages:
 *
 *  A. local checks (ephemeral accepted-not-stored, expired rejected, later
 *     copies of an id already in this run rejected as duplicates);
 *  B. one `id in (…)` duplicate query per [CHECK_CHUNK], fanned out bounded;
 *  C. per-owner tombstone/vanish guards (one query each; an owner with a guard
 *     set too large for one page falls back to the exact per-event [probe]);
 *  D. per-address supersession resolved IN RUN ORDER. Existing versions are
 *     fetched per (kind, author), and losers inside the run are
 *     Accepted-then-superseded exactly as sequential inserts would end up;
 *  E. one pipelined [EventIndex.putAll] of the survivors.
 *
 * [probe] runs the exact per-event deletion/vanish checks (throwing
 * [RejectedException] on a block) for the guard-page fallback in stage C.
 */
internal class BulkInsert(
    private val index: EventIndex,
    private val relay: NormalizedRelayUrl?,
    private val probe: suspend (Event) -> Unit,
) {
    /**
     * What [plan] resolved from its LOCK-FREE reads, handed to [commit] to
     * finish under the single writer lock. Stages A–C and the grouping are all
     * read-only work that never needed the lock; only the supersession read
     * (atomic with its own write) and the writes themselves stay inside it.
     */
    internal class Plan(
        val events: List<Event>,
        val outcome: Array<IEventStore.InsertOutcome?>,
        val toPut: LinkedHashMap<String, Event>,
        val groups: LinkedHashMap<Triple<Int, String, String?>, MutableList<Int>>,
    )

    /** Plan then commit in one call, for callers that already hold the writer lock across both. */
    suspend fun run(events: List<Event>): List<IEventStore.InsertOutcome> = commit(plan(events))

    /**
     * The LOCK-FREE half: stages A–C (local checks, dedup, tombstone/vanish
     * guards) plus the address grouping — reads that only observe the store,
     * never mutate it. Two plans racing here is fine; [commit] does the one
     * read that must be atomic with its write (supersession) under the lock.
     */
    suspend fun plan(events: List<Event>): Plan {
        val outcome = arrayOfNulls<IEventStore.InsertOutcome>(events.size)

        fun alive() = events.indices.filter { outcome[it] == null }

        // Stage A — no I/O: ephemeral accepted-not-stored, expired rejected,
        // later copies of an id already in this run rejected as duplicates.
        val seen = HashSet<String>()
        events.forEachIndexed { i, e ->
            when {
                e.kind.isEphemeral() -> outcome[i] = IEventStore.InsertOutcome.Accepted
                e.isExpired() -> outcome[i] = IEventStore.InsertOutcome.Rejected(Rejections.EXPIRED)
                !seen.add(e.id) -> outcome[i] = IEventStore.InsertOutcome.Rejected(Rejections.DUPLICATE)
            }
        }

        // Stage B — ids already stored. The chunk queries are independent
        // reads, so they fan out with BOUNDED concurrency. Serialized round
        // trips starve the batch, but unbounded fan-out measurably 504s the
        // engine's summary stage.
        val stored = HashSet<String>()
        IngestStats.timed("dedup") {
            alive()
                .map { events[it].id }
                .chunked(CHECK_CHUNK)
                .mapBounded(QUERY_FANOUT) { chunk -> index.search(EventQuery(ids = chunk)) }
                .forEach { docs -> docs.forEach { stored += it.id } }
        }
        alive().forEach { i -> if (events[i].id in stored) outcome[i] = IEventStore.InsertOutcome.Rejected(Rejections.DUPLICATE) }

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
                            probe(events[i])
                            null
                        } catch (e: RejectedException) {
                            // Semantic block only; a transient engine failure propagates.
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
                    outcome[i] = IEventStore.InsertOutcome.Rejected(Rejections.DELETED)
                } else if (e.createdAt <= vanishAt) {
                    outcome[i] = IEventStore.InsertOutcome.Rejected(Rejections.VANISHED)
                }
            }
        }

        // Stage D-setup (local): group survivors by replaceable address; plain
        // events go straight to toPut. The version READ + resolution runs in
        // commit(), under the lock, so it stays atomic with the write.
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
        return Plan(events, outcome, toPut, groups)
    }

    /**
     * The LOCKED half: the supersession read+resolve (which must see every
     * prior commit's writes, so it runs under the single writer lock) and the
     * pipelined writes. Kept as short as the semantics allow — an empty remove
     * or put set skips its round trip.
     */
    suspend fun commit(plan: Plan): List<IEventStore.InsertOutcome> {
        val events = plan.events
        val outcome = plan.outcome
        val toPut = plan.toPut
        val groups = plan.groups

        fun alive() = events.indices.filter { outcome[it] == null }

        // Stage D — supersession per replaceable address, resolved in run order.
        // Existing versions for every touched address, chunked. Replaceables are
        // fetched by (kind, authors…). Addressables are fetched by
        // (kind, author, d-tags…) via tag_index recall, then bucketed doc-side
        // (the d filter is exact there).
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
                // Addressables recall PER (kind, author), never across authors. A
                // multi-author (authors x d-tags) query is a CROSS PRODUCT. In a
                // dense corpus (dozens of service keys scoring the same subjects)
                // that recalls authors×ds real docs, which runs past the 10k
                // search page and silently misses existing versions. One author's
                // d-set is bounded.
                for ((ka, keys) in addressable.groupBy { it.first to it.second }) {
                    val (kind, author) = ka
                    keys.mapNotNull { it.third }.distinct().chunked(CHECK_CHUNK).forEach { ds ->
                        add(EventQuery(kinds = listOf(kind), authors = listOf(author), tags = mapOf("d" to ds)))
                    }
                }
            }
        IngestStats
            .timed("versions") {
                versionQueries.mapBounded(QUERY_FANOUT) { q -> index.search(q) }
            }.forEach { docs ->
                docs.forEach { doc ->
                    val d = if (doc.kind.isAddressable()) doc.dTagOrEmpty() else null
                    existing.getOrPut(Triple(doc.kind, doc.pubkey, d)) { mutableListOf() } += doc
                }
            }
        val removeFromStore = ArrayList<String>()
        // Ids of the trust-neutral supersessions (a 30382 replacing 30382s with
        // identical rank+followers): stored like any other, but the projection's
        // re-derive is skipped — the ranking tensor they feed can't have moved.
        val neutralRemoveIds = HashSet<String>()
        val neutralPutIds = HashSet<String>()
        for ((key, idxs) in groups) {
            val versions = existing[key].orEmpty()
            // The run competes against the store's best. Every stored version
            // strictly older than the final winner is swept.
            var bestDocId: String? = versions.maxWithOrNull(compareBy<EventDoc> { it.createdAt }.thenByDescending { it.id })?.id
            var bestAt = versions.maxOfOrNull { it.createdAt } ?: Long.MIN_VALUE
            var bestId = versions.filter { it.createdAt == bestAt }.minOfOrNull { it.id }
            var bestInRun: Int? = null
            val removedHere = ArrayList<String>()
            for (i in idxs) {
                val e = events[i]
                val lost = bestId != null && (bestAt > e.createdAt || (bestAt == e.createdAt && bestId!! < e.id))
                if (lost) {
                    outcome[i] = IEventStore.InsertOutcome.Rejected(Rejections.REPLACED)
                } else {
                    // The previous best is superseded. An in-run best stays
                    // Accepted but never lands; a stored best is removed.
                    bestInRun?.let { toPut.remove(events[it].id) }
                    bestDocId?.let { removedHere += it }
                    bestDocId = null
                    bestInRun = i
                    bestAt = e.createdAt
                    bestId = e.id
                    toPut[e.id] = e
                }
            }
            // Older stored versions beyond the single best also fall (drift repair).
            versions.forEach { doc -> if (doc.id != bestDocId && doc.id !in removedHere) removedHere += doc.id }
            removeFromStore += removedHere

            // A landed winner whose every swept version carries the same trust
            // tags moves no tensor cell — mark it so the writes below skip the
            // re-derive without changing the final state.
            val winner = bestInRun?.let { events[it] }
            if (winner != null && removedHere.isNotEmpty() &&
                isTrustNeutralSupersession(winner, versions.filter { it.id in removedHere })
            ) {
                neutralPutIds += winner.id
                neutralRemoveIds += removedHere
            }
        }
        val toRemove = removeFromStore.distinct()
        val putDocs = toPut.values.map { it.toDoc() }

        // Stage E — trust-neutral supersessions first: their event docs are
        // written (so any reactive re-derive below observes them), but the
        // recompute is skipped. Reactive mutations keep the remove-then-put
        // ordering. Each empty set skips its round trip — the common case for a
        // fresh corpus is all-reactive, first-seen, and removes nothing.
        val neutralRemove = toRemove.filter { it in neutralRemoveIds }
        val neutralPut = putDocs.filter { it.id in neutralPutIds }
        if (neutralRemove.isNotEmpty() || neutralPut.isNotEmpty()) {
            withContext(SkipDerivedRecompute()) {
                if (neutralRemove.isNotEmpty()) index.removeAll(neutralRemove)
                if (neutralPut.isNotEmpty()) index.putAll(neutralPut)
            }
        }
        val reactiveRemove = toRemove.filter { it !in neutralRemoveIds }
        val reactivePut = putDocs.filter { it.id !in neutralPutIds }
        if (reactiveRemove.isNotEmpty()) index.removeAll(reactiveRemove)
        if (reactivePut.isNotEmpty()) index.putAll(reactivePut)
        alive().forEach { i -> outcome[i] = IEventStore.InsertOutcome.Accepted }
        return outcome.map { it ?: IEventStore.InsertOutcome.Rejected(Rejections.INSERT_FAILED) }
    }

    private companion object {
        // Ids/authors/d-tags per check query — well under the engine's page cap.
        const val CHECK_CHUNK = 500

        // A guard set this big may have been page-capped by the engine; those
        // owners fall back to the exact per-event probes.
        const val GUARD_PAGE = 10_000
    }
}
