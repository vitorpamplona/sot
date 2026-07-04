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
package com.vitorpamplona.sot.profile

import com.vitorpamplona.quartz.nip01Core.core.Event
import com.vitorpamplona.quartz.nip85TrustedAssertions.list.TrustProviderListEvent
import com.vitorpamplona.quartz.nip85TrustedAssertions.list.tags.ProviderTypes
import com.vitorpamplona.quartz.nip85TrustedAssertions.users.ContactCardEvent
import com.vitorpamplona.sot.vespa.DocRef
import com.vitorpamplona.sot.vespa.EventDoc
import com.vitorpamplona.sot.vespa.EventIndex
import com.vitorpamplona.sot.vespa.EventQuery
import com.vitorpamplona.sot.vespa.ProfileDoc
import com.vitorpamplona.sot.vespa.ProfileIndex
import com.vitorpamplona.sot.vespa.QUERY_FANOUT
import com.vitorpamplona.sot.vespa.mapBounded

/**
 * Maintains the `profile` parent documents — the per-pubkey trust tensors the
 * schema imports into every event's ranking — as an [EventIndex] DECORATOR:
 * wrap the index the store writes through, and every mutation that touches
 * trust data triggers a recompute.
 *
 * Observing the index (not the events) is the whole trick: the store's
 * semantic machinery — supersession, kind-5, vanish, sweeps, admin deletes —
 * all funnel into [put]/[remove] calls here, so every deletion style updates
 * the tensors with ZERO deletion-specific code. This replaces v1's
 * change-feed projection and its provenance maps, id lookups, and
 * million-cell sweeps.
 *
 * Recompute, never cell surgery: a change re-derives the SUBJECT's whole
 * [ProfileDoc] from the stored kind-30382s about them —
 *
 *   subject's 30382s (d = subject) -> signer is a SERVICE key
 *   -> observer = the kind-10040 author whose `30382:rank` entry lists that
 *      service key (NIP-85: cells are keyed by the OBSERVER, never the signer)
 *   -> quality_scores{observer} = rank tag, follower_counts{observer} =
 *      followers tag; a version without a rank tag contributes nothing
 *      (the provider retracted the score).
 *
 * Idempotent and self-healing; no cells left -> the parent doc is removed. A
 * 10040 change (new provider, switched provider, vanished observer)
 * recomputes every subject its service keys had scored, so late-arriving or
 * superseded provider lists re-attribute stored scores automatically —
 * v1's "unresolved" events are picked up instead of dropped.
 *
 * Recomputes run inline with the store's single-writer insert, so ranking is
 * read-your-writes consistent with the event corpus. [rebuildAll] re-derives
 * everything (bootstrap after enabling the projection on an existing index).
 */
class TrustProjection(
    private val inner: EventIndex,
    private val profiles: ProfileIndex,
) : EventIndex {
    override suspend fun get(id: String): EventDoc? = inner.get(id)

    override suspend fun search(query: EventQuery): List<EventDoc> = inner.search(query)

    override suspend fun visitIds(
        query: EventQuery,
        withDTag: Boolean,
        onPage: suspend (List<DocRef>) -> Unit,
    ) = inner.visitIds(query, withDTag, onPage)

    override suspend fun count(query: EventQuery): Int = inner.count(query)

    override fun close() {
        inner.close()
        profiles.close()
    }

    override suspend fun put(doc: EventDoc) {
        inner.put(doc)
        react(doc)
    }

    /**
     * The bulk path: one provider-map read for the whole batch, the touched
     * subjects' score docs fetched back in CHUNKED queries (hundreds of
     * subjects per round trip, not one), every parent derived locally, and
     * the results written through one pipelined [ProfileIndex.putAll] — a
     * million-score sync costs a handful of engine calls per chunk instead
     * of two per subject.
     */
    override suspend fun putAll(docs: List<EventDoc>) {
        inner.putAll(docs)
        // Provider lists first: they change the map the scores attribute through.
        docs.filter { it.kind == TrustProviderListEvent.KIND }.forEach { recomputeSubjectsOf(it) }
        val subjects = docs.filter { it.kind == ContactCardEvent.KIND }.mapNotNull { subjectOf(it) }.distinct()
        if (subjects.isEmpty()) return
        // No parent-doc REMOVE on the insert path: adding cards can only grow
        // a derivation, so an empty one was empty before this batch too — the
        // doc doesn't exist. (A corpus signed by unmapped providers would
        // otherwise flood the engine with no-op deletes.) Removal flows
        // through the single-doc react path (deletions).
        recomputeBatch(subjects, providerMap(), removeEmpties = false)
    }

    /**
     * The batched recompute behind [putAll], [recomputeSubjectsOf] and
     * [rebuildAll]: the touched subjects' score docs fetched back in CHUNKED,
     * concurrency-BOUNDED queries (hundreds of subjects per round trip, a few
     * round trips in flight — unbounded fan-out measurably times the engine
     * out), every parent derived locally, and the results written through one
     * pipelined [ProfileIndex.putAll].
     */
    private suspend fun recomputeBatch(
        subjects: List<String>,
        serviceToObserver: Map<String, String>,
        removeEmpties: Boolean,
    ) {
        val bySubject = HashMap<String, MutableList<EventDoc>>(subjects.size * 2)
        val wanted = subjects.toHashSet()
        subjects
            .chunked(FETCH_CHUNK)
            .mapBounded(QUERY_FANOUT) { chunk -> inner.search(EventQuery(kinds = listOf(ContactCardEvent.KIND), tags = mapOf("d" to chunk))) }
            .forEach { docs ->
                docs.forEach { doc ->
                    subjectOf(doc)?.takeIf { it in wanted }?.let { bySubject.getOrPut(it) { mutableListOf() } += doc }
                }
            }
        val puts = ArrayList<ProfileDoc>(subjects.size)
        val removes = ArrayList<String>()
        for (subject in subjects) {
            val profile = derive(subject, bySubject[subject].orEmpty(), serviceToObserver)
            if (!profile.isEmpty()) {
                puts += profile
            } else if (removeEmpties) {
                removes += subject
            }
        }
        profiles.putAll(puts)
        removes.mapBounded(QUERY_FANOUT) { profiles.remove(it) }
    }

    override suspend fun remove(id: String) {
        // The doomed doc says what the removal invalidates — read before deleting.
        val doc = inner.get(id)
        inner.remove(id)
        doc?.let { react(it) }
    }

    private suspend fun react(doc: EventDoc) {
        when (doc.kind) {
            ContactCardEvent.KIND -> subjectOf(doc)?.let { recompute(it) }
            TrustProviderListEvent.KIND -> recomputeSubjectsOf(doc)
        }
    }

    /** Re-derive [subject]'s whole parent doc from the stored 30382s about them. */
    suspend fun recompute(subject: String) = recompute(subject, providerMap())

    private suspend fun recompute(
        subject: String,
        serviceToObserver: Map<String, String>,
    ) {
        val docs = inner.search(EventQuery(kinds = listOf(ContactCardEvent.KIND), tags = mapOf("d" to listOf(subject))))
        val profile = derive(subject, docs, serviceToObserver)
        if (profile.isEmpty()) profiles.remove(subject) else profiles.put(profile)
    }

    /** [subject]'s parent doc from its score docs — pure derivation, no I/O. */
    private fun derive(
        subject: String,
        docs: List<EventDoc>,
        serviceToObserver: Map<String, String>,
    ): ProfileDoc {
        val quality = LinkedHashMap<String, Int>()
        val followers = LinkedHashMap<String, Double>()
        for (doc in docs) {
            val card = Event.fromJsonOrNull(doc.toEventJson()) as? ContactCardEvent ?: continue
            val observer = serviceToObserver[card.pubKey] ?: continue
            card.rank()?.let { quality[observer] = it }
            card.followerCount()?.let { followers[observer] = it.toDouble() }
        }
        return ProfileDoc(subject, quality, followers)
    }

    /** service key -> observer, from every stored 10040's `30382:rank` entries. */
    private suspend fun providerMap(): Map<String, String> =
        inner
            .search(EventQuery(kinds = listOf(TrustProviderListEvent.KIND)))
            .mapNotNull { Event.fromJsonOrNull(it.toEventJson()) as? TrustProviderListEvent }
            .flatMap { list ->
                list
                    .serviceProviders()
                    .filter { it.service == ProviderTypes.rank }
                    .map { it.pubkey to list.pubKey }
            }.toMap()

    /**
     * A 10040 appeared or disappeared: every subject its rank services have
     * scored needs re-attribution under the new provider map. The subjects
     * are enumerated through the engine's VISIT walk (d tags projected), not
     * a search — a provider with millions of stored scores is exactly where
     * a 10k search page would silently miss most of them — and re-derived in
     * batches, empties removed (a re-attribution can empty a parent).
     */
    private suspend fun recomputeSubjectsOf(listDoc: EventDoc) {
        val list = Event.fromJsonOrNull(listDoc.toEventJson()) as? TrustProviderListEvent ?: return
        val services =
            list
                .serviceProviders()
                .filter { it.service == ProviderTypes.rank }
                .map { it.pubkey }
        if (services.isEmpty()) return
        recomputeWalk(EventQuery(kinds = listOf(ContactCardEvent.KIND), authors = services))
    }

    /** Re-derive every parent doc from scratch (bootstrap over an existing index). */
    suspend fun rebuildAll() = recomputeWalk(EventQuery(kinds = listOf(ContactCardEvent.KIND)))

    /** Visit every score doc matching [query], collect the subjects, re-derive them in batches. */
    private suspend fun recomputeWalk(query: EventQuery) {
        val subjects = LinkedHashSet<String>()
        inner.visitIds(query, withDTag = true) { page -> page.forEach { ref -> ref.dTag?.let(subjects::add) } }
        if (subjects.isEmpty()) return
        val map = providerMap()
        subjects.toList().chunked(RECOMPUTE_BATCH).forEach { batch -> recomputeBatch(batch, map, removeEmpties = true) }
    }

    /** The 30382's d tag is the SUBJECT the score is about. */
    private fun subjectOf(doc: EventDoc): String? =
        doc.tags
            .firstOrNull { it.size >= 2 && it[0] == "d" }
            ?.get(1)
            ?.takeIf { it.isNotEmpty() }

    private companion object {
        // Subjects per batched score-fetch query. Sized for DENSE subjects: a
        // real NIP-85 corpus scores each subject from dozens of service keys
        // (~50 observed), so 100 subjects already recall ~5k docs — a bigger
        // chunk would cross the engine's 10k search page and silently truncate
        // the derivation.
        const val FETCH_CHUNK = 100

        // Subjects per recompute round in a full walk (memory-bounded batches).
        const val RECOMPUTE_BATCH = 20_000
    }
}
