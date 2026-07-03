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
package com.vitorpamplona.sot.v2.profile

import com.vitorpamplona.quartz.nip01Core.core.Event
import com.vitorpamplona.quartz.nip85TrustedAssertions.list.TrustProviderListEvent
import com.vitorpamplona.quartz.nip85TrustedAssertions.list.tags.ProviderTypes
import com.vitorpamplona.quartz.nip85TrustedAssertions.users.ContactCardEvent
import com.vitorpamplona.sot.v2.vespa.EventDoc
import com.vitorpamplona.sot.v2.vespa.EventIndex
import com.vitorpamplona.sot.v2.vespa.EventQuery
import com.vitorpamplona.sot.v2.vespa.ProfileDoc
import com.vitorpamplona.sot.v2.vespa.ProfileIndex

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

    override suspend fun count(query: EventQuery): Int = inner.count(query)

    override fun close() {
        inner.close()
        profiles.close()
    }

    override suspend fun put(doc: EventDoc) {
        inner.put(doc)
        react(doc)
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
    suspend fun recompute(subject: String) {
        val serviceToObserver = providerMap()
        val quality = LinkedHashMap<String, Int>()
        val followers = LinkedHashMap<String, Double>()
        for (doc in inner.search(EventQuery(kinds = listOf(ContactCardEvent.KIND), tags = mapOf("d" to listOf(subject))))) {
            val card = Event.fromJsonOrNull(doc.toEventJson()) as? ContactCardEvent ?: continue
            val observer = serviceToObserver[card.pubKey] ?: continue
            card.rank()?.let { quality[observer] = it }
            card.followerCount()?.let { followers[observer] = it.toDouble() }
        }
        val profile = ProfileDoc(subject, quality, followers)
        if (profile.isEmpty()) profiles.remove(subject) else profiles.put(profile)
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
     * scored needs re-attribution under the new provider map.
     */
    private suspend fun recomputeSubjectsOf(listDoc: EventDoc) {
        val list = Event.fromJsonOrNull(listDoc.toEventJson()) as? TrustProviderListEvent ?: return
        val services =
            list
                .serviceProviders()
                .filter { it.service == ProviderTypes.rank }
                .map { it.pubkey }
        if (services.isEmpty()) return
        inner
            .search(EventQuery(kinds = listOf(ContactCardEvent.KIND), authors = services))
            .mapNotNull { subjectOf(it) }
            .distinct()
            .forEach { recompute(it) }
    }

    /** Re-derive every parent doc from scratch (bootstrap over an existing index). */
    suspend fun rebuildAll() {
        inner
            .search(EventQuery(kinds = listOf(ContactCardEvent.KIND)))
            .mapNotNull { subjectOf(it) }
            .distinct()
            .forEach { recompute(it) }
    }

    /** The 30382's d tag is the SUBJECT the score is about. */
    private fun subjectOf(doc: EventDoc): String? =
        doc.tags
            .firstOrNull { it.size >= 2 && it[0] == "d" }
            ?.get(1)
            ?.takeIf { it.isNotEmpty() }
}
