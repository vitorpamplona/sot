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
package com.vitorpamplona.sot.indexer

import com.vitorpamplona.quartz.nip01Core.metadata.MetadataEvent
import com.vitorpamplona.quartz.nip01Core.relay.filters.Filter
import com.vitorpamplona.quartz.nip01Core.store.ObservableEventStore
import com.vitorpamplona.quartz.nip85TrustedAssertions.list.TrustProviderListEvent
import com.vitorpamplona.quartz.nip85TrustedAssertions.list.tags.ProviderTypes
import com.vitorpamplona.quartz.nip85TrustedAssertions.users.ContactCardEvent
import com.vitorpamplona.sot.vespa.VespaClient
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext
import java.util.concurrent.CompletableFuture

/**
 * Anti-entropy between the event store (the source of truth) and the Vespa
 * index. The projection is fire-and-forget — a Vespa outage or crash can drop
 * writes — so this walks both sides and reports (or, with [repair], fixes)
 * every difference. Two streaming passes, O(one page) memory plus 8 bytes per
 * indexed doc:
 *
 *  1. INDEX -> STORE: visit every Vespa doc and compare its provenance ids
 *     (`event_id`, `score_event_ids{observer}`) against what the store says
 *     they should be. Matching source ids imply matching content — both were
 *     written by the same update. Catches stale/blanked profiles, superseded
 *     or retracted scores, and score cells whose events are gone.
 *  2. STORE -> INDEX: stream the store's kind:0s and 30382s and catch pubkeys
 *     the index has NO doc for at all (pass 1 never saw them, so every write
 *     for them was lost).
 *
 * Repairs go through the same [VespaClient] writes the live projection uses.
 * Pass 2's membership check keeps a 64-bit fingerprint per visited pubkey
 * (the first 16 hex chars) instead of the full key; a collision could hide a
 * missing doc, which at ~10M docs has odds around 10^-6 — and only until the
 * doc's next real update.
 */
class IndexVerifier(
    private val store: ObservableEventStore,
    private val vespa: VespaClient,
    private val repair: Boolean,
    private val log: (String) -> Unit,
) {
    class Report {
        var docs = 0
        var staleProfiles = 0
        var extraProfiles = 0
        var staleScores = 0
        var extraScores = 0
        var missingWrites = 0
        var repairs = 0

        val diffs get() = staleProfiles + extraProfiles + staleScores + extraScores + missingWrites

        fun summary(): String =
            "docs=$docs diffs=$diffs (stale-profiles=$staleProfiles extra-profiles=$extraProfiles " +
                "stale-scores=$staleScores extra-scores=$extraScores missing-writes=$missingWrites) repairs=$repairs"
    }

    // service key (30382 signer) -> observer pubkey (10040 author), like the projection's map.
    private val serviceToObserver = HashMap<String, String>()

    private val report = Report()
    private val inflight = ArrayList<CompletableFuture<Unit>>()

    // Pubkey fingerprints of every doc pass 1 visited, for pass 2's membership check.
    private var seen = LongArray(1 shl 16)
    private var seenCount = 0

    suspend fun verify(): Report {
        loadObservers()

        var continuation: String? = null
        var pages = 0
        do {
            val page = withContext(Dispatchers.IO) { vespa.visitDocs(continuation) }
            for (doc in page.docs) {
                remember(doc.pubkey)
                checkDoc(doc)
            }
            report.docs += page.docs.size
            continuation = page.continuation
            if (++pages % 50 == 0) log("  ... verified ${report.docs} docs, ${report.diffs} diff(s) so far")
            awaitRepairs(onlyWhenPiledUp = true)
        } while (continuation != null)
        log("  index walk done: ${report.docs} docs, ${report.diffs} diff(s)")

        seen = seen.copyOf(seenCount).also { it.sort() }
        store.query<MetadataEvent>(Filter(kinds = listOf(MetadataEvent.KIND))) { ev ->
            if (!visited(ev.pubKey)) {
                report.missingWrites++
                ev.toProfile()?.let { p -> fix { vespa.upsertProfile(p) } }
            }
        }
        store.query<ContactCardEvent>(Filter(kinds = listOf(ContactCardEvent.KIND))) { ev ->
            // aboutUser() is the d-tag: never null, but "" when the tag is missing.
            val subject = ev.aboutUser()
            if (subject.isEmpty() || visited(subject)) return@query
            val observer = serviceToObserver[ev.pubKey] ?: return@query
            val rank = ev.rank() ?: return@query
            report.missingWrites++
            fix { vespa.upsertScore(subject, observer, rank, ev.id) }
        }

        awaitRepairs(onlyWhenPiledUp = false)
        return report
    }

    /** Compare one indexed doc's provenance against the store's current truth. */
    private suspend fun checkDoc(doc: VespaClient.IndexedDoc) {
        val kind0 = store.query<MetadataEvent>(Filter(kinds = listOf(MetadataEvent.KIND), authors = listOf(doc.pubkey), limit = 1)).firstOrNull()
        if (doc.eventId.orEmpty() != kind0?.id.orEmpty()) {
            if (kind0 == null) {
                // Profile fields with no kind:0 behind them (deleted or vanished).
                report.extraProfiles++
                fix { vespa.blankProfile(doc.pubkey) }
            } else {
                report.staleProfiles++
                kind0.toProfile()?.let { p -> fix { vespa.upsertProfile(p) } }
            }
        }

        // The newest 30382 per observer is the whole truth for that cell.
        val cards = store.query<ContactCardEvent>(Filter(kinds = listOf(ContactCardEvent.KIND), tags = mapOf("d" to listOf(doc.pubkey))))
        val expected = HashMap<String, ContactCardEvent>()
        for (card in cards.sortedBy { it.createdAt }) {
            val observer = serviceToObserver[card.pubKey] ?: continue
            expected[observer] = card
        }
        for ((observer, card) in expected) {
            val rank = card.rank()
            val current = doc.scoreEventIds[observer]
            if (rank == null) {
                if (current != null) {
                    report.extraScores++
                    fix { vespa.removeScore(doc.pubkey, observer) }
                }
            } else if (current != card.id) {
                report.staleScores++
                fix { vespa.upsertScore(doc.pubkey, observer, rank, card.id) }
            }
        }
        for (observer in doc.scoreEventIds.keys) {
            if (observer !in expected) {
                report.extraScores++
                fix { vespa.removeScore(doc.pubkey, observer) }
            }
        }
    }

    private fun fix(op: () -> CompletableFuture<Unit>) {
        if (!repair) return
        report.repairs++
        inflight.add(op().whenComplete { _, e -> if (e != null) log("  ! repair failed: ${e.message}") })
    }

    /** Bound the in-flight repair futures; the final call (not [onlyWhenPiledUp]) drains them all. */
    private fun awaitRepairs(onlyWhenPiledUp: Boolean) {
        if (onlyWhenPiledUp && inflight.size < MAX_INFLIGHT_REPAIRS) return
        inflight.forEach { runCatching { it.join() } }
        inflight.clear()
    }

    private suspend fun loadObservers() {
        store.query<TrustProviderListEvent>(Filter(kinds = listOf(TrustProviderListEvent.KIND))) { list ->
            list
                .serviceProviders()
                .filter { it.service == ProviderTypes.rank }
                .forEach { serviceToObserver[it.pubkey] = list.pubKey }
        }
    }

    private fun remember(pubkey: String) {
        if (seenCount == seen.size) seen = seen.copyOf(seen.size * 2)
        seen[seenCount++] = fingerprint(pubkey)
    }

    private fun visited(pubkey: String) = java.util.Arrays.binarySearch(seen, fingerprint(pubkey)) >= 0

    private fun fingerprint(pubkey: String): Long = pubkey.take(16).toULongOrNull(16)?.toLong() ?: 0L

    private companion object {
        const val MAX_INFLIGHT_REPAIRS = 10_000
    }
}
