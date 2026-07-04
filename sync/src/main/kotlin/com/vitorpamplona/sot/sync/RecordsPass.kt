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
package com.vitorpamplona.sot.sync

import com.vitorpamplona.quartz.nip01Core.core.HexKey
import com.vitorpamplona.quartz.nip01Core.relay.filters.Filter
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.NormalizedRelayUrl
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.displayUrl
import com.vitorpamplona.quartz.nip01Core.store.IEventStore
import com.vitorpamplona.quartz.nip09Deletions.DeletionEvent
import com.vitorpamplona.quartz.nip62RequestToVanish.RequestToVanishEvent
import com.vitorpamplona.quartz.nip65RelayList.AdvertisedRelayListEvent
import com.vitorpamplona.quartz.nip85TrustedAssertions.users.ContactCardEvent

/**
 * The RECORDS plane: the searchable content itself. Where the scores plane
 * ([BlendedPass]) syncs the web of trust (who trusts whom, and by how much),
 * this pass syncs the events people actually search for — notes, articles,
 * pictures, every [IndexableKinds] kind — for the authors that trust vouches
 * for. It runs AFTER the scores plane, because the scored authors are read
 * straight back out of the stored 30382s.
 *
 * Three ideas shape it:
 *
 *  - **Trust-ordered.** Authors sync highest-score first, so under a per-pass
 *    budget the index fills top-down: the most-trusted authors' content — the
 *    content that ranks at the top of search — lands first. Score is the SUBJECT
 *    priority: an author's max `rank()` over every observer that scored them, so
 *    one observer's niche-but-trusted author isn't buried by a popularity sum.
 *
 *  - **Score bands, batched by relay.** Authors are cut into fixed-size bands by
 *    descending score. Within a band, order no longer matters, so authors that
 *    share an outbox relay fold into ONE download: a single (relay × 500-author)
 *    sync pulls every [IndexableKinds] kind for all of them. Popular relays host
 *    hundreds of a band's authors in one filter.
 *
 *  - **Load-balanced for relay parallelism.** A relay caps around a few thousand
 *    events/s; the store ingests far more, so throughput is the number of relays
 *    streaming at once. The outbox model puts an author's events on ALL their
 *    write relays, so each author is fetched from just ONE — the least-loaded of
 *    theirs — turning the shared-relay overlap into the freedom to SPREAD load
 *    across many relays instead of hammering the few popular ones. Fetching an
 *    author from several relays would only re-download the same events.
 *
 * Deletion here is EXPLICIT (kinds 5 and 62 ride along), never absence: unlike
 * the scores plane, an author's outbox is one replica among many, so a hole in
 * one relay's set means nothing. This is the records-plane contract called out
 * in [BlendedPass.scoreUnit].
 */
internal class RecordsPass(
    private val syncer: RelaySyncer,
    private val store: IEventStore,
    private val opts: SyncOptions,
    private val progress: SyncProgress,
    private val log: (String) -> Unit,
    private val indexRelays: List<NormalizedRelayUrl>,
) {
    /** Every searchable kind, plus the two deletion kinds that erase them locally. */
    private val recordKinds = IndexableKinds.kinds + DeletionEvent.KIND + RequestToVanishEvent.KIND

    suspend fun run() {
        val authors = rankedAuthors()
        if (authors.isEmpty()) {
            log("[records] no scored authors yet - nothing to index")
            return
        }
        val bands = authors.chunked(opts.recordBandSize.coerceAtLeast(1))
        val budget = if (opts.maxRecordBands > 0) minOf(opts.maxRecordBands, bands.size) else bands.size
        log("[records] ${authors.size} scored author(s) in $budget/${bands.size} band(s) of ${opts.recordBandSize}, ${recordKinds.size} indexable kind(s)")
        bands.take(budget).forEachIndexed { i, band ->
            log("[records] band ${i + 1}/$budget: ${band.size} author(s)")
            syncBand(band)
        }
    }

    /** Distinct 30382 subjects, ordered by descending max score. Unscored subjects are skipped — trust is the priority. */
    private suspend fun rankedAuthors(): List<HexKey> {
        val best = HashMap<HexKey, Int>()
        store.query<ContactCardEvent>(Filter(kinds = listOf(ContactCardEvent.KIND))).forEach { card ->
            val subject = subjectOf(card) ?: return@forEach
            val rank = card.rank() ?: return@forEach
            best.merge(subject, rank) { a, b -> maxOf(a, b) }
        }
        return best.entries.sortedByDescending { it.value }.map { it.key }
    }

    private suspend fun syncBand(band: List<HexKey>) {
        resolveRelayLists(band)

        // Read each author's write relays back out of the store.
        val byAuthor =
            band
                .chunked(AUTHORS_PER_FILTER)
                .flatMap { batch -> store.query<AdvertisedRelayListEvent>(Filter(kinds = listOf(AdvertisedRelayListEvent.KIND), authors = batch)) }
                .associateBy { it.pubKey }

        // Assign every author to the LEAST-LOADED of their write relays (index
        // relays as the fallback for authors with no 10002), so the band's load
        // spreads across as many distinct relays as possible.
        val load = HashMap<NormalizedRelayUrl, Int>()
        val byRelay = HashMap<NormalizedRelayUrl, MutableList<HexKey>>()
        var stranded = 0
        for (author in band) {
            val relays = byAuthor[author]?.writeRelaysNorm().orEmpty().ifEmpty { indexRelays }
            val chosen = relays.minByOrNull { load[it] ?: 0 }
            if (chosen == null) {
                stranded++
                continue
            }
            load.merge(chosen, 1) { a, b -> a + b }
            byRelay.getOrPut(chosen) { mutableListOf() }.add(author)
        }
        if (stranded > 0) log("[records] $stranded author(s) with no discoverable outbox and no index relay - skipped this pass")

        // One sync per (relay × author batch); all relays stream in parallel to
        // saturate ingest — a slow relay never blocks the others.
        val units = byRelay.flatMap { (relay, authors) -> authors.chunked(AUTHORS_PER_FILTER).map { relay to it } }
        progress.addPhaseItems(units.size)
        forEachParallel(units, opts.concurrency) { (relay, batch) ->
            val o = syncer.sync(relay, Filter(kinds = recordKinds, authors = batch), maxEvents = opts.maxEvents)
            log("[records ${progress.itemDone()}/${progress.position().substringAfter('/')}] ${batch.size} author(s) @ ${relay.displayUrl()}: +${o.inserted}/${o.downloaded}")
        }
    }

    /** Discover the band's write relays: fetch their 10002s from the index relays into the store. */
    private suspend fun resolveRelayLists(band: List<HexKey>) {
        if (indexRelays.isEmpty()) return
        val units = indexRelays.flatMap { relay -> band.chunked(AUTHORS_PER_FILTER).map { relay to it } }
        forEachParallel(units, opts.concurrency) { (relay, batch) ->
            syncer.sync(relay, Filter(kinds = listOf(AdvertisedRelayListEvent.KIND), authors = batch), maxEvents = opts.maxEvents)
        }
    }

    /** The 30382's `d` tag is the SUBJECT the score is about (matches the trust projection). */
    private fun subjectOf(card: ContactCardEvent): HexKey? =
        card.tags
            .firstOrNull { it.size > 1 && it[0] == "d" }
            ?.get(1)
            ?.takeIf { it.isNotEmpty() }
}
