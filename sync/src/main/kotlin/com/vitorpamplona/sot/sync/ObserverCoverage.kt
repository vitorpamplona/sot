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
import com.vitorpamplona.quartz.nip65RelayList.AdvertisedRelayListEvent
import com.vitorpamplona.quartz.nip85TrustedAssertions.list.TrustProviderListEvent
import com.vitorpamplona.quartz.nip85TrustedAssertions.users.ContactCardEvent
import com.vitorpamplona.sot.store.VespaEventStore
import com.vitorpamplona.sot.vespa.doc.CrawlIndex

/**
 * A snapshot of how completely ONE observer's network is indexed — the numbers
 * `sot status --observer` and the live pass line report. Every field is a count
 * derived from server-side groupings intersected in memory over the observer's
 * roster; nothing is stored per-observer.
 *
 * The roster splits by whether we could find the author's outbox, because the
 * two halves are measured differently: an OUTBOX-KNOWN author has a well-defined
 * "fully synced" (we reconciled their 10002 relays), while a NO-OUTBOX author
 * can only be measured by how many of their posts we scraped from the fallback
 * relays — "did we get everything" is unanswerable without their outbox.
 */
data class ObserverCoverage(
    val observer: HexKey,
    /** Rank service keys this observer's kind-10040 names (0 = not an observer we can root). */
    val providers: Int,
    /** Distinct scored subjects — the denominator, the people we must index. */
    val rosterSize: Int,
    /** Roster members we hold a kind-10002 for (routable to their own outbox). */
    val outboxKnown: Int,
    /** Roster members with NO stored 10002 (scraped from fallback relays only). */
    val noOutbox: Int,
    /** Roster members reconciled cleanly at least once (crawl ledger content_synced_at > 0). */
    val synced: Int,
    /** No-outbox roster members we hold at least one post for (the best-effort yardstick). */
    val postsForNoOutbox: Int,
) {
    /** Outbox-known members not yet synced — the backlog that drives the ETA. */
    val pending: Int get() = (outboxKnown - syncedWithOutbox).coerceAtLeast(0)

    /** Of [synced], how many are outbox-known (the rest were fallback-scraped no-outbox authors). */
    val syncedWithOutbox: Int get() = (synced - syncedNoOutbox).coerceAtLeast(0)

    // Filled during computation; no-outbox authors that nonetheless reached a clean fallback sync.
    internal var syncedNoOutbox: Int = 0

    /** 0..1 fraction of the OUTBOX-KNOWN roster that is fully synced (the honest completion bar). */
    val syncedFraction: Double get() = if (outboxKnown == 0) 0.0 else syncedWithOutbox.toDouble() / outboxKnown
}

/**
 * Compute [observer]'s coverage. The roster is the distinct `d`-tag subjects of
 * every kind-30382 signed by the service keys the observer's kind-10040 names;
 * the numerators are the roster intersected with the crawl ledger's synced set,
 * the stored-10002 authors, and the authors we hold content for. All three are
 * whole-corpus groupings, so this is an on-demand report, not a per-tick call.
 *
 * NOTE (v1): no-outbox is `roster − has-10002`, so during an in-progress load an
 * author whose 10002 simply hasn't been fetched yet counts as no-outbox until it
 * lands. The crawl doc's `outbox_checked_at` exists to tighten this later.
 */
suspend fun observerCoverage(
    observer: HexKey,
    store: VespaEventStore,
    crawl: CrawlIndex,
    contentKinds: List<Int> = IndexableKinds.kinds,
): ObserverCoverage {
    val providerKeys =
        store
            .query<TrustProviderListEvent>(Filter(kinds = listOf(TrustProviderListEvent.KIND), authors = listOf(observer)))
            .flatMap { it.rankProviders().map { p -> p.pubkey } }
            .toSet()
    if (providerKeys.isEmpty()) return ObserverCoverage(observer, 0, 0, 0, 0, 0, 0)

    val roster = store.distinctDTags(Filter(kinds = listOf(ContactCardEvent.KIND), authors = providerKeys.toList()))
    if (roster.isEmpty()) return ObserverCoverage(observer, providerKeys.size, 0, 0, 0, 0, 0)

    val syncedAll = crawl.syncedSince(1) // content_synced_at >= 1, i.e. ever reconciled
    val outboxAll = store.distinctAuthors(Filter(kinds = listOf(AdvertisedRelayListEvent.KIND)))
    val postsAll = store.distinctAuthors(Filter(kinds = contentKinds))

    val outboxKnown = roster.count { it in outboxAll }
    val noOutboxSet = roster.filterTo(HashSet()) { it !in outboxAll }
    val synced = roster.count { it in syncedAll }
    val syncedNoOutbox = noOutboxSet.count { it in syncedAll }
    val postsForNoOutbox = noOutboxSet.count { it in postsAll }

    return ObserverCoverage(
        observer = observer,
        providers = providerKeys.size,
        rosterSize = roster.size,
        outboxKnown = outboxKnown,
        noOutbox = noOutboxSet.size,
        synced = synced,
        postsForNoOutbox = postsForNoOutbox,
    ).also { it.syncedNoOutbox = syncedNoOutbox }
}
