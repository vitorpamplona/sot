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

import com.vitorpamplona.quartz.eventstore.store.NostrEventStore
import com.vitorpamplona.quartz.nip01Core.core.HexKey
import com.vitorpamplona.quartz.nip01Core.relay.filters.Filter
import com.vitorpamplona.quartz.nip65RelayList.AdvertisedRelayListEvent
import com.vitorpamplona.quartz.nip85TrustedAssertions.list.TrustProviderListEvent
import com.vitorpamplona.quartz.nip85TrustedAssertions.users.ContactCardEvent

/**
 * A snapshot of how completely ONE observer's network is indexed — the numbers
 * `sot status --observer` and the live pass line report. Every field is a count
 * derived from server-side groupings intersected in memory over the observer's
 * roster; nothing is stored per-observer.
 *
 * Completion is UNIFORM across the roster: an author with no discoverable 10002
 * is fetched from the most popular relays AS IF that were their outbox (see
 * BlendedPass.fallbackRelays), so "fully synced" means the same thing for
 * everyone — we reconciled their content from their outbox, real or the
 * popular-relay proxy. Whether they advertise their own 10002 is kept only as a
 * diagnostic ([withOwnOutbox]); it does not split the completion bar.
 */
data class ObserverCoverage(
    val observer: HexKey,
    /** Rank service keys this observer's kind-10040 names (0 = not an observer we can root). */
    val providers: Int,
    /** Distinct scored subjects — the denominator, the people we must index. */
    val rosterSize: Int,
    /** Diagnostic: roster members that advertise their own kind-10002 (the rest ride the popular-relay proxy). */
    val withOwnOutbox: Int,
    /** Roster members reconciled cleanly at least once, from their outbox OR the popular-relay proxy. */
    val synced: Int,
    /** Pending members never yet routed for content (no sync attempt) — a strict slice of [pending]. */
    val unreached: Int,
) {
    /** Roster members not yet synced — the backlog that drives the ETA. */
    val pending: Int get() = (rosterSize - synced).coerceAtLeast(0)

    /** Pending members we HAVE attempted (routed at least once) but not yet completed; [pending] = attempted + [unreached]. */
    val attempted: Int get() = (pending - unreached).coerceAtLeast(0)

    /** 0..1 fraction of the whole roster that is fully synced (the completion bar). */
    val syncedFraction: Double get() = if (rosterSize == 0) 0.0 else synced.toDouble() / rosterSize
}

/**
 * Compute [observer]'s coverage. The roster is the distinct `d`-tag subjects of
 * every kind-30382 signed by the service keys the observer's kind-10040 names;
 * the numerators are the roster intersected with the crawl ledger's synced set,
 * the ledger's outbox-checked set, and the stored-10002 authors. All are
 * whole-corpus groupings, so this is an on-demand report, not a per-tick call.
 */
suspend fun observerCoverage(
    observer: HexKey,
    store: NostrEventStore,
    crawl: CrawlIndex,
): ObserverCoverage {
    val providerKeys =
        store
            .query<TrustProviderListEvent>(Filter(kinds = listOf(TrustProviderListEvent.KIND), authors = listOf(observer)))
            .flatMap { it.rankProviders().map { p -> p.pubkey } }
            .toSet()
    if (providerKeys.isEmpty()) return ObserverCoverage(observer, 0, 0, 0, 0, 0)

    val roster = store.distinctDTags(Filter(kinds = listOf(ContactCardEvent.KIND), authors = providerKeys.toList()))
    if (roster.isEmpty()) return ObserverCoverage(observer, providerKeys.size, 0, 0, 0, 0)

    val syncedAll = crawl.syncedSince(1) // content_synced_at >= 1, i.e. ever reconciled (outbox OR proxy)
    val checkedAll = crawl.outboxCheckedSet() // routed for content at least once
    val outboxAll = store.distinctAuthors(Filter(kinds = listOf(AdvertisedRelayListEvent.KIND)))

    return ObserverCoverage(
        observer = observer,
        providers = providerKeys.size,
        rosterSize = roster.size,
        withOwnOutbox = roster.count { it in outboxAll },
        synced = roster.count { it in syncedAll },
        // A strict slice of pending: not synced AND never routed. Keeping it inside
        // "not synced" preserves pending = attempted + unreached even if a synced
        // author is missing its outbox_checked stamp (older ledger / write miss).
        unreached = roster.count { it !in syncedAll && it !in checkedAll },
    )
}
