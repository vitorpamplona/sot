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

import com.vitorpamplona.quartz.nip01Core.relay.filters.Filter
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.NormalizedRelayUrl
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.displayUrl
import com.vitorpamplona.quartz.nip01Core.store.ObservableEventStore
import com.vitorpamplona.quartz.nip65RelayList.AdvertisedRelayListEvent

/**
 * Bounded NIP-65 outbox crawl: sync kind-10002 relay lists, then expand the
 * relay pool with the relays those lists advertise, round by round, until no
 * new relays appear or [maxRounds]/[maxRelays] is hit. The pool is persisted in
 * [SyncState.relayPool] so periodic re-runs start from the known set instead of
 * rediscovering from scratch.
 */
class Discovery(
    private val syncer: RelaySyncer,
    private val store: ObservableEventStore,
    private val state: SyncState,
    private val log: (String) -> Unit,
    private val progress: SyncProgress = SyncProgress(log = { }),
) {
    suspend fun crawl(
        seeds: List<NormalizedRelayUrl>,
        maxRounds: Int,
        maxRelays: Int,
        concurrency: Int,
    ): Set<NormalizedRelayUrl> {
        val pool = LinkedHashSet<NormalizedRelayUrl>()
        pool.addAll(state.relayPool)
        pool.addAll(seeds)

        var frontier = pool.toList()
        for (round in 1..maxRounds) {
            log("=== discovery round $round: sync 10002 from ${frontier.size} relays (pool ${pool.size}, $concurrency in parallel) ===")
            progress.startPhase("discovery r$round", frontier.size)
            forEachParallel(frontier, concurrency) { r ->
                // Capped: discovery only needs a big-enough sample of relay lists to find
                // the popular relays — some aggregators hold millions of 10002s.
                val o = syncer.sync(r, Filter(kinds = listOf(AdvertisedRelayListEvent.KIND)), maxEvents = MAX_RELAY_LISTS_PER_RELAY)
                log("[10002 ${progress.itemDone()}/${frontier.size}] ${r.displayUrl()}: +${o.inserted}")
            }
            val fresh = advertisedRelays().filter { it !in pool }.take((maxRelays - pool.size).coerceAtLeast(0))
            if (fresh.isEmpty()) {
                log("=== discovery converged: ${pool.size} relays ===")
                break
            }
            pool.addAll(fresh)
            frontier = fresh
            log("[discovery] +${fresh.size} new relays (pool ${pool.size})")
        }

        state.relayPool.clear()
        state.relayPool.addAll(pool)
        return pool
    }

    /** Every relay advertised by the kind-10002 lists currently in the store. */
    private suspend fun advertisedRelays(): Set<NormalizedRelayUrl> =
        store
            .query<AdvertisedRelayListEvent>(Filter(kinds = listOf(AdvertisedRelayListEvent.KIND)))
            .flatMap { it.relaysNorm() }
            .toSet()

    private companion object {
        const val MAX_RELAY_LISTS_PER_RELAY = 25_000
    }
}
