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
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.RelayUrlNormalizer
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
) {
    suspend fun crawl(
        seeds: List<String>,
        maxRounds: Int,
        maxRelays: Int,
    ): Set<String> {
        val pool = LinkedHashSet<String>(state.relayPool)
        seeds.forEach { s -> RelayUrlNormalizer.normalizeOrNull(s)?.let { pool.add(it.url) } }

        var frontier = pool.toList()
        for (round in 1..maxRounds) {
            log("=== discovery round $round: sync 10002 from ${frontier.size} relays (pool ${pool.size}) ===")
            for (r in frontier) {
                if (pool.size > maxRelays) break
                syncer.sync(r, Filter(kinds = listOf(AdvertisedRelayListEvent.KIND)))
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

    /** Every relay URL advertised by the kind-10002 lists currently in the store. */
    private suspend fun advertisedRelays(): Set<String> =
        store
            .query<AdvertisedRelayListEvent>(Filter(kinds = listOf(AdvertisedRelayListEvent.KIND)))
            .flatMap { it.relaysNorm() }
            .map { it.url }
            .toSet()
}
