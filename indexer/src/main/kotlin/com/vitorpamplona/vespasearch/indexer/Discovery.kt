package com.vitorpamplona.vespasearch.indexer

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
    suspend fun crawl(seeds: List<String>, maxRounds: Int, maxRelays: Int): Set<String> {
        val pool = LinkedHashSet<String>(state.relayPool)
        seeds.forEach { s -> RelayUrlNormalizer.normalizeOrNull(s)?.let { pool.add(it.url) } }

        var frontier = pool.toList()
        for (round in 1..maxRounds) {
            log("=== discovery round $round: sync 10002 from ${frontier.size} relays (pool ${pool.size}) ===")
            for (r in frontier) {
                if (pool.size > maxRelays) break
                syncer.sync(r, Filter(kinds = listOf(AdvertisedRelayListEvent.KIND)))
            }
            val advertised =
                store.query<AdvertisedRelayListEvent>(Filter(kinds = listOf(AdvertisedRelayListEvent.KIND)))
                    .flatMap { it.relaysNorm() }
                    .map { it.url }
                    .toSet()
            val fresh = advertised.filter { it !in pool }.take((maxRelays - pool.size).coerceAtLeast(0))
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
}
