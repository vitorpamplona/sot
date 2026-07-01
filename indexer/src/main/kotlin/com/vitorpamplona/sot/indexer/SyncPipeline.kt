package com.vitorpamplona.sot.indexer

import com.vitorpamplona.quartz.nip01Core.metadata.MetadataEvent
import com.vitorpamplona.quartz.nip01Core.relay.client.NostrClient
import com.vitorpamplona.quartz.nip01Core.relay.filters.Filter
import com.vitorpamplona.quartz.nip01Core.store.ObservableEventStore
import com.vitorpamplona.quartz.nip09Deletions.DeletionEvent
import com.vitorpamplona.quartz.nip85TrustedAssertions.list.TrustProviderListEvent
import com.vitorpamplona.quartz.nip85TrustedAssertions.list.tags.ProviderTypes
import com.vitorpamplona.quartz.nip85TrustedAssertions.users.ContactCardEvent

/**
 * Nostr sync pipeline. Fetches events from relays into the store — the source of
 * truth — and nothing more; mapping the store into Vespa is a separate concern
 * (a projection subscribes to the store's change feed, wired up by the caller).
 * [RelaySyncer] prefers negentropy and falls back to
 * paginated `since` fetches, advancing per-relay cursors in [SyncState] so
 * periodic re-runs only pull the delta.
 *
 *   (optional) DISCOVER: bounded kind-10002 outbox crawl -> relay pool
 *   phase 1: 10040 / 5 / 0 from the relay set
 *   phase 2: resolve `30382:rank` providers from stored 10040s
 *   phase 3: each provider's 30382 from its relay hint
 *
 * Scores land keyed by the OBSERVER (10040 author), per NIP-85 Trusted Assertions.
 */
suspend fun runSync(
    client: NostrClient,
    store: ObservableEventStore,
    state: SyncState,
    statePath: String,
    seedRelays: List<String>,
    maxEvents: Int,
    log: (String) -> Unit,
    fetchTimeoutMs: Long = 30_000,
    maxProviders: Int = 0,
    syncProfiles: Boolean = true,
    discover: Boolean = false,
    maxRounds: Int = 3,
    maxRelays: Int = 200,
    syncScores: Boolean = true,
    verifyEvents: Boolean = true,
) {
    val syncer = RelaySyncer(client, store, state, log, idleTimeoutMs = fetchTimeoutMs, verifyEvents = verifyEvents)

    try {
    val relays =
        if (discover) {
            Discovery(syncer, store, state, log).crawl(seedRelays, maxRounds, maxRelays).toList()
        } else {
            seedRelays
        }

    log("=== phase 1: ${if (syncScores) "10040 / " else ""}5${if (syncProfiles) " / 0" else ""} from ${relays.size} relay(s) ===")
    for (r in relays) {
        // 10040s (provider lists) only matter when we're resolving scores.
        val nl = if (syncScores) syncer.sync(r, Filter(kinds = listOf(TrustProviderListEvent.KIND))) else null
        val nd = syncer.sync(r, Filter(kinds = listOf(DeletionEvent.KIND)))
        val np = if (syncProfiles) syncer.sync(r, Filter(kinds = listOf(MetadataEvent.KIND)), maxEvents = maxEvents) else null
        log("[sync] ${short(r)}  10040=${nl?.let { "${it.inserted}${neg(it)}" } ?: "-"}  del=${nd.inserted}  kind0=${np?.inserted ?: "-"}")
    }

    if (syncScores) {
        log("=== phase 2: resolve rank providers from stored 10040s ===")
        var providers =
            store.query<TrustProviderListEvent>(Filter(kinds = listOf(TrustProviderListEvent.KIND)))
                .flatMap { l -> l.serviceProviders().filter { it.service == ProviderTypes.rank }.map { it.pubkey to it.relayUrl } }
                .distinct()
        log("[sync] ${providers.size} rank provider(s)")
        if (maxProviders > 0 && providers.size > maxProviders) {
            providers = providers.take(maxProviders)
            log("[sync] capped to $maxProviders providers for this run")
        }

        log("=== phase 3: kind 30382 per provider, from its relay hint ===")
        for ((service, relay) in providers) {
            val o = syncer.sync(relay, Filter(kinds = listOf(ContactCardEvent.KIND), authors = listOf(service)), maxEvents = maxEvents)
            log("[sync] provider ${service.take(12)} @ ${short(relay.url)}: +${o.inserted}/${o.downloaded}${neg(o)}")
        }
    }

    } finally {
        SyncState.save(statePath, state)
        log("[state] saved cursors for ${state.relays.size} relay(s); pool=${state.relayPool.size}")
    }
}

private fun neg(o: RelaySyncer.Outcome) = if (o.usedNegentropy) " (neg)" else ""

private fun short(url: String) = url.removePrefix("wss://").removePrefix("ws://").trimEnd('/')
