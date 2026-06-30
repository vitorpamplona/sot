package com.vitorpamplona.vespasearch.indexer

import com.vitorpamplona.quartz.nip01Core.core.Event
import com.vitorpamplona.quartz.nip01Core.metadata.MetadataEvent
import com.vitorpamplona.quartz.nip01Core.relay.client.NostrClient
import com.vitorpamplona.quartz.nip01Core.relay.client.accessories.fetchAllPages
import com.vitorpamplona.quartz.nip01Core.relay.filters.Filter
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.RelayUrlNormalizer
import com.vitorpamplona.quartz.nip01Core.store.IEventStore
import com.vitorpamplona.quartz.nip01Core.store.ObservableEventStore
import com.vitorpamplona.quartz.nip09Deletions.DeletionEvent
import com.vitorpamplona.quartz.nip85TrustedAssertions.list.TrustProviderListEvent
import com.vitorpamplona.quartz.nip85TrustedAssertions.list.tags.ProviderTypes
import com.vitorpamplona.quartz.nip85TrustedAssertions.users.ContactCardEvent
import java.util.Collections
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.withTimeoutOrNull

/**
 * Stage-A pipeline: sync events into the store; the [VespaProjection] (running on
 * the store's change feed) writes them to Vespa.
 *
 *   Phase 1: kind 0 / 10040 / 5 from seed relays
 *   Phase 2: read stored 10040s -> (serviceKey, relayHint) for `30382:rank`
 *   Phase 3: kind 30382 per provider, from its relay hint only
 *
 * Scores land keyed by the OBSERVER (10040 author), matching Brainstorm — the
 * projection resolves serviceKey -> observer. Uses fetchAllPages here; the
 * negentropy-preferred syncer + cursors come in stage B.
 */
suspend fun runSync(
    client: NostrClient,
    store: ObservableEventStore,
    projection: VespaProjection,
    seedRelays: List<String>,
    maxEvents: Int,
    log: (String) -> Unit,
    fetchTimeoutMs: Long = 30_000,
    maxProviders: Int = 0,
    syncProfiles: Boolean = true,
) = coroutineScope {
    val projJob = launch { projection.run() }
    var emitted = 0

    suspend fun syncInto(relayUrl: String, filter: Filter): Int {
        val relay = RelayUrlNormalizer.normalizeOrNull(relayUrl) ?: return 0
        val buf = Collections.synchronizedList(ArrayList<Event>())
        val n = withTimeoutOrNull(fetchTimeoutMs) {
            client.fetchAllPages(relay, listOf(filter)) { ev -> buf.add(ev) }
        } ?: 0
        val accepted = store.batchInsert(ArrayList(buf)).count { it is IEventStore.InsertOutcome.Accepted }
        emitted += accepted
        return n
    }

    val k0 = Filter(kinds = listOf(MetadataEvent.KIND), limit = if (maxEvents > 0) maxEvents else null)
    log("=== phase 1: kind 0 / 10040 / 5 from ${seedRelays.size} seed relays ===")
    for (r in seedRelays) {
        val nList = syncInto(r, Filter(kinds = listOf(TrustProviderListEvent.KIND)))
        val nDel = syncInto(r, Filter(kinds = listOf(DeletionEvent.KIND)))
        val nProf = if (syncProfiles) syncInto(r, k0) else 0
        log("[sync] ${short(r)}  10040=$nList  del=$nDel  kind0=$nProf")
    }

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
        val buf = Collections.synchronizedList(ArrayList<Event>())
        val filter = Filter(kinds = listOf(ContactCardEvent.KIND), authors = listOf(service), limit = if (maxEvents > 0) maxEvents else null)
        withTimeoutOrNull(fetchTimeoutMs) { client.fetchAllPages(relay, listOf(filter)) { ev -> buf.add(ev) } }
        val accepted = store.batchInsert(ArrayList(buf)).count { it is IEventStore.InsertOutcome.Accepted }
        emitted += accepted
        log("[sync] provider ${service.take(12)} @ ${short(relay.url)}: ${buf.size} assertions")
    }

    // Let the projection drain the change feed before we stop.
    log("=== draining projection ($emitted events) ===")
    withTimeoutOrNull(120_000) {
        while (projection.processedCount() < emitted) delay(200)
    }
    projJob.cancel()
}

private fun short(url: String) = url.removePrefix("wss://").removePrefix("ws://").trimEnd('/')
