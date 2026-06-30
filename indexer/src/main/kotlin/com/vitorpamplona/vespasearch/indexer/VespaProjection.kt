package com.vitorpamplona.vespasearch.indexer

import com.vitorpamplona.quartz.nip01Core.core.Event
import com.vitorpamplona.quartz.nip01Core.metadata.MetadataEvent
import com.vitorpamplona.quartz.nip01Core.relay.filters.Filter
import com.vitorpamplona.quartz.nip01Core.store.ObservableEventStore
import com.vitorpamplona.quartz.nip01Core.store.ObservableEventStore.StoreChange
import com.vitorpamplona.quartz.nip09Deletions.DeletionEvent
import com.vitorpamplona.quartz.nip85TrustedAssertions.list.TrustProviderListEvent
import com.vitorpamplona.quartz.nip85TrustedAssertions.list.tags.ProviderTypes
import com.vitorpamplona.quartz.nip85TrustedAssertions.users.ContactCardEvent
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.ExecutorService
import java.util.concurrent.atomic.AtomicInteger

/**
 * Projects the event store into Vespa. It consumes [ObservableEventStore.changes]
 * and turns each accepted event into a Vespa write:
 *
 *  - kind 0  (MetadataEvent)        -> upsert profile fields
 *  - kind 30382 (ContactCardEvent)  -> upsert quality_scores{OBSERVER}=rank,
 *                                       where OBSERVER is resolved from kind 10040:
 *                                       the 30382 is signed by a *service* key, and
 *                                       the observer is whoever published a 10040
 *                                       mapping `30382:rank` -> that service key.
 *  - kind 10040 (TrustProviderList) -> learn service-key -> observer mapping
 *  - kind 5  (DeletionEvent)        -> erase the corresponding profile/score
 *
 * Map updates happen synchronously on the change-feed thread; the actual Vespa
 * HTTP writes are submitted to [writers] so they don't stall the feed. Sync
 * 10040s before 30382s so the mapping is populated first.
 */
class VespaProjection(
    private val store: ObservableEventStore,
    private val vespa: VespaClient,
    private val writers: ExecutorService,
    private val log: (String) -> Unit,
) {
    // service key (30382 signer) -> observer pubkey (10040 author)
    private val serviceToObserver = ConcurrentHashMap<String, String>()

    val profiles = AtomicInteger(0)
    val scores = AtomicInteger(0)
    val deletions = AtomicInteger(0)
    val unresolved = AtomicInteger(0)
    private val processed = AtomicInteger(0)
    private val failures = AtomicInteger(0)

    fun processedCount() = processed.get()

    /** Collect the change feed forever (launch in its own coroutine before syncing). */
    suspend fun run() {
        store.changes.collect { change ->
            when (change) {
                is StoreChange.Insert -> handle(change.event)
                is StoreChange.DeleteByFilter -> {} // we drive deletions via kind:5 events
                is StoreChange.DeleteExpired -> {}
            }
            processed.incrementAndGet()
        }
    }

    private suspend fun handle(ev: Event) {
        when (ev) {
            is MetadataEvent -> {
                val md = ev.contactMetaData() ?: return
                submit("profile") { vespa.upsertProfile(ev.pubKey, md); profiles.incrementAndGet() }
            }
            is TrustProviderListEvent -> {
                ev.serviceProviders()
                    .filter { it.service == ProviderTypes.rank }
                    .forEach { serviceToObserver[it.pubkey] = ev.pubKey }
            }
            is ContactCardEvent -> {
                val observer = serviceToObserver[ev.pubKey] ?: resolveObserver(ev.pubKey)
                val subject = ev.aboutUser()
                val rank = ev.rank()
                if (observer == null) { unresolved.incrementAndGet(); return }
                if (subject != null && rank != null) {
                    submit("score") { vespa.upsertScore(subject, observer, rank); scores.incrementAndGet() }
                }
            }
            is DeletionEvent -> handleDeletion(ev)
            else -> {}
        }
    }

    /** kind:5 -> erase the targeted profile/score from Vespa. */
    private suspend fun handleDeletion(ev: DeletionEvent) {
        // Addressable targets carry kind+pubkey(+dtag): "0:pubkey" or "30382:service:subject".
        for (addr in ev.deleteAddressIds()) {
            val parts = addr.split(":", limit = 3)
            val kind = parts.getOrNull(0)?.toIntOrNull() ?: continue
            when (kind) {
                MetadataEvent.KIND -> parts.getOrNull(1)?.let { pk ->
                    submit("del-profile") { vespa.blankProfile(pk); deletions.incrementAndGet() }
                }
                ContactCardEvent.KIND -> {
                    val service = parts.getOrNull(1) ?: continue
                    val subject = parts.getOrNull(2) ?: continue
                    val observer = serviceToObserver[service] ?: resolveObserver(service) ?: continue
                    submit("del-score") { vespa.removeScore(subject, observer); deletions.incrementAndGet() }
                }
            }
        }
    }

    /** Fall back to the store when a 30382's service key wasn't seen as a 10040 insert yet. */
    private suspend fun resolveObserver(serviceKey: String): String? {
        serviceToObserver[serviceKey]?.let { return it }
        val lists = store.query<TrustProviderListEvent>(Filter(kinds = listOf(TrustProviderListEvent.KIND)))
        for (l in lists) {
            for (p in l.serviceProviders()) {
                if (p.service == ProviderTypes.rank) serviceToObserver[p.pubkey] = l.pubKey
            }
        }
        return serviceToObserver[serviceKey]
    }

    private fun submit(what: String, block: () -> Unit) {
        writers.submit {
            try {
                block()
            } catch (e: Exception) {
                if (failures.incrementAndGet() <= 5) log("  ! $what write failed: ${e.message}")
            }
        }
    }
}
