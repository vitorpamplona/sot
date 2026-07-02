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

import com.vitorpamplona.quartz.nip01Core.core.Event
import com.vitorpamplona.quartz.nip01Core.metadata.MetadataEvent
import com.vitorpamplona.quartz.nip01Core.relay.filters.Filter
import com.vitorpamplona.quartz.nip01Core.store.ObservableEventStore
import com.vitorpamplona.quartz.nip01Core.store.ObservableEventStore.StoreChange
import com.vitorpamplona.quartz.nip09Deletions.DeletionEvent
import com.vitorpamplona.quartz.nip62RequestToVanish.RequestToVanishEvent
import com.vitorpamplona.quartz.nip85TrustedAssertions.list.TrustProviderListEvent
import com.vitorpamplona.quartz.nip85TrustedAssertions.list.tags.ProviderTypes
import com.vitorpamplona.quartz.nip85TrustedAssertions.users.ContactCardEvent
import com.vitorpamplona.quartz.utils.Hex
import com.vitorpamplona.sot.vespa.Profile
import com.vitorpamplona.sot.vespa.VespaClient
import kotlinx.coroutines.delay
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.ExecutorService
import java.util.concurrent.atomic.AtomicInteger

/**
 * Projects the event store into Vespa. It registers with [ObservableEventStore.changes]
 * and maps each accepted Nostr event into a ready-made object, then calls the
 * schema-aware [VespaClient] (in :vespa, which knows nothing about Nostr):
 *
 *  - kind 0  (MetadataEvent)        -> upsert profile fields
 *  - kind 30382 (ContactCardEvent)  -> upsert quality_scores{OBSERVER}=rank,
 *                                       where OBSERVER is resolved from kind 10040:
 *                                       the 30382 is signed by a *service* key, and
 *                                       the observer is whoever published a 10040
 *                                       mapping `30382:rank` -> that service key.
 *  - kind 10040 (TrustProviderList) -> learn service-key -> observer mapping
 *  - kind 5  (DeletionEvent)        -> erase the targeted profile/score, whether
 *                                       the target is an address (a-tag) or a raw
 *                                       event id (e-tag, resolved through the
 *                                       provenance ids Vespa stores)
 *  - kind 62 (RequestToVanish)      -> blank the author's profile when the request
 *                                       covers this store's relay identity
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

    /**
     * Suspend until the change feed has been idle — nothing newly processed for
     * [idleMs] — or [maxMs] elapses. Call after the sync finishes to let the
     * projection catch up before the writers are drained.
     */
    suspend fun awaitIdle(
        idleMs: Long = 3000,
        maxMs: Long = 120_000,
    ) {
        var last = -1
        var stable = 0L
        var total = 0L
        while (stable < idleMs && total < maxMs) {
            delay(300)
            total += 300
            val p = processed.get()
            if (p == last) {
                stable += 300
            } else {
                last = p
                stable = 0
            }
        }
    }

    /** Collect the change feed forever (launch in its own coroutine before syncing). */
    suspend fun run() {
        store.changes.collect { change ->
            try {
                when (change) {
                    is StoreChange.Insert -> {
                        handle(change.event)
                    }

                    is StoreChange.DeleteByFilter -> {
                        handleVanish(change.filters)
                    }

                    is StoreChange.DeleteExpired -> {}
                }
            } catch (e: Exception) {
                // A single bad event must never kill the projection (and, if it were
                // a child job, the sync with it).
                if (failures.incrementAndGet() <= 5) log("  ! projection: ${e.message}")
            }
            processed.incrementAndGet()
        }
    }

    private suspend fun handle(ev: Event) {
        when (ev) {
            is MetadataEvent -> {
                val profile = ev.toProfile() ?: return
                submit("profile") {
                    vespa.upsertProfile(profile)
                    profiles.incrementAndGet()
                }
            }

            is TrustProviderListEvent -> {
                learn(ev)
            }

            is ContactCardEvent -> {
                val observer = serviceToObserver[ev.pubKey] ?: resolveObserver(ev.pubKey)
                val subject = ev.aboutUser()
                val rank = ev.rank()
                if (observer == null) {
                    unresolved.incrementAndGet()
                    return
                }
                if (subject != null && rank != null) {
                    submit("score") {
                        vespa.upsertScore(subject, observer, rank, ev.id)
                        scores.incrementAndGet()
                    }
                }
            }

            is DeletionEvent -> {
                handleDeletion(ev)
            }

            is RequestToVanishEvent -> {
                // The store's own vanish module already erased the author's events for
                // this relay identity; mirror the decision into Vespa. (Score cells
                // keyed by a vanished *observer* aren't reversed — see handleVanish.)
                if (ev.shouldVanishFrom(store.relay)) {
                    submit("vanish") {
                        vespa.blankProfile(ev.pubKey)
                        deletions.incrementAndGet()
                    }
                }
            }

            else -> {}
        }
    }

    /** kind:5 -> erase the targeted profile/score from Vespa, by event id (e-tags) and by address (a-tags). */
    private suspend fun handleDeletion(ev: DeletionEvent) {
        // e-tag targets are raw event ids. The store has already deleted the events
        // themselves, so the only way back to the affected doc is the provenance ids
        // Vespa stores (event_id / score_event_ids).
        for (id in ev.deleteEventIds()) deleteByEventId(id)

        for (addr in ev.deleteAddressIds()) {
            // Addressable target = "kind:author(:dtag)". kind:0 -> "0:<profile pubkey>";
            // kind:30382 -> "30382:<service key>:<subject pubkey>".
            val parts = addr.split(":", limit = 3)
            val kind = parts.getOrNull(0)?.toIntOrNull() ?: continue
            val author = parts.getOrNull(1) ?: continue
            when (kind) {
                MetadataEvent.KIND -> {
                    submit("del-profile") {
                        vespa.blankProfile(author)
                        deletions.incrementAndGet()
                    }
                }

                ContactCardEvent.KIND -> {
                    val subject = parts.getOrNull(2) ?: continue
                    // The 30382 author is a *service key*; the score cell is keyed by its observer.
                    val observer = serviceToObserver[author] ?: resolveObserver(author) ?: continue
                    submit("del-score") {
                        vespa.removeScore(subject, observer)
                        deletions.incrementAndGet()
                    }
                }
            }
        }
    }

    /**
     * The store deleted events matching [filters] via an explicit `store.delete()`
     * call. (Kind 5/62 enforcement happens inside the store's SQLite modules and
     * emits nothing — those are mirrored from their Insert instead.) Blank each
     * targeted author's profile and resolve targeted event ids through the
     * provenance ids. (Score cells keyed by a vanished *observer* aren't reversed —
     * Vespa can't remove one tensor label across every doc without knowing the
     * subjects; those decay when the doc is re-fed.)
     */
    private suspend fun handleVanish(filters: List<Filter>) {
        for (author in filters.flatMap { it.authors.orEmpty() }.toSet()) {
            submit("vanish") {
                vespa.blankProfile(author)
                deletions.incrementAndGet()
            }
        }
        for (id in filters.flatMap { it.ids.orEmpty() }.toSet()) deleteByEventId(id)
    }

    /**
     * Erase whatever [eventId] produced in Vespa: the profile fields if it was a
     * kind:0 (blanking is correct for replaceables — an old superseded id simply
     * no longer matches the doc's event_id and becomes a no-op), or one observer's
     * score cell if it was a kind:30382.
     */
    private fun deleteByEventId(eventId: String) {
        // Deletion tags are attacker-controlled text; only a real 64-hex event id
        // may reach the engine's YQL lookups.
        if (!Hex.isHex64(eventId)) return
        submit("del-by-id") {
            vespa.findProfileByEventId(eventId)?.let { pubkey ->
                vespa.blankProfile(pubkey)
                deletions.incrementAndGet()
            }
            vespa.findScoreByEventId(eventId)?.let { (subject, observer) ->
                vespa.removeScore(subject, observer)
                deletions.incrementAndGet()
            }
        }
    }

    /** Record a 10040's `30382:rank` providers as service-key -> observer mappings. */
    private fun learn(list: TrustProviderListEvent) {
        list
            .serviceProviders()
            .filter { it.service == ProviderTypes.rank }
            .forEach { serviceToObserver[it.pubkey] = list.pubKey }
    }

    /** Fall back to the store when a 30382's service key wasn't seen as a 10040 insert yet. */
    private suspend fun resolveObserver(serviceKey: String): String? {
        store.query<TrustProviderListEvent>(Filter(kinds = listOf(TrustProviderListEvent.KIND))).forEach(::learn)
        return serviceToObserver[serviceKey]
    }

    private fun submit(
        what: String,
        block: () -> Unit,
    ) {
        writers.submit {
            try {
                block()
            } catch (e: Exception) {
                if (failures.incrementAndGet() <= 5) log("  ! $what write failed: ${e.message}")
            }
        }
    }
}

/** kind:0 -> the plain [Profile] document the engine indexes; null if the metadata doesn't parse. */
private fun MetadataEvent.toProfile(): Profile? {
    val md = contactMetaData() ?: return null
    return Profile(
        pubkey = pubKey,
        name = md.name,
        displayName = md.displayName,
        about = md.about,
        picture = md.picture,
        banner = md.banner,
        nip05 = md.nip05,
        lud06 = md.lud06,
        lud16 = md.lud16,
        website = md.website,
        eventId = id,
    )
}
