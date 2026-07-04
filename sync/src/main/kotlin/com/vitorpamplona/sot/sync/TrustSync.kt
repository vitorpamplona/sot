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
import com.vitorpamplona.quartz.nip01Core.metadata.MetadataEvent
import com.vitorpamplona.quartz.nip01Core.relay.filters.Filter
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.NormalizedRelayUrl
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.displayUrl
import com.vitorpamplona.quartz.nip01Core.store.IEventStore
import com.vitorpamplona.quartz.nip09Deletions.DeletionEvent
import com.vitorpamplona.quartz.nip62RequestToVanish.RequestToVanishEvent
import com.vitorpamplona.quartz.nip65RelayList.AdvertisedRelayListEvent
import com.vitorpamplona.quartz.nip85TrustedAssertions.list.TrustProviderListEvent
import com.vitorpamplona.quartz.nip85TrustedAssertions.list.tags.ProviderTypes
import com.vitorpamplona.quartz.nip85TrustedAssertions.users.ContactCardEvent
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.joinAll
import kotlinx.coroutines.launch
import kotlinx.coroutines.sync.Semaphore
import kotlinx.coroutines.sync.withPermit

/** Tuning knobs for one sync pass. Everything has a sensible default. */
data class SyncOptions(
    /** Per-download ingest cap for bounded experiments; 0 (the default) = full sync. */
    val maxEvents: Int = 0,
    /** Idle timeout: give up on a relay only after this long with NOTHING received. */
    val fetchTimeoutMs: Long = 10_000,
    /** Cap on how many rank providers a pass visits, 0 = all. */
    val maxProviders: Int = 0,
    /** How many relays sync at the same time (store writes stay serialized). */
    val concurrency: Int = 64,
    /**
     * Force the authoritative silent-deletion pass on pages-only provider
     * relays too (full enumeration + diff). Negentropy-capable relays detect
     * silent deletions automatically on every full sync; this is only for
     * providers whose relay can't reconcile. Costly — run on a slow cadence.
     */
    val reconcileScores: Boolean = false,
    /** Verify id + signature before storing. Test-only seam — leave on. */
    val verifyEvents: Boolean = true,
)

/**
 * The observer behind every unauthenticated search: a real user whose web of
 * trust defines the relay's default ranking. [relay] is their home relay —
 * where their FIRST 10002 is synced from, with no dependence on indexer
 * coverage; from there the standard chain runs.
 */
data class HouseAccount(
    val pubkey: HexKey,
    val relay: NormalizedRelayUrl,
)

/**
 * The SCORES plane (docs/v2-sync-proposal.md, phase 1): per-observer trust
 * data, synced author-first in the one dependency order that keeps 10040s
 * authoritative — **10002 resolution precedes every per-author sync**,
 * because the freshest 10040 lives on the observer's own outbox relay:
 *
 * ```
 * seed relays ──10040 hints──▶ store            (who the observers are)
 * index relays ──observer 10002s──▶ store        (where their outboxes are)
 * observer outboxes ──0/10002/10040/5/62──▶ store (the AUTHORITATIVE 10040)
 * provider relays ──30382/5──▶ store              (the scores themselves)
 * orphan sweep: 30382 authors no stored 10040 references — deleted
 * ```
 *
 * Every step syncs INTO the store and reads the previous step's results back
 * OUT of the store — replaceable supersession is what makes "authoritative"
 * need no special code (whatever source a 10040 arrives from, the newest
 * wins; the outbox pass just makes sure the newest is actually seen).
 * Provider switches need no invalidation logic either: the sweep deletes any
 * provider's 30382s the moment no 10040 lists it, and the ranking projection
 * (`:profile`) re-derives the observer's cells from what remains.
 */
class TrustSync(
    private val syncer: RelaySyncer,
    private val store: IEventStore,
    private val opts: SyncOptions,
    private val log: (String) -> Unit,
    private val progress: SyncProgress = SyncProgress(log = { }),
) {
    /**
     * One full pass of the scores plane. [observers] are the externally-known
     * observers (config extras + NIP-42 enrollments); the house account and
     * every stored 10040's author join automatically.
     */
    suspend fun run(
        observers: Set<HexKey> = emptySet(),
        indexRelays: List<NormalizedRelayUrl>,
        house: HouseAccount? = null,
        seedRelays: List<NormalizedRelayUrl> = emptyList(),
    ) {
        discoverObserverHints(seedRelays)
        val all = observers + setOfNotNull(house?.pubkey) + storedObservers()
        if (all.isEmpty()) {
            log("[sync] no observers (no house account, no config extras, no stored 10040s) - nothing to sync")
            return
        }
        resolveObserverRelayLists(all, indexRelays, house)
        syncObserverOutboxes(all, indexRelays)
        syncProviderScores()
        sweepOrphanScores()
    }

    /**
     * Phase 0 — discovery hints: 10040s held by the seed relays tell us WHO
     * the observers are. Never the authority (that's the outbox pass); a
     * stale hint is harmless because supersession keeps the newest.
     */
    private suspend fun discoverObserverHints(seedRelays: List<NormalizedRelayUrl>) {
        if (seedRelays.isEmpty()) return
        log("=== scores 0: 10040 hints from ${seedRelays.size} seed relay(s) ===")
        progress.startPhase("hints", seedRelays.size)
        forEachParallel(seedRelays, opts.concurrency) { relay ->
            val o = syncer.sync(relay, Filter(kinds = listOf(TrustProviderListEvent.KIND)), maxEvents = opts.maxEvents)
            log("[${progress.itemDone()}/${seedRelays.size}] ${relay.displayUrl()} 10040=+${o.inserted}/${o.downloaded}")
        }
    }

    /** Every stored 10040's author is an observer — however the list got here. */
    private suspend fun storedObservers(): Set<HexKey> =
        store
            .query<TrustProviderListEvent>(Filter(kinds = listOf(TrustProviderListEvent.KIND)))
            .map { it.pubKey }
            .toSet()

    /**
     * Phase A — 10002 resolution from the index relays (the identity's stored
     * 10086), plus the house account's home relay for its bootstrap. NIP-65
     * says relay lists are broadcast widely, which is exactly what the index
     * relays exist to hold — this is how 10002 escapes the circularity that
     * sinks 10040-first designs.
     */
    private suspend fun resolveObserverRelayLists(
        observers: Set<HexKey>,
        indexRelays: List<NormalizedRelayUrl>,
        house: HouseAccount?,
    ) {
        val batches = observers.chunked(AUTHORS_PER_FILTER)
        val units =
            buildList {
                for (relay in indexRelays) for (batch in batches) add(relay to batch)
                house?.let { add(it.relay to listOf(it.pubkey)) }
            }
        log("=== scores A: ${observers.size} observer 10002(s) from ${indexRelays.size} index relay(s) ===")
        progress.startPhase("relay lists", units.size)
        forEachParallel(units, opts.concurrency) { (relay, batch) ->
            syncer.sync(relay, Filter(kinds = listOf(AdvertisedRelayListEvent.KIND), authors = batch), maxEvents = opts.maxEvents)
            progress.itemDone()
        }
    }

    /**
     * Phase B — the observers' outboxes: the AUTHORITATIVE 10040 lives there,
     * and their kind 0, a fresher 10002, and their own 5/62 ride along (a
     * deletion published only to the author's outbox must still erase here —
     * the v2 store interprets both).
     */
    private suspend fun syncObserverOutboxes(
        observers: Set<HexKey>,
        indexRelays: List<NormalizedRelayUrl>,
    ) {
        val outboxes = writeRelaysByAuthor(observers)
        val noList = observers.filterNot { it in outboxes.keys }
        val units =
            buildList {
                outboxes
                    .flatMap { (author, relays) -> relays.map { it to author } }
                    .groupBy({ it.first }, { it.second })
                    .forEach { (relay, authors) -> authors.distinct().chunked(AUTHORS_PER_FILTER).forEach { add(relay to it) } }
                // No 10002 anywhere: degrade to the index relays, never drop the observer.
                for (relay in indexRelays) for (batch in noList.chunked(AUTHORS_PER_FILTER)) add(relay to batch)
            }
        val kinds =
            listOf(
                MetadataEvent.KIND,
                AdvertisedRelayListEvent.KIND,
                TrustProviderListEvent.KIND,
                DeletionEvent.KIND,
                RequestToVanishEvent.KIND,
            )
        log("=== scores B: outboxes of ${observers.size} observer(s) (${noList.size} without a 10002) across ${units.size} unit(s) ===")
        progress.startPhase("outboxes", units.size)
        forEachParallel(units, opts.concurrency) { (relay, batch) ->
            val o = syncer.sync(relay, Filter(kinds = kinds, authors = batch), maxEvents = opts.maxEvents)
            log("[${progress.itemDone()}/${units.size}] ${batch.size} observer(s) @ ${relay.displayUrl()}: +${o.inserted}/${o.downloaded}${neg(o)}")
        }
    }

    /** The stored (newest — supersession) 10002 write/both relays per author. */
    private suspend fun writeRelaysByAuthor(authors: Set<HexKey>): Map<HexKey, List<NormalizedRelayUrl>> =
        authors
            .chunked(AUTHORS_PER_FILTER)
            .flatMap { batch -> store.query<AdvertisedRelayListEvent>(Filter(kinds = listOf(AdvertisedRelayListEvent.KIND), authors = batch)) }
            .mapNotNull { list -> list.writeRelaysNorm()?.let { list.pubKey to it } }
            .toMap()

    /**
     * Phase C — the providers: every stored 10040's `30382:rank` entries,
     * pulled from their own relay hints. Providers cluster hard on a few
     * NIP-85 relays, so services group by relay into ONE multi-author filter
     * (chunked).
     *
     * **The provider relay is the AUTHORITATIVE source of truth for its
     * service keys' 30382s** — the observer chose it in their 10040. So
     * deletion is ABSENCE: a full sync reconciles the batch's complete id
     * set, and any score we hold that the relay no longer serves is deleted
     * locally. No kind-5 download is needed for this scope (a provider
     * retracting a score just removes it; the reconcile diff sees the hole).
     * This absence-is-deletion rule applies ONLY here — kind 30382 on the
     * 10040-chosen relay — never to the records plane, where an author's
     * outbox is one replica among many and deletion needs an explicit
     * kind 5/62.
     */
    private suspend fun syncProviderScores() {
        var providers =
            store
                .query<TrustProviderListEvent>(Filter(kinds = listOf(TrustProviderListEvent.KIND)))
                .flatMap { l -> l.serviceProviders().filter { it.service == ProviderTypes.rank }.map { it.pubkey to it.relayUrl } }
                .distinct()
        if (opts.maxProviders > 0 && providers.size > opts.maxProviders) {
            providers = providers.take(opts.maxProviders)
            log("[sync] capped to ${opts.maxProviders} providers for this run")
        }
        val units =
            providers
                .groupBy({ it.second }, { it.first })
                .flatMap { (relay, services) -> services.distinct().chunked(AUTHORS_PER_FILTER).map { relay to it } }

        log("=== scores C: 30382 for ${providers.size} provider(s) across ${units.size} relay batch(es) ===")
        progress.startPhase("providers", units.size)
        forEachParallel(units, opts.concurrency) { (relay, services) ->
            val scores = Filter(kinds = listOf(ContactCardEvent.KIND), authors = services)
            if (opts.maxEvents == 0) {
                // Full sync: reconcile the batch's complete id set; absence IS
                // deletion here (the relay is authoritative for this scope).
                val r = syncer.reconcile(relay, scores, forceEnumerate = opts.reconcileScores)
                if (r.relayIds != null) {
                    val stale = store.query<ContactCardEvent>(scores).map { it.id }.filterNot { it in r.relayIds }
                    if (stale.isNotEmpty()) {
                        log("[reconcile] ${services.size} provider(s) @ ${relay.displayUrl()}: ${stale.size} score event(s) no longer served - deleting")
                        stale.chunked(100).forEach { syncer.deleteFromStore(Filter(ids = it)) }
                    }
                }
                log("[${progress.itemDone()}/${units.size}] ${services.size} provider(s) @ ${relay.displayUrl()}: +${r.inserted}${if (r.usedNegentropy) " (neg)" else ""}")
            } else {
                // Bounded experiment: incremental slice only, no deletion diff (a
                // capped download must never be read as "the rest was deleted").
                val o = syncer.sync(relay, scores, maxEvents = opts.maxEvents)
                log("[${progress.itemDone()}/${units.size}] ${services.size} provider(s) @ ${relay.displayUrl()}: +${o.inserted}/${o.downloaded}${neg(o)}")
            }
        }
    }

    /**
     * Phase D — the stateless correctness backstop for provider switches:
     * 30382s whose author (service key) NO stored 10040 lists for
     * `30382:rank` anymore are stale — they pollute ranking — and are
     * deleted. Catches every path to staleness (provider switch, observer
     * removed, a 10040 deleted or vanished); the change-feed reactions only
     * buy promptness, never correctness. The ranking projection re-derives
     * each observer's cells automatically from what remains.
     */
    private suspend fun sweepOrphanScores() {
        val referenced =
            store
                .query<TrustProviderListEvent>(Filter(kinds = listOf(TrustProviderListEvent.KIND)))
                .flatMap { l -> l.serviceProviders().filter { it.service == ProviderTypes.rank }.map { it.pubkey } }
                .toSet()
        // Reference-grade enumeration; a Vespa grouping query (distinct 30382
        // authors in one round trip) replaces this when the corpus grows.
        val orphans =
            store
                .query<ContactCardEvent>(Filter(kinds = listOf(ContactCardEvent.KIND)))
                .map { it.pubKey }
                .distinct()
                .filterNot { it in referenced }
        if (orphans.isEmpty()) return
        log("[sweep] ${orphans.size} orphaned score provider(s) - deleting their 30382s")
        orphans.chunked(100).forEach { syncer.deleteFromStore(Filter(kinds = listOf(ContactCardEvent.KIND), authors = it)) }
    }

    private fun neg(o: RelaySyncer.Outcome) = if (o.usedNegentropy) " (neg)" else ""
}

/** Relays reject filters with unboundedly many authors; fold author sets into batches this size. */
internal const val AUTHORS_PER_FILTER = 500

/** Run [body] for every item, [concurrency] at a time; one item's failure doesn't stop the rest. */
internal suspend fun <T> forEachParallel(
    items: List<T>,
    concurrency: Int,
    body: suspend (T) -> Unit,
) = coroutineScope {
    val gate = Semaphore(concurrency.coerceAtLeast(1))
    items
        .map { item ->
            launch {
                gate.withPermit {
                    runCatching { body(item) }
                }
            }
        }.joinAll()
}
