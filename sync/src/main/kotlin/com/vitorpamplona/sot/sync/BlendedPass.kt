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
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.NormalizedRelayUrl
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.displayUrl
import com.vitorpamplona.quartz.nip01Core.store.IEventStore
import com.vitorpamplona.quartz.nip65RelayList.AdvertisedRelayListEvent
import com.vitorpamplona.quartz.nip85TrustedAssertions.list.TrustProviderListEvent
import com.vitorpamplona.quartz.nip85TrustedAssertions.users.ContactCardEvent
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.launch
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger

/**
 * One scores-plane pass as a work-unit pipeline. Every download is a unit in
 * one worker pool, and each unit's completion enqueues exactly the downstream
 * units it unblocks. An observer's outbox sync starts the moment THEIR 10002
 * resolution finishes, not when everyone's does. A provider's score sync starts
 * the moment any completed outbox names it. No relay waits for an unrelated
 * relay: a slow index relay delays only the observers whose lists it still
 * owes, and the (typically massive) provider score downloads begin while the
 * rest of the directory chain still resolves.
 *
 * The phased dependency order survives PER OBSERVER. Outbox units are derived
 * only after ALL of that observer's 10002 lookups complete, and provider units
 * only from 10040s read back after their observer's outbox unit. So the outbox
 * version of a 10040 still supersedes any seed-relay hint before its providers
 * are chosen. The end-of-pass catch-up derives providers from EVERYTHING stored
 * (hint-only observers, failed outbox units); this is phase C run over the
 * final store state.
 *
 * Work units batch as they accumulate. Same-relay authors buffer up to
 * [AUTHORS_PER_FILTER] and flush either on overflow or when the pool goes idle.
 * Big rosters still get big filters, and small ones never wait.
 */
internal class BlendedPass(
    private val syncer: RelaySyncer,
    private val store: IEventStore,
    private val opts: SyncOptions,
    private val progress: SyncProgress,
    private val log: (String) -> Unit,
    private val indexRelays: List<NormalizedRelayUrl>,
    private val house: HouseAccount?,
) {
    private val queue = Channel<suspend () -> Unit>(Channel.UNLIMITED)
    private val pending = AtomicInteger(0)
    private val known = ConcurrentHashMap.newKeySet<HexKey>()
    private val listsLeft = ConcurrentHashMap<HexKey, AtomicInteger>()
    private val outboxBuf = ConcurrentHashMap<NormalizedRelayUrl, MutableList<HexKey>>()
    private val noList = ConcurrentLinkedQueue<HexKey>()
    private val providerBuf = ConcurrentHashMap<NormalizedRelayUrl, MutableList<HexKey>>()
    private val seenServices = ConcurrentHashMap.newKeySet<HexKey>()

    // Every relay a service key is hinted on across all discovered 10040s.
    // Absence-is-deletion is only authoritative when a service has exactly one
    // canonical relay. A key hinted on two relays by two observers would
    // otherwise have the second relay's scores deleted by the first relay's
    // reconcile diff.
    private val serviceRelays = ConcurrentHashMap<HexKey, MutableSet<NormalizedRelayUrl>>()
    private val catchUpRan = AtomicBoolean(false)

    suspend fun run(
        initialObservers: Set<HexKey>,
        seedRelays: List<NormalizedRelayUrl>,
    ) = coroutineScope {
        registerObservers(initialObservers + storedObservers())
        for (relay in seedRelays) submit { hintUnit(relay) }
        // No initial units doesn't mean no work. An observer with no
        // discoverable 10002 lands in the no-list fallback, and stored 10040s
        // feed the catch-up; both surface through onIdle. Only a pass that
        // STILL finds nothing after that has nothing to sync.
        if (pending.get() == 0 && !onIdle()) {
            log("[sync] no observers (no house account, no config extras, no stored 10040s) and no seed relays - nothing to sync")
            queue.close()
        }
        repeat(opts.concurrency.coerceAtLeast(1)) {
            launch {
                for (job in queue) {
                    // Rethrow cancellation so a torn-down pass actually
                    // unwinds. Only real unit failures are logged and skipped.
                    try {
                        job()
                    } catch (e: CancellationException) {
                        throw e
                    } catch (e: Exception) {
                        log("  ! chain unit failed: ${e.message}")
                    }
                    unitDone()
                }
            }
        }
    }

    private fun submit(job: suspend () -> Unit) {
        pending.incrementAndGet()
        progress.addPhaseItems(1)
        queue.trySend(job)
    }

    /** Pool drained: flush the partial batches; when nothing is left to flush either, the pass is over. */
    private suspend fun unitDone() {
        if (pending.decrementAndGet() > 0) return
        if (!onIdle()) queue.close()
    }

    private suspend fun onIdle(): Boolean {
        var submitted = false
        for ((relay, batch) in drain(outboxBuf)) {
            submit { outboxUnit(relay, batch) }
            submitted = true
        }
        val unresolved = generateSequence { noList.poll() }.toList()
        if (unresolved.isNotEmpty()) {
            // No 10002 anywhere: degrade to the index relays, never drop the observer.
            if (indexRelays.isEmpty()) {
                log("[sync] ${unresolved.size} observer(s) have no discoverable 10002 and no index relay - their outbox is not synced this pass (stored-10040 providers still are, via catch-up)")
            }
            for (relay in indexRelays) {
                unresolved.chunked(AUTHORS_PER_FILTER).forEach { batch ->
                    submit { outboxUnit(relay, batch) }
                    submitted = true
                }
            }
        }
        for ((relay, batch) in drain(providerBuf)) {
            submit { scoreUnit(relay, batch) }
            submitted = true
        }
        if (!submitted && catchUpRan.compareAndSet(false, true)) {
            // Phase C over the final store state: hint-only observers and
            // failed outbox units still get their providers.
            storedProviderPairs().forEach { (service, relay) -> enqueueProvider(service, relay) }
            for ((relay, batch) in drain(providerBuf)) {
                submit { scoreUnit(relay, batch) }
                submitted = true
            }
        }
        return submitted
    }

    /** New observers join the pipeline: their 10002 lookups are submitted immediately. */
    private suspend fun registerObservers(observers: Collection<HexKey>) {
        val fresh = observers.filter(known::add)
        if (fresh.isEmpty()) return
        for (o in fresh) {
            listsLeft[o] = AtomicInteger(indexRelays.size + if (house?.pubkey == o) 1 else 0)
        }
        for (relay in indexRelays) {
            fresh.chunked(AUTHORS_PER_FILTER).forEach { batch -> submit { listUnit(relay, batch) } }
        }
        house?.takeIf { it.pubkey in fresh }?.let { h -> submit { listUnit(h.relay, listOf(h.pubkey)) } }
        // Nowhere to look a 10002 up: resolve from whatever the store already has.
        if (indexRelays.isEmpty()) {
            resolveOutboxes(fresh.filter { house?.pubkey != it })
        }
    }

    /**
     * Seed-relay 10040 discovery. Newly-verified 10040 authors join the
     * pipeline mid-flight through the [onVerified] hook, so there is no full
     * 10040 table re-scan per seed relay (which would be O(all observers) x
     * seed relays).
     */
    private suspend fun hintUnit(relay: NormalizedRelayUrl) {
        val o =
            syncer.sync(
                relay,
                Filter(kinds = listOf(TrustProviderListEvent.KIND)),
                maxEvents = opts.maxEvents,
                onVerified = { events -> registerObservers(events.filterIsInstance<TrustProviderListEvent>().map { it.pubKey }) },
            )
        log("[chain ${progress.itemDone()}/${progress.position().substringAfter('/')}] hints @ ${relay.displayUrl()}: 10040 +${o.inserted}/${o.downloaded}")
    }

    /** One (index relay x observer batch) 10002 lookup; completing an observer's LAST lookup unblocks their outboxes. */
    private suspend fun listUnit(
        relay: NormalizedRelayUrl,
        batch: List<HexKey>,
    ) {
        syncer.sync(relay, Filter(kinds = listOf(AdvertisedRelayListEvent.KIND), authors = batch), maxEvents = opts.maxEvents)
        progress.itemDone()
        // The observers in this batch whose LAST index-relay lookup just
        // landed are resolved together: one 10002 query, not one per observer.
        resolveOutboxes(batch.filter { listsLeft[it]?.decrementAndGet() == 0 })
    }

    /** [observers]' 10002 lookups are all in: derive their outbox units (or the no-list fallback), in ONE batched query. */
    private suspend fun resolveOutboxes(observers: List<HexKey>) {
        if (observers.isEmpty()) return
        val byAuthor =
            observers
                .chunked(AUTHORS_PER_FILTER)
                .flatMap { batch -> store.query<AdvertisedRelayListEvent>(Filter(kinds = listOf(AdvertisedRelayListEvent.KIND), authors = batch)) }
                .associateBy { it.pubKey }
        for (o in observers) {
            val relays = byAuthor[o]?.writeRelaysNorm().orEmpty()
            if (relays.isEmpty()) {
                noList.add(o)
                continue
            }
            for (relay in relays) {
                bufferedBatch(outboxBuf, relay, o)?.let { batch -> submit { outboxUnit(relay, batch) } }
            }
        }
    }

    /**
     * The observers' outboxes. The AUTHORITATIVE 10040 lives there, and their
     * kind 0, a fresher 10002, and their own 5/62 ride along. A deletion
     * published only to the author's outbox must still erase here, and the
     * store interprets both. Completion feeds the stored 10040s' providers into
     * the pipeline.
     */
    private suspend fun outboxUnit(
        relay: NormalizedRelayUrl,
        authors: List<HexKey>,
    ) {
        val o = syncer.sync(relay, Filter(kinds = OUTBOX_KINDS, authors = authors), maxEvents = opts.maxEvents)
        log("[chain ${progress.itemDone()}/${progress.position().substringAfter('/')}] ${authors.size} observer(s) @ ${relay.displayUrl()}: +${o.inserted}/${o.downloaded}${neg(o)}")
        store
            .query<TrustProviderListEvent>(Filter(kinds = listOf(TrustProviderListEvent.KIND), authors = authors))
            .flatMap { l -> l.rankProviders().map { it.pubkey to it.relayUrl } }
            .forEach { (service, relayHint) -> enqueueProvider(service, relayHint) }
    }

    /** A provider surfaced: buffer it under its relay hint; overflow flushes a full score unit. */
    private fun enqueueProvider(
        service: HexKey,
        relayHint: NormalizedRelayUrl?,
    ) {
        if (relayHint == null) return
        // Record EVERY hinted relay (even for an already-seen service) so the
        // stale-deletion diff can tell single-relay (authoritative) services
        // from multi-relay ones.
        serviceRelays.getOrPut(service) { ConcurrentHashMap.newKeySet() }.add(relayHint)
        if (opts.maxProviders > 0 && seenServices.size >= opts.maxProviders) return
        if (!seenServices.add(service)) return
        bufferedBatch(providerBuf, relayHint, service)?.let { batch -> submit { scoreUnit(relayHint, batch) } }
    }

    /**
     * The providers' 30382s. **The provider relay is the AUTHORITATIVE source
     * of truth for its service keys' 30382s**, because the observer chose it in
     * their 10040. So deletion is ABSENCE: a full sync reconciles the batch's
     * complete id set, and any score we hold that the relay no longer serves is
     * deleted locally. No kind-5 download is needed for this scope, since a
     * provider retracting a score just removes it and the reconcile diff sees
     * the hole. This absence-is-deletion rule applies ONLY here (kind 30382 on
     * the 10040-chosen relay). It never applies to the records plane, where an
     * author's outbox is one replica among many and deletion needs an explicit
     * kind 5/62.
     */
    private suspend fun scoreUnit(
        relay: NormalizedRelayUrl,
        services: List<HexKey>,
    ) {
        val scores = Filter(kinds = listOf(ContactCardEvent.KIND), authors = services)
        if (opts.maxEvents == 0) {
            // Full sync: reconcile the batch's complete id set; absence IS
            // deletion here (the relay is authoritative for this scope).
            val r = syncer.reconcile(relay, scores, forceEnumerate = opts.reconcileScores)
            if (r.relayIds != null) {
                // Absence-is-deletion applies ONLY to services with this as
                // their single canonical relay. A service hinted on multiple
                // relays has scores legitimately served elsewhere, which this
                // relay's set wouldn't include. Multi-relay services still
                // sync, just without a diff.
                val authoritative = services.filter { (serviceRelays[it]?.size ?: 1) == 1 }
                if (authoritative.isNotEmpty()) {
                    val held = store.snapshotIdsForNegentropy(listOf(Filter(kinds = listOf(ContactCardEvent.KIND), authors = authoritative)))
                    val stale = held.map { it.id }.filterNot { it in r.relayIds }
                    if (stale.isNotEmpty()) {
                        log("[reconcile] ${authoritative.size} provider(s) @ ${relay.displayUrl()}: ${stale.size} score event(s) no longer served - deleting")
                        stale.chunked(100).forEach { syncer.deleteFromStore(Filter(ids = it)) }
                    }
                }
            }
            log("[chain ${progress.itemDone()}/${progress.position().substringAfter('/')}] ${services.size} provider(s) @ ${relay.displayUrl()}: +${r.inserted}${if (r.usedNegentropy) " (neg)" else ""}")
        } else {
            // Bounded experiment: incremental slice only, no deletion diff (a
            // capped download must never be read as "the rest was deleted").
            val o = syncer.sync(relay, scores, maxEvents = opts.maxEvents)
            log("[chain ${progress.itemDone()}/${progress.position().substringAfter('/')}] ${services.size} provider(s) @ ${relay.displayUrl()}: +${o.inserted}/${o.downloaded}${neg(o)}")
        }
    }

    /** Add [item] under [key]; returns a full batch to flush when the buffer reaches the filter cap. */
    private fun <K> bufferedBatch(
        buffers: ConcurrentHashMap<K, MutableList<HexKey>>,
        key: K,
        item: HexKey,
    ): List<HexKey>? {
        val buf = buffers.getOrPut(key) { mutableListOf() }
        synchronized(buf) {
            buf.add(item)
            if (buf.size < AUTHORS_PER_FILTER) return null
            val batch = buf.toList()
            buf.clear()
            return batch
        }
    }

    /** Take every non-empty partial batch (the idle flush). */
    private fun <K> drain(buffers: ConcurrentHashMap<K, MutableList<HexKey>>): List<Pair<K, List<HexKey>>> =
        buffers.entries.mapNotNull { (key, buf) ->
            synchronized(buf) {
                if (buf.isEmpty()) {
                    null
                } else {
                    val batch = buf.toList()
                    buf.clear()
                    key to batch
                }
            }
        }

    /** Every stored 10040's author is an observer — however the list got here. */
    private suspend fun storedObservers(): Set<HexKey> =
        store
            .query<TrustProviderListEvent>(Filter(kinds = listOf(TrustProviderListEvent.KIND)))
            .map { it.pubKey }
            .toSet()

    /** Every stored 10040's `30382:rank` (service key -> relay hint) pairs. */
    private suspend fun storedProviderPairs(): List<Pair<HexKey, NormalizedRelayUrl?>> =
        store
            .query<TrustProviderListEvent>(Filter(kinds = listOf(TrustProviderListEvent.KIND)))
            .flatMap { l -> l.rankProviders().map { it.pubkey to it.relayUrl } }
            .distinct()

    private fun neg(o: RelaySyncer.Outcome) = if (o.usedNegentropy) " (neg)" else ""
}
