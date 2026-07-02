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

import com.vitorpamplona.quartz.nip01Core.metadata.MetadataEvent
import com.vitorpamplona.quartz.nip01Core.relay.client.NostrClient
import com.vitorpamplona.quartz.nip01Core.relay.client.auth.EmptyIAuthStatus
import com.vitorpamplona.quartz.nip01Core.relay.client.auth.IAuthStatus
import com.vitorpamplona.quartz.nip01Core.relay.filters.Filter
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.NormalizedRelayUrl
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.displayUrl
import com.vitorpamplona.quartz.nip01Core.store.ObservableEventStore
import com.vitorpamplona.quartz.nip09Deletions.DeletionEvent
import com.vitorpamplona.quartz.nip85TrustedAssertions.list.TrustProviderListEvent
import com.vitorpamplona.quartz.nip85TrustedAssertions.list.tags.ProviderTypes
import com.vitorpamplona.quartz.nip85TrustedAssertions.users.ContactCardEvent
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.joinAll
import kotlinx.coroutines.launch
import kotlinx.coroutines.sync.Semaphore
import kotlinx.coroutines.sync.withPermit

/**
 * Tuning knobs for one [runSync] pass. Everything has a sensible default; the
 * caller only names what it changes.
 */
data class SyncOptions(
    /** Per-kind ingest cap for bounded experiments; 0 (the default) = full sync. */
    val maxEvents: Int = 0,
    val fetchTimeoutMs: Long = 30_000,
    /** Cap on how many rank providers phase 3 visits, 0 = all. */
    val maxProviders: Int = 0,
    /** Expand the relay set with a bounded NIP-65 outbox crawl first (on by default). */
    val discover: Boolean = true,
    val maxRounds: Int = 3,
    val maxRelays: Int = 200,
    /** How many relays sync at the same time (store writes stay serialized). */
    val concurrency: Int = 8,
    /**
     * Force the authoritative silent-deletion pass on pages-only provider relays
     * too (full enumeration + diff). Negentropy-capable relays detect silent
     * deletions automatically on every full sync, so this is only for providers
     * whose relay can't reconcile. Costly — run it on a slow cadence.
     */
    val reconcileScores: Boolean = false,
    /** Verify id + signature before storing. Test-only seam — leave on. */
    val verifyEvents: Boolean = true,
)

/**
 * Nostr sync pipeline. Fetches events from relays into the store — the source of
 * truth — and nothing more; mapping the store into Vespa is a separate concern
 * (a projection subscribes to the store's change feed, wired up by the caller).
 * [RelaySyncer] prefers negentropy and falls back to paginated `since` fetches,
 * advancing per-relay cursors in [SyncState] so periodic re-runs only pull the delta.
 *
 * Scores land keyed by the OBSERVER (10040 author), per NIP-85 Trusted Assertions.
 */
suspend fun runSync(
    client: NostrClient,
    store: ObservableEventStore,
    state: SyncState,
    statePath: String,
    seedRelays: List<NormalizedRelayUrl>,
    opts: SyncOptions,
    sharedProgress: SyncProgress? = null,
    auth: IAuthStatus = EmptyIAuthStatus,
    log: (String) -> Unit,
): Unit =
    coroutineScope {
        val progress = sharedProgress ?: SyncProgress(log = log)
        val syncer = RelaySyncer(client, store, state, log, idleTimeoutMs = opts.fetchTimeoutMs, verifyEvents = opts.verifyEvents, progress = progress, auth = auth)
        // ONE live status line every few seconds for the whole pass; every
        // download and phase reports into it (see SyncProgress).
        val ticker = launch { progress.run() }
        try {
            val relays =
                if (opts.discover) {
                    Discovery(syncer, store, state, log, progress).crawl(seedRelays, opts.maxRounds, opts.maxRelays, opts.concurrency).toList()
                } else {
                    seedRelays
                }
            syncEvents(syncer, relays, opts, log, progress)
            syncProviderScores(syncer, store, opts, log, progress)
        } finally {
            ticker.cancel()
            SyncState.save(statePath, state)
            log("[state] saved cursors for ${state.relays.size} relay(s); pool=${state.relayPool.size}")
        }
    }

/**
 * Phase 1: provider lists (10040), deletions (5), and profiles (0) from every
 * relay in the set — [SyncOptions.concurrency] relays at a time, kinds still
 * sequential within a relay (they share its cursors and capability memory).
 */
private suspend fun syncEvents(
    syncer: RelaySyncer,
    relays: List<NormalizedRelayUrl>,
    opts: SyncOptions,
    log: (String) -> Unit,
    progress: SyncProgress,
) {
    log("=== phase 1: 10040 / 5 / 0 from ${relays.size} relay(s), ${opts.concurrency} in parallel ===")
    progress.startPhase("relays", relays.size)
    forEachParallel(relays, opts.concurrency) { r ->
        val started = System.currentTimeMillis()
        val lists = syncer.sync(r, Filter(kinds = listOf(TrustProviderListEvent.KIND)), maxEvents = opts.maxEvents)
        val dels = syncer.sync(r, Filter(kinds = listOf(DeletionEvent.KIND)), maxEvents = opts.maxEvents)
        val profiles = syncer.sync(r, Filter(kinds = listOf(MetadataEvent.KIND)), maxEvents = opts.maxEvents)
        val secs = (System.currentTimeMillis() - started) / 1000
        log("[${progress.itemDone()}/${relays.size}] ${r.displayUrl()}  10040=${lists.inserted}${neg(lists)}  del=${dels.inserted}  kind0=${profiles.inserted}  (${secs}s)")
    }
}

/**
 * Phases 2 + 3: resolve the `30382:rank` providers from the stored 10040 lists,
 * then pull each provider's kind-30382 assertions from its own relay hint.
 */
private suspend fun syncProviderScores(
    syncer: RelaySyncer,
    store: ObservableEventStore,
    opts: SyncOptions,
    log: (String) -> Unit,
    progress: SyncProgress,
) {
    log("=== phase 2: resolve rank providers from stored 10040s ===")
    var providers =
        store
            .query<TrustProviderListEvent>(Filter(kinds = listOf(TrustProviderListEvent.KIND)))
            .flatMap { l -> l.serviceProviders().filter { it.service == ProviderTypes.rank }.map { it.pubkey to it.relayUrl } }
            .distinct()
    log("[sync] ${providers.size} rank provider(s)")
    if (opts.maxProviders > 0 && providers.size > opts.maxProviders) {
        providers = providers.take(opts.maxProviders)
        log("[sync] capped to ${opts.maxProviders} providers for this run")
    }

    log("=== phase 3: kinds 30382 + 5 per provider, from its relay hint (${opts.concurrency} in parallel) ===")
    progress.startPhase("providers", providers.size)
    forEachParallel(providers, opts.concurrency) { (service, relay) ->
        // The provider's own deletion requests live on its relay too — a kind:5
        // published only there must still erase the score from Vespa.
        syncer.sync(relay, Filter(kinds = listOf(DeletionEvent.KIND), authors = listOf(service)), maxEvents = opts.maxEvents)

        val scores = Filter(kinds = listOf(ContactCardEvent.KIND), authors = listOf(service))
        if (opts.maxEvents == 0) {
            // Full sync: reconcile the provider's complete set, so scores the
            // provider silently removed (no kind:5, no supersession) get deleted
            // from the store — the change feed then erases them from Vespa.
            val r = syncer.reconcile(relay, scores, forceEnumerate = opts.reconcileScores)
            if (r.relayIds != null) {
                val stale = store.query<ContactCardEvent>(scores).map { it.id }.filterNot { it in r.relayIds }
                if (stale.isNotEmpty()) {
                    log("[reconcile] provider ${service.take(12)}: ${stale.size} score event(s) vanished from ${relay.displayUrl()} - deleting")
                    stale.chunked(100).forEach { syncer.deleteFromStore(Filter(ids = it)) }
                }
            }
            log("[${progress.itemDone()}/${providers.size}] provider ${service.take(12)} @ ${relay.displayUrl()}: +${r.inserted}${if (r.usedNegentropy) " (neg)" else ""}")
        } else {
            // Bounded experiment: incremental slice only, no deletion diff (a
            // capped download must never be read as "the rest was deleted").
            val o = syncer.sync(relay, scores, maxEvents = opts.maxEvents)
            log("[${progress.itemDone()}/${providers.size}] provider ${service.take(12)} @ ${relay.displayUrl()}: +${o.inserted}/${o.downloaded}${neg(o)}")
        }
    }
}

/** Run [body] for every item, [concurrency] at a time; one item's failure is logged by [body]'s caller, not fatal to the rest. */
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

private fun neg(o: RelaySyncer.Outcome) = if (o.usedNegentropy) " (neg)" else ""
