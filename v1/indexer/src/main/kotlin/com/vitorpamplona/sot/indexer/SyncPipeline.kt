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
    /** Idle timeout: give up on a relay only after this long with NOTHING received. */
    val fetchTimeoutMs: Long = 10_000,
    /** Cap on how many rank providers phase 3 visits, 0 = all. */
    val maxProviders: Int = 0,
    /** Expand the relay set with a bounded NIP-65 outbox crawl first (on by default). */
    val discover: Boolean = true,
    val maxRounds: Int = 3,
    val maxRelays: Int = 200,
    /**
     * How many relays sync at the same time (store writes stay serialized).
     * Sync is network/idle-bound — a relay spends most of a sync waiting on the
     * wire or idling toward its timeout — so this runs well above CPU count.
     * The shared OkHttp dispatcher is lifted to 1024 concurrent calls (see
     * [okHttpWebsocketBuilder]) so it never throttles this fan-out.
     */
    val concurrency: Int = 64,
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
                    Discovery(syncer, state, log, progress).crawl(seedRelays, opts.maxRounds, opts.maxRelays, opts.concurrency).toList()
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
 * Phase 1: profiles (0), deletions (5), and provider lists (10040) from every
 * relay in the set — [SyncOptions.concurrency] relays at a time. All three kinds
 * ride ONE filter per relay: negentropy reconciles the union in a single session
 * (and the pages fallback interleaves them), so it's one round-trip per relay
 * instead of three — each kind used to pay its own connect + idle-timeout tail.
 */
private suspend fun syncEvents(
    syncer: RelaySyncer,
    relays: List<NormalizedRelayUrl>,
    opts: SyncOptions,
    log: (String) -> Unit,
    progress: SyncProgress,
) {
    log("=== phase 1: kinds 0 / 5 / 10040 from ${relays.size} relay(s), ${opts.concurrency} in parallel ===")
    progress.startPhase("relays", relays.size)
    val filter = Filter(kinds = listOf(MetadataEvent.KIND, DeletionEvent.KIND, TrustProviderListEvent.KIND))
    forEachParallel(relays, opts.concurrency) { r ->
        val started = System.currentTimeMillis()
        val o = syncer.sync(r, filter, maxEvents = opts.maxEvents)
        val secs = (System.currentTimeMillis() - started) / 1000
        log("[${progress.itemDone()}/${relays.size}] ${r.displayUrl()}  0/5/10040=+${o.inserted}/${o.downloaded}${neg(o)}  (${secs}s)")
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

    // Providers cluster hard on a few NIP-85 relays (one relay routinely hints
    // for 100+ services). Syncing each provider as its own (relay, author) REQ
    // fires hundreds of round-trips — and their idle waits — at a single host,
    // one author at a time; cross-provider concurrency can't help because they
    // all contend on that one relay. Group by relay and fold every service on it
    // into ONE multi-author filter (chunked so a huge set doesn't blow a relay's
    // filter limit), turning N per-provider round-trips into ~one per relay.
    val units =
        providers
            .groupBy({ it.second }, { it.first })
            .flatMap { (relay, services) -> services.distinct().chunked(AUTHORS_PER_FILTER).map { relay to it } }

    log("=== phase 3: kinds 30382 + 5 for ${providers.size} provider(s) across ${units.size} relay batch(es) (${opts.concurrency} in parallel) ===")
    progress.startPhase("providers", units.size)
    forEachParallel(units, opts.concurrency) { (relay, services) ->
        // The providers' own deletion requests live on their relay too — a kind:5
        // published only there must still erase the score from Vespa.
        syncer.sync(relay, Filter(kinds = listOf(DeletionEvent.KIND), authors = services), maxEvents = opts.maxEvents)

        val scores = Filter(kinds = listOf(ContactCardEvent.KIND), authors = services)
        if (opts.maxEvents == 0) {
            // Full sync: reconcile the batch's complete set, so scores a provider
            // silently removed (no kind:5, no supersession) get deleted from the
            // store — the change feed then erases them from Vespa. The diff is over
            // the whole author batch at once, which is equivalent to per-provider:
            // an id the relay no longer serves for any of these authors is stale.
            val r = syncer.reconcile(relay, scores, forceEnumerate = opts.reconcileScores)
            if (r.relayIds != null) {
                val stale = store.query<ContactCardEvent>(scores).map { it.id }.filterNot { it in r.relayIds }
                if (stale.isNotEmpty()) {
                    log("[reconcile] ${services.size} provider(s) @ ${relay.displayUrl()}: ${stale.size} score event(s) vanished - deleting")
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

/** Relays reject filters with unboundedly many authors; fold a relay's providers into batches this size. */
private const val AUTHORS_PER_FILTER = 500

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
