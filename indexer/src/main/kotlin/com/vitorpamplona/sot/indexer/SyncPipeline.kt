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
import com.vitorpamplona.quartz.nip01Core.relay.filters.Filter
import com.vitorpamplona.quartz.nip01Core.store.ObservableEventStore
import com.vitorpamplona.quartz.nip09Deletions.DeletionEvent
import com.vitorpamplona.quartz.nip85TrustedAssertions.list.TrustProviderListEvent
import com.vitorpamplona.quartz.nip85TrustedAssertions.list.tags.ProviderTypes
import com.vitorpamplona.quartz.nip85TrustedAssertions.users.ContactCardEvent

/**
 * Tuning knobs for one [runSync] pass. Everything has a sensible default; the
 * caller only names what it changes.
 */
data class SyncOptions(
    /** Per-stage ingest cap, 0 = unlimited. Relays hold millions of events. */
    val maxEvents: Int = 25_000,
    val fetchTimeoutMs: Long = 30_000,
    /** Cap on how many rank providers phase 3 visits, 0 = all. */
    val maxProviders: Int = 0,
    /** Pull kind:0 profiles. */
    val profiles: Boolean = true,
    /** Pull NIP-85 scores (kind 10040 provider lists + each provider's 30382s). */
    val scores: Boolean = true,
    /** Expand the relay set with a bounded NIP-65 outbox crawl first. */
    val discover: Boolean = false,
    val maxRounds: Int = 3,
    val maxRelays: Int = 200,
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
    seedRelays: List<String>,
    opts: SyncOptions,
    log: (String) -> Unit,
) {
    val syncer = RelaySyncer(client, store, state, log, idleTimeoutMs = opts.fetchTimeoutMs, verifyEvents = opts.verifyEvents)
    try {
        val relays =
            if (opts.discover) {
                Discovery(syncer, store, state, log).crawl(seedRelays, opts.maxRounds, opts.maxRelays).toList()
            } else {
                seedRelays
            }
        syncEvents(syncer, relays, opts, log)
        if (opts.scores) syncProviderScores(syncer, store, opts, log)
    } finally {
        SyncState.save(statePath, state)
        log("[state] saved cursors for ${state.relays.size} relay(s); pool=${state.relayPool.size}")
    }
}

/**
 * Phase 1: the kinds this run cares about, from every relay in the set —
 * deletions always, plus profiles (kind 0) and provider lists (kind 10040,
 * only useful when scores are being resolved).
 */
private suspend fun syncEvents(
    syncer: RelaySyncer,
    relays: List<String>,
    opts: SyncOptions,
    log: (String) -> Unit,
) {
    val kinds =
        buildList {
            if (opts.scores) add("10040")
            add("5")
            if (opts.profiles) add("0")
        }
    log("=== phase 1: ${kinds.joinToString(" / ")} from ${relays.size} relay(s) ===")
    for (r in relays) {
        val lists = if (opts.scores) syncer.sync(r, Filter(kinds = listOf(TrustProviderListEvent.KIND))) else null
        val dels = syncer.sync(r, Filter(kinds = listOf(DeletionEvent.KIND)))
        val profiles = if (opts.profiles) syncer.sync(r, Filter(kinds = listOf(MetadataEvent.KIND)), maxEvents = opts.maxEvents) else null
        log("[sync] ${short(r)}  10040=${lists?.let { "${it.inserted}${neg(it)}" } ?: "-"}  del=${dels.inserted}  kind0=${profiles?.inserted ?: "-"}")
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

    log("=== phase 3: kind 30382 per provider, from its relay hint ===")
    for ((service, relay) in providers) {
        val o = syncer.sync(relay, Filter(kinds = listOf(ContactCardEvent.KIND), authors = listOf(service)), maxEvents = opts.maxEvents)
        log("[sync] provider ${service.take(12)} @ ${short(relay.url)}: +${o.inserted}/${o.downloaded}${neg(o)}")
    }
}

private fun neg(o: RelaySyncer.Outcome) = if (o.usedNegentropy) " (neg)" else ""

private fun short(url: String) = url.removePrefix("wss://").removePrefix("ws://").trimEnd('/')
