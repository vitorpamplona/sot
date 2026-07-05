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
import com.vitorpamplona.quartz.nip01Core.store.IEventStore
import com.vitorpamplona.quartz.nip09Deletions.DeletionEvent
import com.vitorpamplona.quartz.nip62RequestToVanish.RequestToVanishEvent
import com.vitorpamplona.quartz.nip65RelayList.AdvertisedRelayListEvent
import com.vitorpamplona.quartz.nip85TrustedAssertions.list.TrustProviderListEvent
import com.vitorpamplona.quartz.nip85TrustedAssertions.list.tags.ProviderTypes
import com.vitorpamplona.quartz.nip85TrustedAssertions.users.ContactCardEvent
import kotlinx.coroutines.CancellationException
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
     * Pool width for the relay-centric scheduler ([BlendedPass]). Relays are
     * I/O-stalled, so this oversubscribes past [concurrency] (the store-bound
     * number) to keep the feed full. The ceiling is Vespa's document API, which
     * REJECTS past 256 enqueued requests (a real run 429'd at 256): every pool
     * worker can have a visit/search in flight, so keep it comfortably under.
     */
    val relayConcurrency: Int = 128,
    /**
     * Force the authoritative silent-deletion pass on pages-only provider
     * relays too (full enumeration plus diff). Negentropy-capable relays detect
     * silent deletions automatically on every full sync, so this is only for
     * providers whose relay can't reconcile. It is costly, so run it on a slow
     * cadence.
     */
    val reconcileScores: Boolean = false,
    /** Verify id + signature before storing. Test-only seam — leave on. */
    val verifyEvents: Boolean = true,
    /**
     * Discovery crawl: cap on distinct (collapsed) relays swept for 10040 in a
     * pass.
     *
     * A sync is ALL-OR-NOTHING: it always discovers (snowball-sweeps write relays
     * for kind-10040 trust lists), always syncs the scores those 10040s point to,
     * and always pulls the RECORDS plane (the searchable content — every
     * [IndexableKinds] kind — for the scored authors, the full firehose). There is
     * no toggle to run only part of it: a partial pass leaves the store partial and
     * advances per-relay cursors such that a later fuller pass can't cleanly
     * backfill (see docs/inverted-relay-sync.md). This knob and [maxDiscoveryHarvest]
     * only TUNE the always-on crawl, which snowballs from the seeds through the pool
     * (no rounds/barriers): every write relay a harvest turns up feeds the next
     * sweep mid-flight, bounded only by this relay budget.
     */
    val maxDiscoveryRelays: Int = 2_000,
    /** Discovery crawl: 10002s to sample per relay — enough to surface its relay URLs, not its whole set. */
    val maxDiscoveryHarvest: Int = 5_000,
)

/**
 * The observer behind every unauthenticated search: a real user whose web of
 * trust defines the relay's default ranking. [relay] is their home relay, where
 * their FIRST 10002 is synced from, with no dependence on indexer coverage.
 * From there the standard chain runs.
 */
data class HouseAccount(
    val pubkey: HexKey,
    val relay: NormalizedRelayUrl,
)

/**
 * The SCORES plane (docs/v2-sync-proposal.md, phase 1): per-observer trust
 * data, synced author-first. The dependency order keeps 10040s authoritative,
 * so **10002 resolution precedes every per-author sync**, because the freshest
 * 10040 lives on the observer's own outbox relay:
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
 * OUT of the store. Replaceable supersession is what makes "authoritative" need
 * no special code: whatever source a 10040 arrives from, the newest wins, and
 * the outbox pass just makes sure the newest is actually seen. Provider
 * switches need no invalidation logic either. The sweep deletes any provider's
 * 30382s the moment no 10040 lists it, and the ranking projection (`:profile`)
 * re-derives the observer's cells from what remains.
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
        log("=== scores + records: relay-centric pool (${seedRelays.size} seed / ${indexRelays.size} index relay(s)) ===")
        progress.startPhase("chain", 0)
        // One relay-centric pool drives BOTH planes: a scored author's content is
        // fetched the moment their score lands, so records never waits for the
        // whole scores plane to finish (see docs/inverted-relay-sync.md).
        BlendedPass(syncer, store, opts, progress, log, indexRelays, house).run(observers + setOfNotNull(house?.pubkey), seedRelays)
        sweepOrphanScores()
    }

    /**
     * Phase D: the stateless correctness backstop for provider switches. A
     * 30382 is stale when no stored 10040 lists its author (the service key)
     * for `30382:rank` anymore. Stale 30382s pollute ranking, so they are
     * deleted. This catches every path to staleness (a provider switch, an
     * observer removed, a 10040 deleted or vanished); the change-feed reactions
     * only buy promptness, never correctness. The ranking projection re-derives
     * each observer's cells automatically from what remains.
     */
    private suspend fun sweepOrphanScores() {
        val referenced =
            store
                .query<TrustProviderListEvent>(Filter(kinds = listOf(TrustProviderListEvent.KIND)))
                .flatMap { l -> l.rankProviders().map { it.pubkey } }
                .toSet()
        // Full enumeration for now. A Vespa grouping query (distinct 30382
        // authors in one round trip) will replace this when the corpus grows.
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
}

/** The rank-scoring providers a 10040 nominates (service key + its relay) — the one place that rule lives. */
internal fun TrustProviderListEvent.rankProviders() = serviceProviders().filter { it.service == ProviderTypes.rank }

/** Relays reject filters with unboundedly many authors; fold author sets into batches this size. */
internal const val AUTHORS_PER_FILTER = 500

/**
 * What an observer's outbox owes the scores plane: their profile, a fresher
 * 10002, the AUTHORITATIVE 10040, and their own 5/62. A deletion published only
 * to the author's outbox must still erase here, and the store interprets both.
 */
internal val OUTBOX_KINDS =
    listOf(
        MetadataEvent.KIND,
        AdvertisedRelayListEvent.KIND,
        TrustProviderListEvent.KIND,
        DeletionEvent.KIND,
        RequestToVanishEvent.KIND,
    )

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
                    // One item's failure doesn't stop the rest — but cancellation must propagate.
                    try {
                        body(item)
                    } catch (e: CancellationException) {
                        throw e
                    } catch (e: Exception) {
                        // swallowed: the pass tolerates a single unit failing
                    }
                }
            }
        }.joinAll()
}
