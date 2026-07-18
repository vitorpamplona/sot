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

import com.vitorpamplona.quartz.nip01Core.core.Event
import com.vitorpamplona.quartz.nip01Core.core.HexKey
import com.vitorpamplona.quartz.nip01Core.metadata.MetadataEvent
import com.vitorpamplona.quartz.nip01Core.relay.filters.Filter
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.NormalizedRelayUrl
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.RelayUrlNormalizer
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.displayUrl
import com.vitorpamplona.quartz.nip01Core.store.IEventStore
import com.vitorpamplona.quartz.nip09Deletions.DeletionEvent
import com.vitorpamplona.quartz.nip62RequestToVanish.RequestToVanishEvent
import com.vitorpamplona.quartz.nip65RelayList.AdvertisedRelayListEvent
import com.vitorpamplona.quartz.nip85TrustedAssertions.list.TrustProviderListEvent
import com.vitorpamplona.quartz.nip85TrustedAssertions.users.ContactCardEvent
import com.vitorpamplona.sot.store.VespaEventStore
import com.vitorpamplona.sot.vespa.doc.CrawlIndex
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.cancelChildren
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.launch
import kotlinx.coroutines.selects.select
import kotlinx.coroutines.sync.Semaphore
import kotlinx.coroutines.sync.withPermit
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
    private val crawl: CrawlIndex,
) {
    // Two priority lanes over ONE worker pool. The HOUSE's chain rides
    // [primaryQueue]; every other observer (stored 10040 authors, NIP-42
    // enrollees) enters on [secondaryQueue]. Workers always drain the primary lane
    // first, so the house computes first and is available fast, while the others
    // run on the capacity it leaves — the "second round". Both are UNLIMITED, so
    // submit never blocks.
    private val primaryQueue = Channel<suspend () -> Unit>(Channel.UNLIMITED)
    private val secondaryQueue = Channel<suspend () -> Unit>(Channel.UNLIMITED)

    // Two independent in-flight counters, one per lane. The pipeline ADVANCES
    // (drains its batch buffers into the next stage) whenever the PRIMARY lane
    // alone goes idle ([primaryPending] == 0) — NOT when the whole pool is idle —
    // so the house's stages progress without waiting on the secondary backlog.
    private val primaryPending = AtomicInteger(0)
    private val secondaryPending = AtomicInteger(0)
    private val known = ConcurrentHashMap.newKeySet<HexKey>()

    // Every 10040 author the up-front broad sweep turned up (~all ~200 observers on
    // Nostr). A CANDIDATE, not yet an observer: only the ones the house's trust
    // graph actually scores (a scored subject shows up here AND gets a profile doc)
    // are activated, so we never index a perspective disconnected from the house.
    private val candidateObservers = ConcurrentHashMap.newKeySet<HexKey>()
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

    // The records plane, folded into the SAME pool (no scores->records barrier):
    // a scored author's content is fetched the moment their score lands. Each
    // author is assigned to their least-loaded write relay, so the load spreads.
    // The value is the author's content-sync state THIS pass (false = routed but
    // not yet reconciled, true = reconciled): one roster-scale map serves both
    // the "seen it this pass" dedup and the live coverage gauge's synced count,
    // instead of a second parallel set.
    private val knownContent = ConcurrentHashMap<HexKey, Boolean>()
    private val syncedThisPass = AtomicInteger(0)
    private val contentBuf = ConcurrentHashMap<NormalizedRelayUrl, MutableList<HexKey>>()

    /** First time this pass we've routed [author]; returns true only on the transition. */
    private fun markSeen(author: HexKey): Boolean = knownContent.putIfAbsent(author, false) == null

    // The convergence set: authors whose content we reconciled cleanly within the
    // refresh TTL, loaded ONCE at pass start. They are SKIPPED this pass, so a
    // load only ever works the not-yet-synced (or gone-stale) remainder — the
    // fix for "every restart re-processes the whole roster". Authors synced
    // DURING this pass are deduped by [knownContent] instead, and re-enter next
    // pass's set once their sync ages past the TTL (the refresh cadence).
    @Volatile private var alreadySynced: Set<HexKey> = emptySet()

    // Fallback content sources for authors with NO 10002: the big relays that
    // actually aggregate notes (the profile/index aggregators hold no content). A
    // no-10002 author is spread across a hash-picked [FALLBACK_FANOUT] of these so
    // the load fans out across the whole set instead of piling every such author
    // onto the same two or three relays (which then jam the pool).
    private val contentRelays =
        listOf(
            "wss://nos.lol",
            "wss://relay.damus.io",
            "wss://relay.primal.net",
            "wss://nostr.mom",
            "wss://relay.nostr.band",
            "wss://nostr.wine",
            "wss://relay.snort.social",
            "wss://eden.nostr.land",
        ).mapNotNull { RelayUrlNormalizer.normalizeOrNull(it) }
    private val recordKinds = IndexableKinds.kinds + DeletionEvent.KIND + RequestToVanishEvent.KIND

    // The content plane runs a dedicated PUMP per relay so relays drain
    // INDEPENDENTLY: a busy or huge relay never suspends a shared worker that could
    // be serving another relay (the old shared pool collapsed to ~one relay's worth
    // of slots). Pumps live in [passScope]; [contentGate] bounds TOTAL concurrent
    // content fetches, and each pump caps its own relay at [MAX_PER_RELAY_CONTENT].
    // A relay that keeps returning few events per author is DISCARDED — dropped and
    // taken off routing — because those authors also ride their other write relays.
    private lateinit var passScope: CoroutineScope
    private val contentGate = Semaphore(opts.relayConcurrency.coerceAtLeast(1))
    private val contentPumps = ConcurrentHashMap<NormalizedRelayUrl, Channel<List<HexKey>>>()
    private val lowYieldStreak = ConcurrentHashMap<NormalizedRelayUrl, AtomicInteger>()
    private val discardedRelays = ConcurrentHashMap.newKeySet<NormalizedRelayUrl>()

    // 10002-lookup health, per pass. A broken aggregator that times out
    // [LOOKUP_DEAD_AFTER] times is skipped for the rest of the pass so it can't
    // gate resolution. [knownRelays] accumulates the write relays of lists we DID
    // resolve — the second-tier lookup pool (most-common first) for authors the
    // aggregators don't carry.
    private val lookupTimeouts = ConcurrentHashMap<NormalizedRelayUrl, AtomicInteger>()
    private val deadLookups = ConcurrentHashMap.newKeySet<NormalizedRelayUrl>()
    private val knownRelays = ConcurrentHashMap<NormalizedRelayUrl, AtomicInteger>()

    suspend fun run(initialObservers: Set<HexKey>) =
        coroutineScope {
            passScope = this // content pumps launch here, so the pass waits for them
            syncer.resetSeen() // fresh per-pass duplicate filter
            // Load the "already synced within the TTL" set once: these authors are
            // skipped this pass so the crawl converges instead of re-pulling the
            // whole roster every run. A read failure just means we skip nobody
            // (correct, only slower), so it must never abort the pass.
            alreadySynced = runCatching { crawl.syncedSince(nowSecs() - opts.refreshTtlSecs) }.getOrDefault(emptySet())
            if (alreadySynced.isNotEmpty()) log("[records] ${alreadySynced.size} author(s) synced within TTL - skipping this pass")
            // The reserved refresh slice: front-load the stalest already-synced
            // authors so their NEW posts are re-pulled BEFORE the backlog, instead
            // of freshness waiting for a multi-day load to drain. They're marked in
            // [knownContent] so the chain later dedups them; routing them here (on
            // the secondary lane) reserves a bounded slice of the pass for refresh.
            if (opts.refreshBudget > 0) {
                val due = runCatching { crawl.dueForRefresh(nowSecs() - opts.refreshTtlSecs, opts.refreshBudget) }.getOrDefault(emptyList())
                val toRefresh = due.filter(::markSeen)
                if (toRefresh.isNotEmpty()) {
                    log("[refresh] re-pulling ${toRefresh.size} stalest synced author(s) ahead of the backlog")
                    submitSecondary { routeAuthors(toRefresh) }
                }
            }
            // Live coverage: fully-synced (carried over + this pass) out of the roster
            // discovered so far (skipped + newly routed). A running progress bar for
            // "how much of the observer's network is indexed".
            progress.gauge {
                val done = alreadySynced.size + syncedThisPass.get()
                val seen = alreadySynced.size + knownContent.size
                if (seen == 0) "" else "cov ${SyncProgress.compact(done.toLong())}/${SyncProgress.compact(seen.toLong())}"
            }
            // The house is the trust ROOT and the ONLY primary: it computes first
            // (primary lane) so its 10002/kind-0/10040, scores, and content are
            // available fast. Every other observer — stored 10040 authors and
            // NIP-42 enrollees — rides the secondary lane and computes on whatever
            // capacity the house leaves (the "second round"). There is NO broad
            // relay crawl: the users we index are exactly the subjects the trust
            // graph names (each observer's providers' 30382 targets), all fetched
            // author-bounded, so nothing unbounded (or spammable) enters the sync.
            val housePk = house?.pubkey
            registerObservers(listOfNotNull(housePk), primary = true)
            registerObservers((initialObservers + storedObservers()) - setOfNotNull(housePk), primary = false)
            // kind 10040 is RARE — only ~100-200 people on all of Nostr publish one —
            // so a BROAD kind-10040 query is cheap and near-complete on a major relay
            // (nos.lol alone returns ~all of them in under a second). Sweep the lookup
            // relays for it up front so every observer's perspective indexes fast,
            // instead of trickling in one-by-one as we happen to fetch their content.
            for (relay in indexRelays) submitSecondary { sweepObservers(relay) }
            // No initial units doesn't mean no work. An observer with no
            // discoverable 10002 lands in the no-list fallback, and stored 10040s
            // feed the catch-up; both surface through onIdle. Only a pass that
            // STILL finds nothing after that has nothing to sync.
            if (primaryPending.get() == 0 && secondaryPending.get() == 0 && !onIdle()) {
                log("[sync] no house account, no config extras, and no stored 10040s - nothing to sync")
                closeQueues()
            }
            repeat(opts.relayConcurrency.coerceAtLeast(1)) {
                launch {
                    // Each job carries its own try/catch + lane accounting (see
                    // [submit]/[submitSecondary]); the worker just runs the next one.
                    while (true) (nextJob() ?: break).invoke()
                }
            }
        }

    /**
     * The next unit to run, primary-first. A non-blocking peek at [primaryQueue] wins
     * outright when it has work; only when it's empty do we suspend on EITHER lane.
     * Returns null once both lanes are closed and drained, ending the worker.
     */
    private suspend fun nextJob(): (suspend () -> Unit)? =
        primaryQueue.tryReceive().getOrNull() ?: select {
            primaryQueue.onReceiveCatching { it.getOrNull() }
            secondaryQueue.onReceiveCatching { it.getOrNull() }
        }

    private fun submit(job: suspend () -> Unit) {
        primaryPending.incrementAndGet()
        progress.addPhaseItems(1)
        primaryQueue.trySend {
            runUnit(job)
            primaryDone()
        }
    }

    /** Secondary lane: a non-house observer's entry, run only on capacity the house leaves idle. */
    private fun submitSecondary(job: suspend () -> Unit) {
        secondaryPending.incrementAndGet()
        progress.addPhaseItems(1)
        secondaryQueue.trySend {
            runUnit(job)
            secondaryDone()
        }
    }

    /** Run one unit, logging (not propagating) real failures; cancellation tears the pass down. */
    private suspend fun runUnit(job: suspend () -> Unit) {
        try {
            job()
        } catch (e: CancellationException) {
            throw e
        } catch (e: Exception) {
            log("  ! sync unit failed: ${e.message}")
        }
    }

    private fun closeQueues() {
        primaryQueue.close()
        secondaryQueue.close()
        // The chain is done, so no more content will be routed: close every pump's
        // inbox. Each pump drains whatever it already holds, then exits — and since
        // pumps are children of [passScope], the pass waits for that content to land.
        contentPumps.values.forEach { it.close() }
    }

    /**
     * A primary unit finished. When the primary lane alone goes idle, ADVANCE the
     * pipeline: drain the per-relay batch buffers into the next stage (and, once,
     * the stored-10040 catch-up). This runs while the secondary lane is still busy,
     * so the house's stages don't wait on it. The pass ends only when both lanes
     * are idle and there's nothing left to drain.
     */
    private suspend fun primaryDone() {
        if (primaryPending.decrementAndGet() > 0) return
        if (onIdle()) return
        if (secondaryPending.get() == 0) closeQueues()
    }

    /** A secondary observer's entry finished; if the primary lane is also idle, advance/close. */
    private suspend fun secondaryDone() {
        if (secondaryPending.decrementAndGet() > 0) return
        if (primaryPending.get() > 0) return // the chain's own drain will finish the pass
        if (!onIdle()) closeQueues()
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
        // Content rides the per-relay pumps, NOT the shared pool — so draining a
        // partial batch to a pump does not count as pool work (`submitted`); the
        // pumps' own lifecycle (closed at [closeQueues], awaited by [passScope])
        // carries it to completion.
        for ((relay, batch) in drain(contentBuf)) enqueueContent(relay, batch)
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
    private suspend fun registerObservers(
        observers: Collection<HexKey>,
        primary: Boolean = false,
    ) {
        val fresh = observers.filter(known::add)
        if (fresh.isEmpty()) return
        for (o in fresh) {
            listsLeft[o] = AtomicInteger(indexRelays.size + if (house?.pubkey == o) 1 else 0)
        }
        // The house's lookups go on the primary lane so it computes first; every
        // other observer enters on the secondary lane and runs on spare capacity.
        val enqueue: (suspend () -> Unit) -> Unit = if (primary) ::submit else ::submitSecondary
        for (relay in indexRelays) {
            fresh.chunked(AUTHORS_PER_FILTER).forEach { batch -> enqueue { listUnit(relay, batch) } }
        }
        house?.takeIf { it.pubkey in fresh }?.let { h -> enqueue { listUnit(h.relay, listOf(h.pubkey)) } }
        // Nowhere to look a 10002 up: resolve from whatever the store already has.
        if (indexRelays.isEmpty()) {
            resolveOutboxes(fresh.filter { house?.pubkey != it })
        }
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
            // Drop unreachable write relays (a private/local relay in the 10002);
            // an observer left with none falls back to the index relays below.
            val relays = RelayUrls.publiclyRoutable(byAuthor[o]?.writeRelaysNorm().orEmpty())
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
        // Rank per subject captured for THIS batch only (freed when the unit ends),
        // so ordering never accumulates a roster-scale global map. onVerified can
        // fire from parallel verify workers, hence the concurrent map.
        val ranks = ConcurrentHashMap<HexKey, Int>()
        if (opts.maxEvents == 0) {
            // Full sync: reconcile the batch's complete id set; absence IS
            // deletion here (the relay is authoritative for this scope).
            val r = syncer.reconcile(relay, scores, forceEnumerate = opts.reconcileScores, onVerified = { captureRanksInto(it, ranks) })
            // The absence-is-deletion diff is best-effort: it reads the store via
            // a visit, which can 429 under load. A failure here must NOT abort the
            // unit (the scores already landed) or skip the records trigger below.
            if (r.relayIds != null) {
                runCatching {
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
                }.onFailure { log("  ! reconcile diff @ ${relay.displayUrl()} skipped: ${it.message}") }
            }
            log("[chain ${progress.itemDone()}/${progress.position().substringAfter('/')}] ${services.size} provider(s) @ ${relay.displayUrl()}: +${r.inserted}${if (r.usedNegentropy) " (neg)" else ""}")
        } else {
            // Bounded experiment: incremental slice only, no deletion diff (a
            // capped download must never be read as "the rest was deleted").
            val o = syncer.sync(relay, scores, maxEvents = opts.maxEvents, onVerified = { captureRanksInto(it, ranks) })
            log("[chain ${progress.itemDone()}/${progress.position().substringAfter('/')}] ${services.size} provider(s) @ ${relay.displayUrl()}: +${o.inserted}/${o.downloaded}${neg(o)}")
        }
        // Records plane, in the same pool: the authors these services just scored
        // become content targets immediately — no waiting for every score to land.
        registerContentAuthors(scoredSubjects(services), ranks)
    }

    /**
     * The distinct subjects (scored authors) these [services] rank, read back from
     * the stored 30382s — the `d` tag IS the subject pubkey. Read via an UNCAPPED
     * document visit: a single provider scores hundreds of thousands of subjects,
     * and a capped search would truncate the set so we'd sync content for only a
     * slice of the people we track. (A non-Vespa store falls back to the query.)
     */
    private suspend fun scoredSubjects(services: List<HexKey>): List<HexKey> {
        val filter = Filter(kinds = listOf(ContactCardEvent.KIND), authors = services)
        return (store as? VespaEventStore)?.distinctDTags(filter)?.toList()
            ?: store
                .query<ContactCardEvent>(filter)
                .filter { it.rank() != null }
                .mapNotNull { card ->
                    card.tags
                        .firstOrNull { it.size > 1 && it[0] == "d" }
                        ?.get(1)
                        ?.takeIf(String::isNotEmpty)
                }.distinct()
    }

    /**
     * Broad kind-10040 sweep of one relay. 10040 is rare (~200 people on all of
     * Nostr), so this is cheap and near-complete on a major relay — it just
     * COLLECTS candidate observers; [registerContentAuthors] activates only the
     * ones the house's trust graph actually scores.
     */
    private suspend fun sweepObservers(relay: NormalizedRelayUrl) {
        runCatching {
            syncer.sync(
                relay,
                Filter(kinds = listOf(TrustProviderListEvent.KIND)),
                onVerified = { events -> candidateObservers.addAll(events.filterIsInstance<TrustProviderListEvent>().map { it.pubKey }) },
            )
        }
    }

    /** New scored authors join the records side: activate any connected observers among them, resolve, then fetch content. */
    private suspend fun registerContentAuthors(
        authors: List<HexKey>,
        ranks: Map<HexKey, Int>,
    ) {
        // Skip authors reconciled within the TTL (loaded at pass start), THEN dedup
        // the rest within this pass. Order matters: a skipped author must not enter
        // [knownContent], or a later batch that includes them would also skip them
        // for the wrong reason — harmless here, but the intent is "not our work
        // this pass", tracked in one place.
        val fresh =
            authors
                .filter { it !in alreadySynced && markSeen(it) }
                // Highest-rank subjects first, so their content is routed (and
                // fetched, per the pumps' FIFO) before the long tail — an
                // interrupted load keeps the most-trusted authors. Ranks are this
                // batch's; a subject not in it (or from a prior pass) sorts last.
                .sortedByDescending { ranks[it] ?: 0 }
        routeAuthors(fresh)
    }

    /**
     * Resolve [authors]' outboxes and route their content — the shared body of the
     * chain path ([registerContentAuthors]) and the refresh slice. Callers have
     * already deduped via [knownContent] and ordered the list; this just does the
     * work. Preserves the caller's order (rank for the chain, staleness for refresh).
     */
    private suspend fun routeAuthors(authors: List<HexKey>) {
        if (authors.isEmpty()) return
        // Stamp "we resolved these this pass", so the coverage report can tell a
        // confirmed no-outbox author from one whose 10002 simply hasn't been
        // fetched yet. Best-effort: a failure just leaves them "unresolved".
        runCatching { crawl.markOutboxChecked(authors, nowSecs()) }
        // A scored subject that is ALSO a swept observer is CONNECTED to the house
        // (its trust graph scores them), so index their perspective. The swept
        // observers the house doesn't score stay dormant — never activated.
        registerObservers(authors.filter { it in candidateObservers })
        val routed = ConcurrentHashMap.newKeySet<HexKey>()
        // Tier 1 (the aggregators — purplepag.es et al hold ~everyone's profile and
        // list) routes each author's content the MOMENT their 10002 lands, so
        // content flows AS tier-1 resolves rather than after the whole batch. A
        // profile must not depend on us happening to fetch content from a relay
        // that has it, so profiles come from here too.
        resolveIdentity(authors, tier2 = false) { author, writeRelays ->
            if (routed.add(author)) routeContent(author, writeRelays)
        }
        // Whoever tier-1 found no list for: route to the index relays now.
        for (a in authors) if (routed.add(a)) routeContent(a, emptyList())
        // Tier 2 (chasing stragglers across the broader working-relay list) is
        // best-effort — it mostly finds that the missing lists/profiles are simply
        // unpublished — so it runs in the BACKGROUND and must NEVER block content.
        submitSecondary { resolveIdentity(authors, tier2 = true) }
    }

    /**
     * Fetch [author]'s content from ALL their reachable write relays (deduped by the
     * seen-filter). A user's notes are spread across their outbox — relays prune, and
     * only some hold the full history — so picking ONE relay routinely missed most of
     * their content (the same author had 500 notes on nos.lol and 0 stored because
     * we'd chosen a different, emptier write relay). No 10002 at all? fall back to the
     * major CONTENT relays; the profile aggregators hold no notes.
     */
    private fun routeContent(
        author: HexKey,
        writeRelays: List<NormalizedRelayUrl>,
    ) {
        // Cap own write relays at [MAX_CONTENT_RELAYS]: content is replicated across
        // the outbox, so a few cover it. No 10002 -> a hash-spread slice of the
        // content relays, so no-10002 authors fan out instead of all landing on the
        // same relays. Each batch goes to that relay's PUMP, not the shared pool.
        val relays = RelayUrls.publiclyRoutable(writeRelays).take(MAX_CONTENT_RELAYS).ifEmpty { fallbackRelays(author) }
        for (relay in relays) {
            if (relay in discardedRelays) continue
            bufferedBatch(contentBuf, relay, author)?.let { batch -> enqueueContent(relay, batch) }
        }
    }

    private fun nowSecs() = System.currentTimeMillis() / 1000

    /** Record each 30382's rank for its subject (the `d` tag) into [target], keeping the max seen. */
    private fun captureRanksInto(
        events: List<Event>,
        target: ConcurrentHashMap<HexKey, Int>,
    ) {
        for (e in events) {
            if (e.kind != ContactCardEvent.KIND) continue
            val subject =
                e.tags
                    .firstOrNull { it.size > 1 && it[0] == "d" }
                    ?.get(1)
                    ?.takeIf(String::isNotEmpty) ?: continue
            val rank = (e as? ContactCardEvent)?.rank() ?: continue
            target.merge(subject, rank) { a, b -> maxOf(a, b) }
        }
    }

    /** A hash-picked [FALLBACK_FANOUT] slice of the content relays, so no-10002 authors spread evenly across the whole set. */
    private fun fallbackRelays(author: HexKey): List<NormalizedRelayUrl> {
        if (contentRelays.isEmpty()) return emptyList()
        val start = (author.hashCode() and Int.MAX_VALUE) % contentRelays.size
        return (0 until FALLBACK_FANOUT.coerceAtMost(contentRelays.size)).map { contentRelays[(start + it) % contentRelays.size] }
    }

    /** Hand a content batch to its relay's pump, spinning the pump up on first use. */
    private fun enqueueContent(
        relay: NormalizedRelayUrl,
        batch: List<HexKey>,
    ) {
        if (relay in discardedRelays) return
        // Count the batch here (the pool's submit did this for the old contentUnit)
        // so the records X/Y meter stays balanced against fetchContent's itemDone().
        progress.addPhaseItems(1)
        pumpFor(relay).trySend(batch)
    }

    // computeIfAbsent (atomic — runs the builder at most once per key) so a race can
    // never launch a second pump on a channel that isn't in the map: that orphan
    // would loop on a never-closed inbox forever and the pass would never end.
    private fun pumpFor(relay: NormalizedRelayUrl): Channel<List<HexKey>> =
        contentPumps.computeIfAbsent(relay) {
            Channel<List<HexKey>>(Channel.UNLIMITED).also { ch -> passScope.launch { runPump(relay, ch) } }
        }

    /**
     * One relay's content pump. It drains that relay's batch queue, running at most
     * [MAX_PER_RELAY_CONTENT] fetches for this relay at once and bounded globally by
     * [contentGate] — so this relay's work overlaps every OTHER relay's pump instead
     * of queuing behind a shared worker. The enclosing [coroutineScope] awaits the
     * in-flight fetches before the pump exits, and the pump exits when its inbox is
     * closed (chain done) and drained.
     */
    private suspend fun runPump(
        relay: NormalizedRelayUrl,
        inbox: Channel<List<HexKey>>,
    ) = coroutineScope {
        val relayGate = Semaphore(MAX_PER_RELAY_CONTENT)
        for (batch in inbox) {
            if (relay in discardedRelays) {
                progress.itemDone() // counted at enqueue but dropped — keep the meter balanced
                continue
            }
            relayGate.acquire()
            launch {
                try {
                    contentGate.withPermit { fetchContent(relay, batch) }
                } catch (e: CancellationException) {
                    throw e // a real cancellation (pass teardown) must propagate
                } catch (e: Exception) {
                    // A single relay's failure must NOT cancel the pass (the pump is a
                    // child of passScope) — swallow it like the pool's runUnit does.
                    log("  ! content fetch failed @ ${relay.displayUrl()}: ${e.message}")
                } finally {
                    relayGate.release()
                }
            }
        }
    }

    /** One (relay x author batch) content download, with fast-discard of low-yield relays. */
    private suspend fun fetchContent(
        relay: NormalizedRelayUrl,
        authors: List<HexKey>,
    ) {
        val o = syncer.sync(relay, Filter(kinds = recordKinds, authors = authors), maxEvents = opts.maxEvents)
        // A clean finish means we pulled ALL of these authors' content from this
        // relay — mark them fully content-indexed. A timeout doesn't count.
        if (o.completed) {
            syncer.state.markContentDone(authors)
            // Count each author's FIRST clean reconcile this pass (an author spread
            // across write relays completes on several — dedup on the false->true
            // transition so the gauge's synced count stays honest).
            for (a in authors) if (knownContent.replace(a, false, true)) syncedThisPass.incrementAndGet()
            // The persisted, roster-scale ledger the NEXT pass reads to skip these
            // authors (convergence) and the coverage report groups over. A ledger
            // write failure must not fail the content fetch that already landed.
            runCatching { crawl.markSynced(authors, nowSecs()) }
                .onFailure { log("  ! crawl ledger markSynced failed @ ${relay.displayUrl()}: ${it.message}") }
        }
        log("[records ${progress.itemDone()}/${progress.position().substringAfter('/')}] ${authors.size} author(s) @ ${relay.displayUrl()}: +${o.inserted}/${o.downloaded}${neg(o)}")
        // Fast discard: a relay that keeps returning few events per author isn't
        // holding these authors' content, so it just wastes a pump slot. Drop it (its
        // authors ride their OTHER write relays). The curated content relays are the
        // no-10002 safety net and are never discarded.
        val perAuthor = if (authors.isEmpty()) Int.MAX_VALUE else o.downloaded / authors.size
        if (perAuthor < DISCARD_YIELD && relay !in contentRelays) {
            if (lowYieldStreak.getOrPut(relay) { AtomicInteger() }.incrementAndGet() >= DISCARD_AFTER) discardRelay(relay)
        } else {
            lowYieldStreak[relay]?.set(0)
        }
    }

    /** Stop using [relay] for content this pass: close its pump (drops the queue) and take it off routing. */
    private fun discardRelay(relay: NormalizedRelayUrl) {
        if (discardedRelays.add(relay)) {
            contentPumps[relay]?.close()
            log("  [${relay.displayUrl()}] low content yield - discarded for this pass")
        }
    }

    /**
     * Resolve [authors]' kind-0 profile AND kind-10002 relay list from the
     * aggregators, first-success UNION: tier 1 is the index/aggregator relays, then
     * tier 2 the relays real users write to (surfaced by lists we already resolved).
     * Each author leaves `needProfile`/`needList` as their events arrive, and we
     * keep querying until both are covered or the relays are exhausted. Profiles
     * live on the aggregators (purplepag.es holds ~everyone), so fetching them HERE
     * — not bundled into a per-author content pull from one write relay — is what
     * lifts coverage toward 100%. A debug line reports what actually resolved.
     */
    private suspend fun resolveIdentity(
        authors: List<HexKey>,
        tier2: Boolean,
        onList: ((HexKey, List<NormalizedRelayUrl>) -> Unit)? = null,
    ) {
        if (authors.isEmpty()) return
        // Recompute what's still missing from the store — for the tier-2 background
        // pass this runs AFTER tier-1 landed, so it only chases the real remainder.
        val haveProfile = storedProfiles(authors)
        val haveList = storedRelayLists(authors).keys
        val needProfile = ConcurrentHashMap.newKeySet<HexKey>().apply { addAll(authors.filterNot { it in haveProfile }) }
        val needList = ConcurrentHashMap.newKeySet<HexKey>().apply { addAll(authors.filterNot { it in haveList }) }
        if (needProfile.isEmpty() && needList.isEmpty()) return
        val n = authors.size
        val relays =
            if (!tier2) {
                indexRelays
            } else {
                knownRelays.entries
                    .sortedByDescending { it.value.get() }
                    .map { it.key }
                    .filterNot { it in indexRelays }
                    .take(EXPAND_RELAYS)
            }
        val p0 = n - needProfile.size
        val l0 = n - needList.size
        // Tier-1 gives slow-but-alive aggregators the full idle window; tier-2 is a
        // best-effort background straggler chase, so it uses a short one — a relay
        // either has the list fast or it doesn't, and this keeps tier-2 from
        // grinding for hours on the unresolvable tail.
        fanOutIdentity(relays, needProfile, needList, if (tier2) TIER2_IDLE_MS else LOOKUP_IDLE_MS, onList)
        log(
            "  [identity ${if (tier2) "t2" else "t1"}] $n author(s): profiles $p0->${n - needProfile.size}/$n, " +
                "lists $l0->${n - needList.size}/$n (${relays.size} relays, ${deadLookups.size} dead)",
        )
    }

    /**
     * One tier's fan-out resolving kind 0 + 10002 for [needProfile]/[needList] in
     * parallel, FIRST-SUCCESS: each event is taken from whichever relay serves it
     * first, and the stragglers are cancelled once BOTH needs are empty — so no
     * single relay gates the batch. A relay that times out [LOOKUP_DEAD_AFTER] times
     * is dead-relayed for the rest of the pass, and the per-call idle timeout is
     * short ([LOOKUP_IDLE_MS]) so even a first contact with a slow relay costs
     * seconds, not the syncer-wide 10s.
     */
    private suspend fun fanOutIdentity(
        relays: List<NormalizedRelayUrl>,
        needProfile: MutableSet<HexKey>,
        needList: MutableSet<HexKey>,
        idleMs: Long = LOOKUP_IDLE_MS,
        onList: ((HexKey, List<NormalizedRelayUrl>) -> Unit)? = null,
    ) {
        val live = relays.filterNot { it in deadLookups }
        if (live.isEmpty() || (needProfile.isEmpty() && needList.isEmpty())) return
        val snapshot = (needProfile + needList).toList()
        coroutineScope {
            val group = this
            for (relay in live) {
                launch {
                    for (batch in snapshot.chunked(AUTHORS_PER_FILTER)) {
                        if ((needProfile.isEmpty() && needList.isEmpty()) || relay in deadLookups) break
                        val want = batch.filter { it in needProfile || it in needList }
                        if (want.isEmpty()) continue
                        val o =
                            syncer.sync(
                                relay,
                                Filter(kinds = listOf(MetadataEvent.KIND, AdvertisedRelayListEvent.KIND), authors = want),
                                maxEvents = opts.maxEvents,
                                idleMs = idleMs,
                                onVerified = { events ->
                                    events.forEach { e ->
                                        when (e) {
                                            is MetadataEvent -> {
                                                needProfile.remove(e.pubKey)
                                            }

                                            is AdvertisedRelayListEvent -> {
                                                needList.remove(e.pubKey)
                                                val wr = e.writeRelaysNorm().orEmpty()
                                                wr.forEach { r -> knownRelays.getOrPut(r) { AtomicInteger() }.incrementAndGet() }
                                                onList?.invoke(e.pubKey, wr)
                                            }

                                            else -> {}
                                        }
                                    }
                                    if (needProfile.isEmpty() && needList.isEmpty()) group.coroutineContext.cancelChildren()
                                },
                            )
                        // A timeout (not a clean "I don't have them") counts toward dead-relaying.
                        if (!o.completed && lookupTimeouts.getOrPut(relay) { AtomicInteger() }.incrementAndGet() >= LOOKUP_DEAD_AFTER) {
                            deadLookups.add(relay)
                            break
                        }
                    }
                }
            }
        }
    }

    private suspend fun storedProfiles(authors: List<HexKey>): Set<HexKey> =
        authors
            .chunked(AUTHORS_PER_FILTER)
            .flatMap { batch -> store.query<MetadataEvent>(Filter(kinds = listOf(MetadataEvent.KIND), authors = batch)) }
            .map { it.pubKey }
            .toSet()

    private suspend fun storedRelayLists(authors: List<HexKey>): Map<HexKey, AdvertisedRelayListEvent> =
        authors
            .chunked(AUTHORS_PER_FILTER)
            .flatMap { batch -> store.query<AdvertisedRelayListEvent>(Filter(kinds = listOf(AdvertisedRelayListEvent.KIND), authors = batch)) }
            .associateBy { it.pubKey }

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

    companion object {
        // A 10002 lookup hears back fast or not at all, so give a slow/broken relay
        // seconds, not the syncer-wide 10s, before moving on.
        private const val LOOKUP_IDLE_MS = 10_000L

        // Consecutive-ish timeouts before a lookup relay is skipped for the pass.
        private const val LOOKUP_DEAD_AFTER = 3

        // Tier-2 fallback breadth: how many of the working relays (the write relays
        // real users advertise, most-common first) to try for authors the
        // aggregators don't carry. Kept SMALL on purpose: probing the biggest relays
        // showed the profiles/lists we miss are genuinely UNPUBLISHED (0/50 sampled
        // missing 10002s existed on purplepag.es/damus/nos.lol/primal), so a wide
        // fan-out only grinds the unresolvable tail for hours without finding
        // anything. A resolvable straggler on a popular relay is still caught here.
        private const val EXPAND_RELAYS = 15

        // Short idle window for the tier-2 background chase (vs the 10s tier-1 uses):
        // a lookup answers fast or not at all, so this bounds how long tier-2 can grind.
        private const val TIER2_IDLE_MS = 3_000L

        // Write relays to pull an author's content from — a few cover the replicated outbox.
        private const val MAX_CONTENT_RELAYS = 4

        // Content relays a no-10002 author fans out to (hash-picked from contentRelays).
        private const val FALLBACK_FANOUT = 2

        // Concurrent content fetches a SINGLE relay's pump runs at once.
        private const val MAX_PER_RELAY_CONTENT = 4

        // Discard a relay for content after this many consecutive batches that
        // averaged fewer than DISCARD_YIELD events per author — it clearly isn't
        // holding these authors' content, so it only wastes a pump slot.
        private const val DISCARD_YIELD = 3
        private const val DISCARD_AFTER = 2
    }
}
