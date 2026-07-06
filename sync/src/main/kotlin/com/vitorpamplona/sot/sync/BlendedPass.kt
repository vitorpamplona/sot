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
import com.vitorpamplona.quartz.nip85TrustedAssertions.users.ContactCardEvent
import com.vitorpamplona.sot.store.VespaEventStore
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.cancelChildren
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.launch
import kotlinx.coroutines.selects.select
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
    private val knownContent = ConcurrentHashMap.newKeySet<HexKey>()
    private val contentBuf = ConcurrentHashMap<NormalizedRelayUrl, MutableList<HexKey>>()
    private val relayLoad = ConcurrentHashMap<NormalizedRelayUrl, AtomicInteger>()
    private val recordKinds = IndexableKinds.kinds + DeletionEvent.KIND + RequestToVanishEvent.KIND

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
            syncer.resetSeen() // fresh per-pass duplicate filter
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
        for ((relay, batch) in drain(contentBuf)) {
            submit { contentUnit(relay, batch) }
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
        if (opts.maxEvents == 0) {
            // Full sync: reconcile the batch's complete id set; absence IS
            // deletion here (the relay is authoritative for this scope).
            val r = syncer.reconcile(relay, scores, forceEnumerate = opts.reconcileScores)
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
            val o = syncer.sync(relay, scores, maxEvents = opts.maxEvents)
            log("[chain ${progress.itemDone()}/${progress.position().substringAfter('/')}] ${services.size} provider(s) @ ${relay.displayUrl()}: +${o.inserted}/${o.downloaded}${neg(o)}")
        }
        // Records plane, in the same pool: the authors these services just scored
        // become content targets immediately — no waiting for every score to land.
        registerContentAuthors(scoredSubjects(services))
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

    /** New scored authors join the records side of the pipeline: resolve profile + outbox, then fetch their content. */
    private suspend fun registerContentAuthors(authors: List<HexKey>) {
        val fresh = authors.filter(knownContent::add)
        if (fresh.isEmpty()) return
        // Pull each author's kind-0 profile AND kind-10002 relay list straight from
        // the aggregators (purplepag.es et al hold ~everyone's). A profile must not
        // depend on us happening to fetch content from a relay that has it.
        resolveIdentity(fresh)
        val byAuthor = storedRelayLists(fresh)
        for (a in fresh) {
            // An author's posts are on all their write relays, so fetch from just
            // ONE — the least-loaded reachable one — to spread work across the set;
            // if none are reachable, fall back to the index relays.
            val relays = RelayUrls.publiclyRoutable(byAuthor[a]?.writeRelaysNorm().orEmpty()).ifEmpty { indexRelays }
            val chosen = relays.minByOrNull { relayLoad[it]?.get() ?: 0 } ?: continue
            relayLoad.getOrPut(chosen) { AtomicInteger() }.incrementAndGet()
            bufferedBatch(contentBuf, chosen, a)?.let { batch -> submit { contentUnit(chosen, batch) } }
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
    private suspend fun resolveIdentity(authors: List<HexKey>) {
        if (authors.isEmpty()) return
        val haveProfile = storedProfiles(authors)
        val haveList = storedRelayLists(authors).keys
        val needProfile = ConcurrentHashMap.newKeySet<HexKey>().apply { addAll(authors.filterNot { it in haveProfile }) }
        val needList = ConcurrentHashMap.newKeySet<HexKey>().apply { addAll(authors.filterNot { it in haveList }) }
        val n = authors.size
        fanOutIdentity(indexRelays, needProfile, needList)
        val t1p = n - needProfile.size
        val t1l = n - needList.size
        var tier2Size = 0
        if (needProfile.isNotEmpty() || needList.isNotEmpty()) {
            val tier2 =
                knownRelays.entries
                    .sortedByDescending { it.value.get() }
                    .map { it.key }
                    .filterNot { it in indexRelays }
                    .take(EXPAND_RELAYS)
            tier2Size = tier2.size
            fanOutIdentity(tier2, needProfile, needList)
        }
        log(
            "  [identity] $n author(s): profiles $t1p->${n - needProfile.size}/$n, lists $t1l->${n - needList.size}/$n " +
                "(tier2 $tier2Size of ${knownRelays.size} working relays, ${deadLookups.size} dead)",
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
                                idleMs = LOOKUP_IDLE_MS,
                                onVerified = { events ->
                                    events.forEach { e ->
                                        when (e) {
                                            is MetadataEvent -> {
                                                needProfile.remove(e.pubKey)
                                            }

                                            is AdvertisedRelayListEvent -> {
                                                needList.remove(e.pubKey)
                                                e.writeRelaysNorm()?.forEach { r -> knownRelays.getOrPut(r) { AtomicInteger() }.incrementAndGet() }
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

    /** One (relay x author batch) content download: every searchable kind plus the author's own deletions. */
    private suspend fun contentUnit(
        relay: NormalizedRelayUrl,
        authors: List<HexKey>,
    ) {
        val o = syncer.sync(relay, Filter(kinds = recordKinds, authors = authors), maxEvents = opts.maxEvents)
        log("[records ${progress.itemDone()}/${progress.position().substringAfter('/')}] ${authors.size} author(s) @ ${relay.displayUrl()}: +${o.inserted}/${o.downloaded}${neg(o)}")
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

    companion object {
        // A 10002 lookup hears back fast or not at all, so give a slow/broken relay
        // seconds, not the syncer-wide 10s, before moving on.
        private const val LOOKUP_IDLE_MS = 3_000L

        // Consecutive-ish timeouts before a lookup relay is skipped for the pass.
        private const val LOOKUP_DEAD_AFTER = 3

        // Tier-2 fallback breadth: how many of the working relays (the write relays
        // real users advertise, most-common first) to try for authors the
        // aggregators don't carry. Kept moderate on purpose: probing the biggest
        // relays showed the profiles/lists we miss are genuinely UNPUBLISHED (0/50
        // sampled missing 10002s existed on purplepag.es/damus/nos.lol/primal), so a
        // wider fan-out only grinds the unresolvable tail without finding anything.
        // A resolvable straggler on a popular relay is still caught here.
        private const val EXPAND_RELAYS = 50
    }
}
