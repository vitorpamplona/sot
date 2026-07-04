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
import com.vitorpamplona.quartz.nip01Core.relay.client.NostrClient
import com.vitorpamplona.quartz.nip01Core.relay.client.accessories.NegentropyOrFetchResult
import com.vitorpamplona.quartz.nip01Core.relay.client.accessories.fetchAllPages
import com.vitorpamplona.quartz.nip01Core.relay.client.accessories.negentropySyncOrFetch
import com.vitorpamplona.quartz.nip01Core.relay.client.auth.EmptyIAuthStatus
import com.vitorpamplona.quartz.nip01Core.relay.client.auth.IAuthStatus
import com.vitorpamplona.quartz.nip01Core.relay.filters.Filter
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.NormalizedRelayUrl
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.displayUrl
import com.vitorpamplona.quartz.nip01Core.store.IEventStore
import java.util.Collections
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicLong

/**
 * Syncs one (relay, filter) into the store using Quartz's generalized
 * `negentropySyncOrFetch`: it prefers NIP-77 negentropy (windowing around the
 * relay's `max_sync_events`, downloading reconciled ids through a bounded pool
 * of REQs) and transparently falls back to paginated fetch — with id-dedup — on
 * relays that can't reconcile.
 *
 * This class is the DOWNLOAD ORCHESTRATOR: it decides transport (negentropy vs
 * pages), judges completeness, and advances the persisted cursor. The
 * mechanics around it live in three collaborators, one responsibility each:
 * [EventStreamPipeline] streams a download into the store (bounded channel,
 * verify, batch insert, single-writer discipline); [NostrAuthHandshake] settles
 * the NIP-42 first-contact challenge; [CursorScope] maps a filter to its
 * incremental `since` cursor.
 *
 * Incrementality is the persisted `since` cursor: each run scopes the filter to
 * `lastSync − slack`, so only new events transfer (the slack overlap absorbs
 * back-dated events). The cursor advances after every successful sync.
 */
class RelaySyncer(
    private val client: NostrClient,
    store: IEventStore,
    private val state: SyncState,
    private val log: (String) -> Unit,
    private val fetchBatch: Int = 500,
    // Idle timeout: how long a download may hear NOTHING from the relay before we
    // give up on it. Measured from the last message received (Quartz's negentropy
    // uses a per-event IdleClock; the pages fallback a per-page wait), so it never
    // fires while events are still streaming — or while we're busy verifying and
    // inserting a chunk (that backpressures the socket, pausing the idle clock).
    private val idleTimeoutMs: Long = 10_000,
    private val slackSecs: Long = 3600,
    // Verify each event's id + Schnorr signature before storing. A relay is
    // untrusted input: without this, a forged kind:0/30382/10040 could
    // impersonate a profile or poison the web-of-trust scores. Always on in
    // production; the seam exists only so tests can feed unsigned fixtures.
    verifyEvents: Boolean = true,
    // Live counters for the pass's status line; silent by default (tests, tools).
    progress: SyncProgress = SyncProgress(log = { }),
    // NIP-42 auth status (a RelayAuthenticator on the same client). When set,
    // the first contact with each relay waits for its challenge handshake, so
    // an auth-required relay doesn't reject the sync's opening downloads.
    auth: IAuthStatus = EmptyIAuthStatus,
    // How many negentropy WINDOWS one session reconciles concurrently
    // (Quartz default 1). The relay computes each window server-side, so at 1
    // the socket idles for the full computation between streams; overlapping
    // windows hides that latency. Raise with care: it multiplies the relay's
    // concurrent NEG work per connection.
    private val reconcileConcurrency: Int = 1,
    // The id-download REQ pool per session (Quartz default 8 REQs of
    // [fetchBatch] ids each).
    private val maxConcurrentReqs: Int = 8,
) {
    data class Outcome(
        val inserted: Int,
        val usedNegentropy: Boolean,
        val downloaded: Int,
    )

    /**
     * The outcome of [reconcile]: [relayIds] is the relay's complete current id
     * set for the filter — non-null ONLY when the enumeration finished cleanly,
     * so a diff against the store can safely treat missing ids as deleted.
     */
    data class ReconcileOutcome(
        val inserted: Int,
        val relayIds: Set<String>?,
        val usedNegentropy: Boolean,
    )

    /** A [Streamed] negentropy-or-fetch download plus the protocol's own result. */
    private class NegentropyStream(
        val streamed: Streamed,
        val result: NegentropyOrFetchResult?,
        val need: Int,
    ) {
        val usedNegentropy get() = result != null && !result.pagedFallback
        val downloaded get() = result?.downloaded ?: 0
    }

    private val pipeline = EventStreamPipeline(store, log, progress, verifyEvents)
    private val handshake = NostrAuthHandshake(client, auth)

    suspend fun sync(
        relay: NormalizedRelayUrl,
        filter: Filter,
        maxEvents: Int = 0,
        // Fires with each chunk of VERIFIED events as they stream in, before they
        // hit the store. Discovery uses it to feed newly-advertised relays into
        // its worker pool the moment they arrive — no store re-scan per relay.
        onVerified: (suspend (List<Event>) -> Unit)? = null,
    ): Outcome {
        handshake.awaitOnFirstContact(relay)
        val scope = CursorScope.of(filter)
        val firstSync = state.cursor(relay, scope) == null
        val scoped = CursorScope.since(filter, state.cursor(relay, scope), slackSecs)

        if (state.relay(relay).negentropyCapable == false) {
            val pages = pagesStream(relay, scoped, maxEvents, onVerified = onVerified)
            // A truncated pages download must not advance the cursor, or the
            // missing tail would be since-filtered away forever.
            if (pages.completed) {
                state.markSynced(relay, scope, nowSecs())
            } else {
                log("  ! ${relay.displayUrl()} pages sync timed out - cursor not advanced, next pass retries")
            }
            return Outcome(pages.inserted, usedNegentropy = false, downloaded = pages.received)
        }

        val neg = negentropyStream(relay, scoped, maxEvents, onVerified = onVerified)
        var inserted = neg.streamed.inserted
        var received = neg.streamed.received
        var usedNeg = neg.usedNegentropy

        if (looksIncomplete(neg, maxEvents, firstSync)) {
            val pages = pagesStream(relay, scoped, maxEvents, onVerified = onVerified)
            if (pages.received > 0) {
                state.relay(relay).negentropyCapable = false
                usedNeg = false
                log("  [${relay.displayUrl()}] negentropy unreliable for kind ${filter.kinds?.firstOrNull()} - using pages")
            }
            inserted += pages.inserted
            received += pages.received
        } else if (usedNeg && neg.downloaded > 0 && state.relay(relay).negentropyCapable == null) {
            state.relay(relay).negentropyCapable = true
        } else if (neg.result?.pagedFallback == true && state.relay(relay).negentropyCapable == null) {
            // The relay couldn't reconcile at all (Quartz already paged this filter
            // as the fallback). Remember it: without this, every kind on every pass
            // re-attempts negentropy and stalls out its idle timeout first.
            state.relay(relay).negentropyCapable = false
            log("  [${relay.displayUrl()}] no negentropy - using pages from now on")
        }

        state.markSynced(relay, scope, nowSecs())
        return Outcome(inserted, usedNeg, received)
    }

    /**
     * A CLEAN negentropy run that still delivered fewer events than it reconciled
     * (or nothing at all on a first sync) probably lost some — some relays
     * advertise NIP-77 but serve it unreliably. A download that merely ran into
     * the [maxEvents] cap is SUPPOSED to be short; that's not suspicion.
     */
    private fun looksIncomplete(
        neg: NegentropyStream,
        maxEvents: Int,
        firstSync: Boolean,
    ): Boolean {
        if (!neg.usedNegentropy) return false
        if (maxEvents > 0 && neg.downloaded >= maxEvents) return false
        return neg.need > neg.downloaded || (neg.downloaded == 0 && firstSync)
    }

    /**
     * Full-set sync that also returns the relay's complete current id set, so
     * the caller can DETECT silent deletions: events we hold that the relay no
     * longer serves (a provider wiping rows without publishing a kind:5 leaves
     * no other trace).
     *
     * Always reconciles the whole set (no cursor). Quartz's negentropy sync
     * reconciles against an EMPTY local set — it enumerates and downloads
     * everything the relay matches — so the ids streaming past ARE the relay's
     * full set; we just collect them. (The protocol's `haveCount` is always 0
     * for the same reason — it can't detect deletions for us.) When negentropy
     * isn't available the set is enumerated with pages instead — by default
     * only when [forceEnumerate] asks, because a full page walk of a big
     * provider is costly. [relayIds] is only returned when the download
     * completed (a timeout must never be read as "everything else was deleted").
     */
    suspend fun reconcile(
        relay: NormalizedRelayUrl,
        filter: Filter,
        forceEnumerate: Boolean = false,
    ): ReconcileOutcome {
        handshake.awaitOnFirstContact(relay)
        val scope = CursorScope.of(filter)
        val ids = Collections.synchronizedSet(HashSet<String>())
        var inserted = 0

        if (state.relay(relay).negentropyCapable != false) {
            val neg = negentropyStream(relay, filter, maxEvents = 0, collectIds = ids)
            inserted += neg.streamed.inserted
            if (neg.usedNegentropy && neg.streamed.completed) {
                state.markSynced(relay, scope, nowSecs())
                return ReconcileOutcome(inserted, HashSet(ids), usedNegentropy = true)
            }
            // Negentropy failed or fell back to (possibly incomplete) paging:
            // the collected ids aren't the whole set. Enumerate explicitly.
            if (neg.result?.pagedFallback == true && state.relay(relay).negentropyCapable == null) state.relay(relay).negentropyCapable = false
            ids.clear()
        } else if (!forceEnumerate) {
            // Pages-only relay and no authoritative pass requested: plain incremental sync.
            val o = sync(relay, filter)
            return ReconcileOutcome(o.inserted, relayIds = null, usedNegentropy = o.usedNegentropy)
        }

        // Authoritative enumeration: the relay's complete current set, via pages.
        // Only the (small) id set is held in memory; the events themselves stream.
        val pages = pagesStream(relay, filter, maxEvents = 0, collectIds = ids)
        inserted += pages.inserted
        if (!pages.completed) {
            log("  ! ${relay.displayUrl()} enumeration timed out - skipping the deletion diff")
            return ReconcileOutcome(inserted, relayIds = null, usedNegentropy = false)
        }
        state.markSynced(relay, scope, nowSecs())
        // Authoritative enumeration goes through pages, never negentropy.
        return ReconcileOutcome(inserted, HashSet(ids), usedNegentropy = false)
    }

    /** Store deletions share the single-writer lock with inserts. */
    suspend fun deleteFromStore(filter: Filter) = pipeline.deleteFromStore(filter)

    /**
     * One negentropy-or-fetch download, streamed into the store. Quartz picks the
     * transport (NIP-77 reconcile, or its own paged fallback); we track the
     * reconciled `need` count so the heartbeat can show a target and [sync] can
     * judge completeness. A thrown failure is logged and surfaces as a null
     * [NegentropyStream.result].
     */
    private suspend fun negentropyStream(
        relay: NormalizedRelayUrl,
        filter: Filter,
        maxEvents: Int,
        collectIds: MutableSet<String>? = null,
        onVerified: (suspend (List<Event>) -> Unit)? = null,
    ): NegentropyStream {
        val need = AtomicInteger(0)
        var result: NegentropyOrFetchResult? = null
        val streamed =
            pipeline.stream(relay, filter, collectIds = collectIds, needHint = need::get, onVerified = onVerified) { onEvent ->
                result =
                    runCatching {
                        client.negentropySyncOrFetch(
                            relay,
                            filter,
                            maxEvents = maxEvents,
                            fetchBatch = fetchBatch,
                            idleTimeoutMs = idleTimeoutMs,
                            reconcileConcurrency = reconcileConcurrency,
                            maxConcurrentReqs = maxConcurrentReqs,
                            onProgress = { needSoFar, _ -> need.updateAndGet { maxOf(it, needSoFar) } },
                            onEvent = onEvent,
                        )
                    }.getOrElse {
                        log("  ! ${relay.url} sync failed: ${it.message}")
                        null
                    }
                true
            }
        return NegentropyStream(streamed, result, need.get())
    }

    /** Paginated `since` fetch — works on every relay. [maxEvents] caps ingest via Filter.limit. */
    private suspend fun pagesStream(
        relay: NormalizedRelayUrl,
        filter: Filter,
        maxEvents: Int,
        collectIds: MutableSet<String>? = null,
        onVerified: (suspend (List<Event>) -> Unit)? = null,
    ): Streamed {
        val paged = if (maxEvents > 0) filter.copy(limit = maxEvents) else filter
        return pipeline.stream(relay, paged, collectIds, onVerified = onVerified) { onEvent ->
            // NO outer wall-clock cap: a long-but-healthy download (many pages, each
            // fast) must never be cut mid-stream. fetchAllPages' own per-page
            // [idleTimeoutMs] watchdog is what stops a page that goes silent.
            //
            // Tell a clean finish from an idle stall by the gap since the last event:
            // a real end-of-stream returns right after its final page's events, while
            // a stall returns ~idleTimeoutMs later having heard nothing. Only a clean
            // finish may advance the cursor — a stall's missing tail must be retried.
            val lastEventMs = AtomicLong(System.currentTimeMillis())
            client.fetchAllPages(relay, listOf(paged), timeoutMs = idleTimeoutMs) { e ->
                onEvent(e)
                // Stamp AFTER onEvent, not before: onEvent blocks under
                // backpressure (trySendBlocking while the consumer flushes a
                // slow Vespa batch). Stamping before would count that ingest
                // time as idle, so a clean finish whose last insert ran long
                // would be misread as a timeout and the cursor would never
                // advance — re-downloading the same window every pass.
                lastEventMs.set(System.currentTimeMillis())
            }
            System.currentTimeMillis() - lastEventMs.get() < idleTimeoutMs
        }
    }

    private fun nowSecs() = System.currentTimeMillis() / 1000
}
