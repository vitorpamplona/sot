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
import com.vitorpamplona.quartz.nip01Core.crypto.verify
import com.vitorpamplona.quartz.nip01Core.relay.client.NostrClient
import com.vitorpamplona.quartz.nip01Core.relay.client.accessories.NegentropyOrFetchResult
import com.vitorpamplona.quartz.nip01Core.relay.client.accessories.fetchAllPages
import com.vitorpamplona.quartz.nip01Core.relay.client.accessories.negentropySyncOrFetch
import com.vitorpamplona.quartz.nip01Core.relay.filters.Filter
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.NormalizedRelayUrl
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.displayUrl
import com.vitorpamplona.quartz.nip01Core.store.IEventStore
import com.vitorpamplona.quartz.nip01Core.store.ObservableEventStore
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.channels.trySendBlocking
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.launch
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import kotlinx.coroutines.withTimeoutOrNull
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
 * Events STREAM into the store: the download callback feeds a bounded channel,
 * and a consumer verifies + batch-inserts in [CHUNK_SIZE] chunks as they arrive.
 * A multi-million-event relay never sits in memory (the old whole-download
 * buffer was multiple GB for one big relay), the projection starts working
 * during the download, and a full channel backpressures the socket instead of
 * growing the heap.
 *
 * Incrementality is the persisted `since` cursor: each run scopes the filter to
 * `lastSync − slack`, so only new events transfer (the slack overlap absorbs
 * back-dated events). The cursor advances after every successful sync.
 */
class RelaySyncer(
    private val client: NostrClient,
    private val store: ObservableEventStore,
    private val state: SyncState,
    private val log: (String) -> Unit,
    private val fetchBatch: Int = 500,
    private val idleTimeoutMs: Long = 30_000,
    private val slackSecs: Long = 3600,
    // Verify each event's id + Schnorr signature before storing. A relay is
    // untrusted input: without this, a forged kind:0/30382/10040 could
    // impersonate a profile or poison the web-of-trust scores. Always on in
    // production; the seam exists only so tests can feed unsigned fixtures.
    private val verifyEvents: Boolean = true,
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
    class ReconcileOutcome(
        val inserted: Int,
        val relayIds: Set<String>?,
        val usedNegentropy: Boolean,
    )

    /** What one streamed download did: [received] events seen, [inserted] newly accepted. */
    private class Streamed(
        val inserted: Int,
        val received: Int,
        val completed: Boolean,
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

    // Relay syncs run in parallel, but the SQLite store stays a single writer.
    private val storeWrites = Mutex()

    suspend fun sync(
        relay: NormalizedRelayUrl,
        filter: Filter,
        maxEvents: Int = 0,
    ): Outcome {
        val scope = cursorScope(filter)
        val firstSync = state.cursor(relay, scope) == null
        val scoped = sinceCursor(filter, relay, scope)
        val heartbeat = Heartbeat(relay, filter)

        if (state.relay(relay).negentropyCapable == false) {
            val pages = pagesStream(relay, scoped, maxEvents, heartbeat)
            state.markSynced(relay, scope, nowSecs())
            return Outcome(pages.inserted, usedNegentropy = false, downloaded = pages.received)
        }

        val neg = negentropyStream(relay, scoped, maxEvents, heartbeat)
        var inserted = neg.streamed.inserted
        var received = neg.streamed.received
        var usedNeg = neg.usedNegentropy

        if (looksIncomplete(neg, maxEvents, firstSync)) {
            val pages = pagesStream(relay, scoped, maxEvents, heartbeat)
            if (pages.received > 0) {
                state.relay(relay).negentropyCapable = false
                usedNeg = false
                log("  [${relay.displayUrl()}] negentropy unreliable for kind ${filter.kinds?.firstOrNull()} - using pages")
            }
            inserted += pages.inserted
            received += pages.received
        } else if (usedNeg && neg.downloaded > 0 && state.relay(relay).negentropyCapable == null) {
            state.relay(relay).negentropyCapable = true
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
     * Full-set sync that can also DETECT silent deletions: events we hold that
     * the relay no longer serves (a provider wiping rows without publishing a
     * kind:5 leaves no other trace).
     *
     * Always reconciles the whole set (no cursor). On a negentropy-capable relay
     * the deleted-on-relay side comes free as the protocol's `haveCount`; when it
     * is zero the sets match and we're done. When it isn't — or when
     * [forceEnumerate] asks for the authoritative answer on a pages-only relay —
     * the full set is enumerated with pages so the caller can diff and delete.
     * The id set is only returned when the enumeration completed (a timeout must
     * never be read as "everything else was deleted").
     */
    suspend fun reconcile(
        relay: NormalizedRelayUrl,
        filter: Filter,
        forceEnumerate: Boolean = false,
    ): ReconcileOutcome {
        val scope = cursorScope(filter)
        val heartbeat = Heartbeat(relay, filter)
        var inserted = 0
        var usedNeg = false

        if (state.relay(relay).negentropyCapable != false) {
            val neg = negentropyStream(relay, filter, maxEvents = 0, heartbeat)
            inserted += neg.streamed.inserted
            if (neg.usedNegentropy) {
                usedNeg = true
                state.markSynced(relay, scope, nowSecs())
                val gone = neg.result?.negentropy?.haveCount ?: 0
                if (gone == 0 && !forceEnumerate) return ReconcileOutcome(inserted, relayIds = null, usedNegentropy = true)
                if (gone > 0) log("  [${relay.displayUrl()}] $gone event(s) we hold are gone from the relay - enumerating for deletion")
            }
        } else if (!forceEnumerate) {
            // Pages-only relay and no authoritative pass requested: plain incremental sync.
            val o = sync(relay, filter)
            return ReconcileOutcome(o.inserted, relayIds = null, usedNegentropy = o.usedNegentropy)
        }

        // Authoritative enumeration: the relay's complete current set, via pages.
        // Only the (small) id set is held in memory; the events themselves stream.
        val ids = Collections.synchronizedSet(HashSet<String>())
        val pages = pagesStream(relay, filter, maxEvents = 0, heartbeat, collectIds = ids, timeoutFactor = ENUMERATION_TIMEOUT_FACTOR)
        inserted += pages.inserted
        if (!pages.completed) {
            log("  ! ${relay.displayUrl()} enumeration timed out - skipping the deletion diff")
            return ReconcileOutcome(inserted, relayIds = null, usedNegentropy = usedNeg)
        }
        state.markSynced(relay, scope, nowSecs())
        return ReconcileOutcome(inserted, HashSet(ids), usedNeg)
    }

    /** Store deletions share the single-writer lock with inserts. */
    suspend fun deleteFromStore(filter: Filter) = storeWrites.withLock { store.delete(filter) }

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
        heartbeat: Heartbeat,
    ): NegentropyStream {
        val need = AtomicInteger(0)
        var result: NegentropyOrFetchResult? = null
        val streamed =
            streamEvents(relay, filter, heartbeat, needHint = need::get) { onEvent ->
                result =
                    runCatching {
                        client.negentropySyncOrFetch(
                            relay,
                            filter,
                            maxEvents = maxEvents,
                            fetchBatch = fetchBatch,
                            idleTimeoutMs = idleTimeoutMs,
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

    /**
     * The streaming core: [producer] runs the relay download, handing every event
     * to the callback; a consumer coroutine verifies + inserts them in
     * [CHUNK_SIZE] chunks as they arrive. The bounded channel backpressures the
     * download (the callback blocks a socket thread briefly) instead of buffering
     * the whole set. [producer] returns whether the download COMPLETED (vs timed out).
     */
    private suspend fun streamEvents(
        relay: NormalizedRelayUrl,
        filter: Filter,
        heartbeat: Heartbeat,
        collectIds: MutableSet<String>? = null,
        needHint: () -> Int = { 0 },
        producer: suspend (onEvent: (Event) -> Unit) -> Boolean,
    ): Streamed =
        coroutineScope {
            val channel = Channel<Event>(2 * CHUNK_SIZE)
            val received = AtomicInteger(0)
            val inserted = AtomicInteger(0)
            val consumer =
                launch(Dispatchers.IO) {
                    val chunk = ArrayList<Event>(CHUNK_SIZE)

                    suspend fun flush() {
                        if (chunk.isNotEmpty()) {
                            inserted.addAndGet(insertBatch(chunk, relay, filter))
                            chunk.clear()
                        }
                    }
                    for (e in channel) {
                        chunk.add(e)
                        if (chunk.size >= CHUNK_SIZE) flush()
                    }
                    flush()
                }
            val completed =
                try {
                    producer { e ->
                        collectIds?.add(e.id)
                        heartbeat.tick(received.incrementAndGet(), needHint())
                        channel.trySendBlocking(e)
                    }
                } finally {
                    channel.close()
                    consumer.join()
                }
            Streamed(inserted.get(), received.get(), completed)
        }

    /** Paginated `since` fetch — works on every relay. [maxEvents] caps ingest via Filter.limit. */
    private suspend fun pagesStream(
        relay: NormalizedRelayUrl,
        filter: Filter,
        maxEvents: Int,
        heartbeat: Heartbeat,
        collectIds: MutableSet<String>? = null,
        timeoutFactor: Int = 1,
    ): Streamed {
        val paged = if (maxEvents > 0) filter.copy(limit = maxEvents) else filter
        return streamEvents(relay, paged, heartbeat, collectIds) { onEvent ->
            withTimeoutOrNull(idleTimeoutMs * timeoutFactor) {
                client.fetchAllPages(relay, listOf(paged), onEvent = onEvent)
            } != null
        }
    }

    private suspend fun insertBatch(
        events: List<Event>,
        relay: NormalizedRelayUrl,
        filter: Filter,
    ): Int {
        val valid = dropForged(events, relay, filter)
        return storeWrites.withLock { store.batchInsert(valid) }.count { it is IEventStore.InsertOutcome.Accepted }
    }

    /** Throttled progress line for one (relay, kind) download — silence means it finished fast. */
    private inner class Heartbeat(
        private val relay: NormalizedRelayUrl,
        private val filter: Filter,
        private val everyMs: Long = 5_000,
    ) {
        private val last = AtomicLong(System.currentTimeMillis())

        fun tick(
            downloaded: Int,
            need: Int,
        ) {
            val now = System.currentTimeMillis()
            val prev = last.get()
            if (now - prev >= everyMs && last.compareAndSet(prev, now)) {
                val kind = filter.kinds?.firstOrNull()
                val target = if (need > 0) " of ~$need" else ""
                log("  ... ${relay.displayUrl()} kind $kind: $downloaded$target events so far")
            }
        }
    }

    /** Cursor scope key: the kind, plus authors so per-provider 30382 syncs don't share a cursor. */
    private fun cursorScope(filter: Filter): String {
        val kind = filter.kinds?.firstOrNull() ?: -1
        val authors = filter.authors?.let { ":" + it.joinToString(",") } ?: ""
        return "$kind$authors"
    }

    /** Scope [filter] to the persisted cursor — minus slack to absorb back-dated events — if one exists. */
    private fun sinceCursor(
        filter: Filter,
        relay: NormalizedRelayUrl,
        scope: String,
    ): Filter {
        val since = state.cursor(relay, scope)?.minus(slackSecs) ?: return filter
        return filter.copy(since = since)
    }

    /** Drop events whose id or Schnorr signature doesn't verify — relays are untrusted input. */
    private fun dropForged(
        events: List<Event>,
        relay: NormalizedRelayUrl,
        filter: Filter,
    ): List<Event> {
        if (!verifyEvents) return events
        val (ok, forged) = events.partition { runCatching { it.verify() }.getOrDefault(false) }
        if (forged.isNotEmpty()) log("  ! [${relay.url}] dropped ${forged.size} event(s) with invalid id/signature (kind ${filter.kinds?.firstOrNull()})")
        return ok
    }

    private fun nowSecs() = System.currentTimeMillis() / 1000

    private companion object {
        // Verify + insert in chunks of this many events as they stream in.
        const val CHUNK_SIZE = 5_000

        // Enumerating a big provider's full set takes longer than one idle window.
        const val ENUMERATION_TIMEOUT_FACTOR = 20
    }
}
