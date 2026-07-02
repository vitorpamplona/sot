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
import com.vitorpamplona.quartz.nip01Core.relay.client.accessories.fetchAllPages
import com.vitorpamplona.quartz.nip01Core.relay.client.accessories.negentropySyncOrFetch
import com.vitorpamplona.quartz.nip01Core.relay.filters.Filter
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.NormalizedRelayUrl
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.displayUrl
import com.vitorpamplona.quartz.nip01Core.store.IEventStore
import com.vitorpamplona.quartz.nip01Core.store.ObservableEventStore
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
 * relays that can't reconcile. No hand-rolled state machine here anymore.
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

    /** What one download produced: the raw events, and whether negentropy did the work. */
    private class Download(
        val events: List<Event>,
        val usedNegentropy: Boolean,
    )

    // Relay syncs run in parallel, but the SQLite store stays a single writer.
    private val storeWrites = Mutex()

    suspend fun sync(
        relay: NormalizedRelayUrl,
        filter: Filter,
        maxEvents: Int = 0,
    ): Outcome {
        val scope = cursorScope(filter)
        val firstSync = state.cursor(relay, scope) == null

        val download = download(relay, sinceCursor(filter, relay, scope), maxEvents, firstSync)
        val valid = dropForged(download.events, relay, filter)
        val inserted = storeWrites.withLock { store.batchInsert(valid) }.count { it is IEventStore.InsertOutcome.Accepted }
        state.markSynced(relay, scope, nowSecs())
        return Outcome(inserted, download.usedNegentropy, download.events.size)
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

    /**
     * Fetch everything matching [filter], choosing the strategy per relay:
     * negentropy first (the library already pages back on a NEG-ERR), plain
     * pages when the relay is remembered as unable to reconcile.
     *
     * A relay can also reconcile to nothing / deliver less than reconciled
     * WITHOUT erroring (purplepag.es on 10040). On the first sync of a scope,
     * double-check such a suspicious result with pages; if pages find data,
     * remember the relay as pages-only so future runs skip the wasted round-trip.
     */
    private suspend fun download(
        relay: NormalizedRelayUrl,
        filter: Filter,
        maxEvents: Int,
        firstSync: Boolean,
    ): Download {
        val heartbeat = Heartbeat(relay, filter)
        if (state.relay(relay).negentropyCapable == false) {
            return Download(fetchPages(relay, filter, maxEvents, heartbeat), usedNegentropy = false)
        }

        val buf = Collections.synchronizedList(ArrayList<Event>())
        val need = AtomicInteger(0)
        val res =
            runCatching {
                client.negentropySyncOrFetch(
                    relay,
                    filter,
                    maxEvents = maxEvents,
                    fetchBatch = fetchBatch,
                    idleTimeoutMs = idleTimeoutMs,
                    onProgress = { needSoFar, _ -> need.updateAndGet { maxOf(it, needSoFar) } },
                ) {
                    buf.add(it)
                    heartbeat.tick(buf.size, need.get())
                }
            }.getOrElse {
                log("  ! ${relay.url} sync failed: ${it.message}")
                null
            }
        var usedNeg = res?.pagedFallback == false

        // A download that ran into the maxEvents cap is SUPPOSED to deliver fewer
        // events than reconciled — don't let that trip the unreliability check.
        val capped = maxEvents > 0 && (res?.downloaded ?: 0) >= maxEvents
        val suspicious =
            res != null && !res.pagedFallback && !capped &&
                (need.get() > res.downloaded || (res.downloaded == 0 && firstSync))
        if (suspicious) {
            val paged = fetchPages(relay, filter, maxEvents, heartbeat)
            if (paged.isNotEmpty()) {
                state.relay(relay).negentropyCapable = false
                usedNeg = false
                log("  [${relay.url}] negentropy unreliable for kind ${filter.kinds?.firstOrNull()} - using pages")
                buf.addAll(paged)
            }
        } else if (usedNeg && (res?.downloaded ?: 0) > 0 && state.relay(relay).negentropyCapable == null) {
            state.relay(relay).negentropyCapable = true
        }
        return Download(ArrayList(buf), usedNeg)
    }

    /** Paginated `since` fetch — works on every relay. [maxEvents] caps ingest via Filter.limit. */
    private suspend fun fetchPages(
        relay: NormalizedRelayUrl,
        filter: Filter,
        maxEvents: Int,
        heartbeat: Heartbeat,
    ): List<Event> {
        val paged = if (maxEvents > 0) filter.copy(limit = maxEvents) else filter
        val buf = Collections.synchronizedList(ArrayList<Event>())
        withTimeoutOrNull(idleTimeoutMs) {
            client.fetchAllPages(relay, listOf(paged)) {
                buf.add(it)
                heartbeat.tick(buf.size, need = 0)
            }
        }
        return ArrayList(buf)
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
}
