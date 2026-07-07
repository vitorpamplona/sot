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
import com.vitorpamplona.quartz.nip01Core.crypto.verify
import com.vitorpamplona.quartz.nip01Core.relay.filters.Filter
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.NormalizedRelayUrl
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.displayUrl
import com.vitorpamplona.quartz.nip01Core.store.IEventStore
import com.vitorpamplona.quartz.utils.SeenIds
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.channels.trySendBlocking
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.launch
import java.time.LocalDate
import java.util.concurrent.atomic.AtomicInteger

/** What one streamed download did: [received] events seen, [inserted] newly accepted, whether it [completed] cleanly. */
internal class Streamed(
    val inserted: Int,
    val received: Int,
    val completed: Boolean,
)

/**
 * The store-writer half of [RelaySyncer]. It turns a relay download (a producer
 * that hands over events one at a time) into stored events, without ever
 * holding the whole set in memory.
 *
 * A bounded channel decouples download from ingest. A consumer coroutine
 * verifies and batch-inserts in [CHUNK_SIZE] chunks as events arrive, and a
 * full channel backpressures the socket instead of growing the heap (for one
 * big relay, buffering the whole download could reach multiple GB).
 *
 * The channel handling also carries the tail-wedge fix: a dead consumer cancels
 * the channel, so a producer parked in the non-cancellable [trySendBlocking]
 * unblocks instead of deadlocking forever.
 *
 * Write serialization is the STORE's job, not this class's: [VespaEventStore]
 * takes its writer lock only for the supersession resolve + the writes, running
 * dedup/guards beside them, so parallel relay syncs overlap their read checks
 * instead of queuing behind one pipeline-wide mutex.
 */
internal class EventStreamPipeline(
    private val store: IEventStore,
    private val log: (String) -> Unit,
    private val progress: SyncProgress,
    private val verifyEvents: Boolean,
) {
    // Shared across every relay stream this pass: a duplicate the walk re-receives
    // from another relay is dropped here, before verify and before the store's
    // existence check. [reset] clears it between passes so it stays pass-scoped.
    private val seenIds = SeenIds()

    /** Forget every id seen so far — call once at the start of a pass. */
    fun resetSeen() = seenIds.reset()

    /** Deletions serialize inside the store, like inserts — no pipeline-level lock. */
    suspend fun deleteFromStore(filter: Filter) = store.delete(filter)

    /**
     * The streaming core. [producer] runs the relay download, handing every
     * event to the callback. A consumer coroutine verifies and inserts them in
     * [CHUNK_SIZE] chunks as they arrive. The bounded channel backpressures the
     * download (the callback blocks a socket thread briefly) instead of
     * buffering the whole set.
     *
     * [producer] returns whether the download completed (rather than timing
     * out). [collectIds] gathers every event's id, for reconcile's deletion
     * diff. [onVerified] fires with each verified chunk before it hits the
     * store; discovery uses it to feed newly-advertised relays. [needHint]
     * supplies the reconcile target for the status line.
     */
    suspend fun stream(
        relay: NormalizedRelayUrl,
        filter: Filter,
        collectIds: MutableSet<String>? = null,
        needHint: () -> Int = { 0 },
        onVerified: (suspend (List<Event>) -> Unit)? = null,
        producer: suspend (onEvent: (Event) -> Unit) -> Boolean,
    ): Streamed =
        coroutineScope {
            val channel = Channel<Event>(2 * CHUNK_SIZE)
            val received = AtomicInteger(0)
            val inserted = AtomicInteger(0)
            val watch = progress.download(label(relay, filter))
            val consumer =
                launch(Dispatchers.IO) {
                    try {
                        val chunk = ArrayList<Event>(CHUNK_SIZE)

                        suspend fun flush() {
                            if (chunk.isNotEmpty()) {
                                inserted.addAndGet(insertBatch(chunk, relay, filter, onVerified))
                                chunk.clear()
                            }
                        }
                        for (e in channel) {
                            progress.onDequeued()
                            chunk.add(e)
                            if (chunk.size >= CHUNK_SIZE) flush()
                        }
                        flush()
                    } finally {
                        // If the consumer stops for any reason (the pass is
                        // cancelled, or an insert/verify throws), close the
                        // channel for send. That way a producer parked in the
                        // non-cancellable trySendBlocking unblocks immediately
                        // instead of deadlocking on a full channel forever.
                        // This is the tail-wedge fix: without it, "the producer
                        // closes the channel" only holds while the consumer
                        // keeps draining, which a dead consumer doesn't.
                        channel.cancel()
                    }
                }
            val completed =
                try {
                    producer { e ->
                        // collectIds feeds the negentropy reconcile diff (what the
                        // relay HAS), so record every id BEFORE the seen-filter, or a
                        // duplicate we skip would look absent and get wrongly deleted.
                        collectIds?.add(e.id)
                        progress.onEvent()
                        watch.tick(received.incrementAndGet(), needHint())
                        // Already handled this id this pass (a copy from another
                        // relay)? Skip it — no re-verify, no store round trip.
                        if (seenIds.add(e.id)) {
                            // A failed send means the consumer closed the channel
                            // (it died or was cancelled). Stop feeding promptly
                            // rather than draining the rest of the download into
                            // the void.
                            if (channel.trySendBlocking(e).isFailure) throw CancellationException("consumer stopped; aborting download")
                            progress.onQueued()
                        }
                    }
                } finally {
                    watch.done()
                    channel.close()
                    consumer.join()
                }
            Streamed(inserted.get(), received.get(), completed)
        }

    private suspend fun insertBatch(
        events: List<Event>,
        relay: NormalizedRelayUrl,
        filter: Filter,
        onVerified: (suspend (List<Event>) -> Unit)?,
    ): Int {
        // Stage-timed for the status line's bottleneck dial: signature CPU time
        // vs. the store insert itself (the store now owns any write serialization).
        val t0 = System.nanoTime()
        val valid = dropForged(events, relay, filter)
        onVerified?.invoke(valid)
        val t1 = System.nanoTime()
        val accepted =
            store
                .batchInsert(valid)
                .also { progress.onBatchTimings(t1 - t0, 0, System.nanoTime() - t1) }
                .count { it is IEventStore.InsertOutcome.Accepted }
        progress.onInserted(accepted)
        return accepted
    }

    /**
     * Drop events whose id or Schnorr signature doesn't verify, since relays
     * are untrusted input. Verification is CPU-bound (~0.2ms each) and the
     * chunks are large, so it fans out across cores. Order is preserved.
     */
    private suspend fun dropForged(
        events: List<Event>,
        relay: NormalizedRelayUrl,
        filter: Filter,
    ): List<Event> {
        if (!verifyEvents) return events
        val ok =
            coroutineScope {
                events
                    .chunked(VERIFY_CHUNK)
                    .map { chunk -> async(Dispatchers.Default) { chunk.filter { runCatching { it.verify() }.getOrDefault(false) } } }
                    .awaitAll()
                    .flatten()
            }
        val forged = events.size - ok.size
        if (forged > 0) log("  ! [${relay.url}] dropped $forged event(s) with invalid id/signature (kind ${filter.kinds?.firstOrNull()})")
        return ok
    }

    /**
     * The download's slot name in the status line: relay, kind, the author
     * (for provider syncs), and the slice's date window (for time-sliced
     * syncs). Without the window, parallel slices of the same filter look
     * identical, and a stalled one can't be told apart.
     */
    private fun label(
        relay: NormalizedRelayUrl,
        filter: Filter,
    ): String {
        val kind = filter.kinds?.firstOrNull()?.let { " k$it" } ?: ""
        val author =
            filter.authors
                ?.firstOrNull()
                ?.take(8)
                ?.let { " $it" } ?: ""
        val window =
            if (filter.since != null || filter.until != null) {
                " ${day(filter.since)}..${day(filter.until)}"
            } else {
                ""
            }
        return "${relay.displayUrl()}$kind$author$window"
    }

    /** Epoch seconds -> "MM-dd" (status-line dates); open bounds render as "…". */
    private fun day(t: Long?): String = t?.let { LocalDate.ofEpochDay(it / 86_400).toString().substring(5) } ?: "…"

    private companion object {
        // Verify + insert in chunks of this many events as they stream in.
        const val CHUNK_SIZE = 5_000

        // Signature checks fan out across cores in slices this size.
        const val VERIFY_CHUNK = 256
    }
}
