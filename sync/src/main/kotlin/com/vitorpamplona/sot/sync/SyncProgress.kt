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

import kotlinx.coroutines.delay
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicLong

/**
 * Live counters for one sync pass, rendered as ONE dense status line every
 * [everyMs] — the answer to "where are things at?" AND "what's the
 * bottleneck?" while a multi-hour pass runs:
 *
 *   ~ providers 3/12 | recv 1.2M (8.9k/s) new 987k (2.1k/s) q 9.8k | vrf 0.4x wait 3.1x ins 92% | feed ok 950k inflight 512 lat 19ms | nip85.… k30382 05-19..06-02 741k/2.3M
 *
 * Reading the bottleneck dial:
 *  - `q` is the download->insert channel depth summed over live downloads:
 *    pinned at capacity = ingest-bound (the socket is waiting on the store);
 *    near zero = network-bound (the store is waiting on the relay).
 *  - `ins` is the store's single-writer busy share of wall time (<=100%):
 *    high = the store/engine is the limiter. `wait` is time spent queued on
 *    that writer lock (sums over parallel downloads, so >1x is normal);
 *    `vrf` is signature-check CPU concurrency.
 *  - the feed [gauge] (registered by the composition) shows the engine
 *    client's live window and latency.
 *
 * Every component reports in: [RelaySyncer] counts events, registers each
 * live [Download], and stamps the stage timings; the pipeline phases set the
 * `done/total` position; other components contribute [gauge]s. The line only
 * prints while something is happening.
 */
class SyncProgress(
    private val everyMs: Long = 5_000,
    private val log: (String) -> Unit,
) {
    /** One live download's slot in the status line. */
    inner class Download internal constructor(
        private val key: String,
    ) {
        @Volatile var received = 0
            private set

        @Volatile var need = 0
            private set

        @Volatile var lastEventAt = System.currentTimeMillis()
            private set

        fun tick(
            receivedSoFar: Int,
            needSoFar: Int,
        ) {
            received = receivedSoFar
            if (needSoFar > 0) need = needSoFar
            lastEventAt = System.currentTimeMillis()
        }

        fun done() {
            active.remove(key)
        }
    }

    @Volatile private var phase = "starting"

    @Volatile private var total = 0
    private val done = AtomicInteger(0)
    private val received = AtomicLong(0)
    private val inserted = AtomicLong(0)
    private val queued = AtomicLong(0)
    private val verifyNanos = AtomicLong(0)
    private val lockWaitNanos = AtomicLong(0)
    private val insertNanos = AtomicLong(0)
    private val active = ConcurrentHashMap<String, Download>()
    private val gauges = CopyOnWriteArrayList<() -> String>()

    /** Register a live download under [label] (shown in the status line until [Download.done]). */
    fun download(label: String): Download = Download(label).also { active[label] = it }

    /** One event arrived from any relay. */
    fun onEvent() {
        received.incrementAndGet()
    }

    /** [n] events were newly accepted into the store. */
    fun onInserted(n: Int) {
        inserted.addAndGet(n.toLong())
    }

    /** An event entered the download->insert channel. */
    fun onQueued() {
        queued.incrementAndGet()
    }

    /** An event left the channel for verification + insert. */
    fun onDequeued() {
        queued.decrementAndGet()
    }

    /** One insert batch's stage timings (signature checks / writer-lock wait / store insert). */
    fun onBatchTimings(
        verifyNanos: Long,
        lockWaitNanos: Long,
        insertNanos: Long,
    ) {
        this.verifyNanos.addAndGet(verifyNanos)
        this.lockWaitNanos.addAndGet(lockWaitNanos)
        this.insertNanos.addAndGet(insertNanos)
    }

    /** Enter a new phase of [totalItems] items; [itemDone] advances through it. */
    fun startPhase(
        name: String,
        totalItems: Int,
    ) {
        phase = name
        total = totalItems
        done.set(0)
    }

    /** One phase item finished; returns its 1-based position for `[n/total]` log lines. */
    fun itemDone(): Int = done.incrementAndGet()

    /** Add [supplier]'s text to every status line (e.g. the Vespa write gauge). */
    fun gauge(supplier: () -> String) {
        gauges.add(supplier)
    }

    /** Print the status line every [everyMs] until cancelled; silent while nothing changes. */
    suspend fun run() {
        var prevReceived = 0L
        var prevInserted = 0L
        var prevVerify = 0L
        var prevWait = 0L
        var prevInsert = 0L
        var prevAt = System.nanoTime()
        var lastLine = ""
        while (true) {
            delay(everyMs)
            val now = System.nanoTime()
            val wall = (now - prevAt).coerceAtLeast(1)
            val recv = received.get()
            val ins = inserted.get()
            val perSec = ((recv - prevReceived) * 1_000_000_000.0 / wall).toLong()
            val insPerSec = ((ins - prevInserted) * 1_000_000_000.0 / wall).toLong()
            val busy =
                busyGauge(
                    verifyX = (verifyNanos.get() - prevVerify).toDouble() / wall,
                    waitX = (lockWaitNanos.get() - prevWait).toDouble() / wall,
                    insertX = (insertNanos.get() - prevInsert).toDouble() / wall,
                )
            prevReceived = recv
            prevInserted = ins
            prevVerify = verifyNanos.get()
            prevWait = lockWaitNanos.get()
            prevInsert = insertNanos.get()
            prevAt = now

            val line = statusLine(perSec, insPerSec, busy)
            // While downloads are live, keep printing even if the numbers froze -
            // a visibly stalled line beats silence. Only true idleness goes quiet.
            if (active.isEmpty() && line == lastLine) continue
            lastLine = line
            log(line)
        }
    }

    /** The per-tick bottleneck dial; empty while nothing was verified or inserted. */
    private fun busyGauge(
        verifyX: Double,
        waitX: Double,
        insertX: Double,
    ): String {
        if (verifyX + waitX + insertX < 0.005) return ""
        // The store writer is a mutex: its busy share is a true <=100% dial.
        // Verify and lock-wait sum over parallel downloads, so they read as
        // concurrency multiples instead.
        return "vrf %.1fx wait %.1fx ins %d%%".format(verifyX, waitX, (insertX * 100).toInt().coerceAtMost(100))
    }

    /** The one-line snapshot: phase position, totals + rates, bottleneck dial, gauges, live downloads. */
    internal fun statusLine(
        perSec: Long,
        insPerSec: Long = 0,
        busy: String = "",
    ): String =
        buildString {
            append("  ~ $phase ${done.get()}/$total")
            append(" | recv ${compact(received.get())}")
            if (perSec > 0) append(" (${compact(perSec)}/s)")
            append(" new ${compact(inserted.get())}")
            if (insPerSec > 0) append(" (${compact(insPerSec)}/s)")
            queued.get().let { if (it > 0) append(" q ${compact(it)}") }
            if (busy.isNotEmpty()) append(" | $busy")
            for (gauge in gauges) append(" | ${gauge()}")

            val busiest = active.entries.sortedByDescending { it.value.received }
            if (busiest.isNotEmpty()) {
                append(" | ")
                val now = System.currentTimeMillis()
                busiest.take(MAX_SHOWN_DOWNLOADS).joinTo(this) { (label, d) ->
                    val target = if (d.need > d.received) "/${compact(d.need.toLong())}" else ""
                    // A download that stopped receiving is the thing worth seeing.
                    val stall = (now - d.lastEventAt).let { if (it >= STALL_AFTER_MS) " (idle ${age(it)})" else "" }
                    "$label ${compact(d.received.toLong())}$target$stall"
                }
                val more = busiest.size - MAX_SHOWN_DOWNLOADS
                if (more > 0) append(", +$more more")
            }
        }

    companion object {
        private const val MAX_SHOWN_DOWNLOADS = 3
        private const val STALL_AFTER_MS = 15_000L

        /** 1234 -> "1.2k", 234567 -> "234k", 1234567 -> "1.2M" — status lines stay short. */
        fun compact(n: Long): String =
            when {
                n >= 10_000_000 -> "${n / 1_000_000}M"
                n >= 1_000_000 -> "%.1fM".format(n / 1_000_000.0)
                n >= 100_000 -> "${n / 1_000}k"
                n >= 1_000 -> "%.1fk".format(n / 1_000.0)
                else -> n.toString()
            }

        /** 45_000 -> "45s", 200_000 -> "3m", 4_500_000 -> "1h15m". */
        fun age(ms: Long): String {
            val s = ms / 1000
            return when {
                s >= 3600 -> "${s / 3600}h${(s % 3600) / 60}m"
                s >= 60 -> "${s / 60}m"
                else -> "${s}s"
            }
        }
    }
}
