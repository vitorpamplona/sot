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

import kotlinx.coroutines.delay
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicLong

/**
 * Live counters for one sync pass, rendered as ONE dense status line every
 * [everyMs] — the answer to "where are things at?" while a multi-hour pass runs:
 *
 *   ~ relays 42/127 | recv 1.2M (4.3k/s) new 987k | vespa ok 950k inflight 12k | purplepag.es k0 741k/2.3M, damus.io k0 234k, +4 more
 *
 * Every component reports in: [RelaySyncer] counts events and registers each
 * live [Download], the pipeline phases set the `done/total` position, and other
 * components ([SyncService]'s Vespa projection) contribute [gauge]s. The line
 * only prints while something is happening.
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
        var prevAt = System.nanoTime()
        var lastLine = ""
        while (true) {
            delay(everyMs)
            val now = System.nanoTime()
            val recv = received.get()
            val perSec = ((recv - prevReceived) * 1_000_000_000.0 / (now - prevAt)).toLong()
            prevReceived = recv
            prevAt = now

            val line = statusLine(perSec)
            // While downloads are live, keep printing even if the numbers froze -
            // a visibly stalled line beats silence. Only true idleness goes quiet.
            if (active.isEmpty() && line == lastLine) continue
            lastLine = line
            log(line)
        }
    }

    /** The one-line snapshot: phase position, totals + rate, gauges, live downloads. */
    internal fun statusLine(perSec: Long): String =
        buildString {
            append("  ~ $phase ${done.get()}/$total")
            append(" | recv ${compact(received.get())}")
            if (perSec > 0) append(" (${compact(perSec)}/s)")
            append(" new ${compact(inserted.get())}")
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
