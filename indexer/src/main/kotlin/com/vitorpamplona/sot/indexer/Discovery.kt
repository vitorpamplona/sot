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

import com.vitorpamplona.quartz.nip01Core.relay.filters.Filter
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.NormalizedRelayUrl
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.displayUrl
import com.vitorpamplona.quartz.nip65RelayList.AdvertisedRelayListEvent
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.Job
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.joinAll
import kotlinx.coroutines.launch
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger

/**
 * Bounded NIP-65 outbox crawl: sync kind-10002 relay lists and expand the relay
 * pool with the relays those lists advertise, until no new relays appear or
 * [maxRelays] is hit. The pool is persisted in [SyncState.relayPool] so periodic
 * re-runs start from the known set instead of rediscovering from scratch.
 *
 * The crawl is a single continuous pool of [concurrency] worker slots draining
 * one shared queue — NOT synchronized BFS rounds. As a relay's 10002s stream in
 * (verified), the relays they advertise are pushed onto the same queue and picked
 * up by whichever worker is free next. So a worker that finishes a fast relay
 * immediately starts another instead of idling at a round barrier while the
 * round's slowest relay (some aggregators trickle toward the per-relay cap for a
 * minute) drags on. [maxRounds] survives as a crawl-*depth* limit: seeds are
 * depth 0, the relays they advertise depth 1, and so on up to [maxRounds] hops.
 */
class Discovery(
    private val syncer: RelaySyncer,
    private val state: SyncState,
    private val log: (String) -> Unit,
    private val progress: SyncProgress = SyncProgress(log = { }),
) {
    suspend fun crawl(
        seeds: List<NormalizedRelayUrl>,
        maxRounds: Int,
        maxRelays: Int,
        concurrency: Int,
    ): Set<NormalizedRelayUrl> =
        coroutineScope {
            // Insertion-ordered pool doubles as the visited/dedup set and the result.
            val pool = LinkedHashSet<NormalizedRelayUrl>()
            val lock = Any()
            val queue = Channel<Pair<NormalizedRelayUrl, Int>>(Channel.UNLIMITED)
            // Relays enqueued but not yet fully processed. The queue closes (ending
            // every worker) the moment this hits zero — the standard termination for
            // a work queue whose own items add more work.
            val inFlight = AtomicInteger(0)
            // Flips the instant the pool fills. From then on there is nothing left to
            // discover, so the crawl stops: no new work is enqueued and the workers'
            // in-flight downloads (a straggler can trickle toward the per-relay cap
            // for a minute, all of it now discarded) are cancelled outright.
            val capReached = AtomicBoolean(false)
            // The workers' shared job, so filling the pool can cancel them mid-download.
            var workers: Job? = null

            // Add [relay] to the pool if new and under the cap; enqueue it for a
            // 10002 sync only while still within the depth budget. Callable from any
            // thread (workers and the streaming verify callback both hit it).
            fun discover(
                relay: NormalizedRelayUrl,
                depth: Int,
            ) {
                var justFilled = false
                val added =
                    synchronized(lock) {
                        if (relay in pool || pool.size >= maxRelays) {
                            false
                        } else {
                            pool.add(relay)
                            if (pool.size >= maxRelays && capReached.compareAndSet(false, true)) justFilled = true
                            true
                        }
                    }
                when {
                    justFilled -> {
                        queue.close()
                        workers?.cancel() // stop the stragglers still draining their caps
                    }

                    added && depth < maxRounds -> {
                        inFlight.incrementAndGet()
                        queue.trySend(relay to depth) // UNLIMITED capacity — never fails.
                    }
                }
            }

            // Seed depth 0: the persisted pool first (known-good), then the configured seeds.
            (state.relayPool + seeds).forEach { discover(it, 0) }
            if (inFlight.get() == 0) queue.close()

            log("=== discovery: crawl from ${seeds.size} seed(s) + ${state.relayPool.size} known, $concurrency slots, depth $maxRounds, cap $maxRelays ===")
            progress.startPhase("discovery", maxRelays)
            workers =
                launch {
                    List(concurrency.coerceAtLeast(1)) {
                        launch {
                            for ((relay, depth) in queue) {
                                val o =
                                    try {
                                        // Capped: discovery only needs a big-enough sample of relay lists
                                        // to find the popular relays — some aggregators hold millions.
                                        syncer.sync(relay, Filter(kinds = listOf(AdvertisedRelayListEvent.KIND)), maxEvents = MAX_RELAY_LISTS_PER_RELAY) { verified ->
                                            // Feed the relays these lists advertise, one hop deeper, into
                                            // the same pool the instant they arrive — verified, so a forged
                                            // list can't steer the crawl. Once full, don't even parse.
                                            if (!capReached.get()) {
                                                verified.forEach { ev -> if (ev is AdvertisedRelayListEvent) ev.relaysNorm().forEach { discover(it, depth + 1) } }
                                            }
                                        }
                                    } catch (e: CancellationException) {
                                        throw e // pool-full cancel: unwind now, don't swallow it as a failed relay
                                    } catch (e: Exception) {
                                        null
                                    }
                                val size = synchronized(lock) { pool.size }
                                log("[10002 ${progress.itemDone()}] ${relay.displayUrl()}: +${o?.inserted ?: 0} (pool $size)")
                                // Last worker out closes the queue so every for-loop ends.
                                if (inFlight.decrementAndGet() == 0) queue.close()
                            }
                        }
                    }.joinAll()
                }
            workers.join() // returns when the queue drains OR the pool fills and cancels it

            val result = synchronized(lock) { LinkedHashSet(pool) }
            log("=== discovery converged: ${result.size} relays ===")
            state.relayPool.clear()
            state.relayPool.addAll(result)
            result
        }

    private companion object {
        // Discovery only needs a big-enough SAMPLE of each relay's lists to surface
        // the popular relays — the ones worth crawling show up in the first handful
        // of lists, not the ten-thousandth. Some aggregators hold millions, and
        // draining even 25k from one straggler cost ~a minute for zero new relays.
        const val MAX_RELAY_LISTS_PER_RELAY = 2_000
    }
}
