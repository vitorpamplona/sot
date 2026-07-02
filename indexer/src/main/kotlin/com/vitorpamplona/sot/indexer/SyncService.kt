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

import com.vitorpamplona.quartz.nip01Core.relay.client.NostrClient
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.RelayUrlNormalizer
import com.vitorpamplona.quartz.nip01Core.store.ObservableEventStore
import com.vitorpamplona.sot.vespa.VespaClient
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.currentCoroutineContext
import kotlinx.coroutines.delay
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
import kotlin.time.Duration

/**
 * The one composition of the indexing side: owns the Nostr client, the Vespa
 * feed client, and the store->Vespa projection, and runs [runSync] passes over
 * them. It CONSUMES an event store — it never creates one; the composition root
 * does, and may share it with other components (the NIP-50 relay in `sot serve`).
 *
 * [runOnce] is one incremental pass (`sot index`); [runForever] is the server's
 * background loop. The projection subscription outlives passes: anything inserted
 * into [store] by ANY path while this service is alive — a sync, a future live
 * subscription, an authed publish on the relay — is mirrored into Vespa.
 *
 * End of life is [drain] (once) then [close]. The store stays open — its owner
 * closes it.
 */
class SyncService(
    private val store: ObservableEventStore,
    vespaUrl: String,
    seedRelays: List<String>,
    private val statePath: String,
    private val opts: SyncOptions,
    private val log: (String) -> Unit,
) : AutoCloseable {
    // Normalize relay urls once, here at the composition edge; typed beyond this point.
    private val relays =
        seedRelays.mapNotNull { s ->
            RelayUrlNormalizer.normalizeOrNull(s) ?: run {
                log("skipping invalid relay url: $s")
                null
            }
        }

    private val client = NostrClient(okHttpWebsocketBuilder(), CoroutineScope(Dispatchers.IO + SupervisorJob()))
    private val state = SyncState.load(statePath)

    // Async writes: the feed client multiplexes them over HTTP/2 and owns
    // concurrency, retries, and throttling — no write pool on our side.
    private val vespa = VespaClient(vespaUrl)
    private val projection = VespaProjection(store, vespa, log)

    // The projection runs in its OWN supervised scope for the service's whole
    // life: a projection error can't cancel a sync pass, and no change slips
    // through between passes.
    private val projScope = CoroutineScope(Dispatchers.IO + SupervisorJob())

    init {
        projScope.launch { projection.run() }
    }

    /** One full incremental pass: relays -> store; Vespa follows via the change feed. */
    suspend fun runOnce() = runSync(client, store, state, statePath, relays, opts, log)

    /** [runOnce] forever, waiting [interval] between the END of one pass and the next. */
    suspend fun runForever(interval: Duration) {
        while (currentCoroutineContext().isActive) {
            try {
                runOnce()
            } catch (e: CancellationException) {
                throw e
            } catch (e: Exception) {
                log("! sync pass failed: ${e.message}")
            }
            log("next sync pass in $interval")
            delay(interval)
        }
    }

    /**
     * Let the projection catch up with the change feed and its in-flight Vespa
     * writes land. Call at end of life, before [close].
     */
    suspend fun drain(maxWait: Duration) = projection.awaitIdle(maxMs = maxWait.inWholeMilliseconds)

    fun summary(): String =
        "profiles=${projection.profiles.get()} scores=${projection.scores.get()} " +
            "deletions=${projection.deletions.get()} unresolved=${projection.unresolved.get()}"

    override fun close() {
        projScope.cancel()
        vespa.close()
        client.close()
    }
}
