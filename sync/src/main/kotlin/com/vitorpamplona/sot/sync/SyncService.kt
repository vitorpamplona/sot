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
import com.vitorpamplona.quartz.nip01Core.relay.client.NostrClient
import com.vitorpamplona.quartz.nip01Core.relay.client.auth.RelayAuthenticator
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.RelayUrlNormalizer
import com.vitorpamplona.quartz.nip01Core.store.IEventStore
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.currentCoroutineContext
import kotlinx.coroutines.delay
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
import java.util.concurrent.ConcurrentHashMap
import kotlin.time.Duration

/**
 * The composition root of the sync side. It owns the Nostr client, the NIP-42
 * authenticator (the identity's key answers upstream AUTH challenges), and the
 * persisted [SyncState]. Each pass runs the [TrustSync] scores plane.
 *
 * It consumes an event store rather than creating one: the composition root
 * creates the store and shares it with the relay. The ranking projection is
 * already wired under the store (`:profile` decorates its EventIndex), so there
 * is no separate projection to babysit. Everything inserted here ranks
 * immediately.
 *
 * [runOnce] is one pass, and [runForever] is the server's background loop.
 * [enroll] is the product loop: the relay calls it when a user NIP-42
 * authenticates. So the first person to search through their own web of trust
 * is also the trigger that starts syncing it, and their scores plane runs on
 * the next pass.
 */
class SyncService(
    private val store: IEventStore,
    private val identity: Identity,
    private val house: HouseAccount? = null,
    seedRelays: List<String> = emptyList(),
    extraObservers: Set<HexKey> = emptySet(),
    private val statePath: String,
    private val opts: SyncOptions = SyncOptions(),
    private val log: (String) -> Unit,
    // Extra status-line gauges (the composition passes the engine's feed
    // health here, so every pass's log can answer "is the store pushing back?").
    private val gauges: List<() -> String> = emptyList(),
    private val client: NostrClient = NostrClient(okHttpWebsocketBuilder(), CoroutineScope(Dispatchers.IO + SupervisorJob())),
) : AutoCloseable {
    // Normalize relay urls once, here at the composition edge; typed beyond this point.
    private val seeds =
        seedRelays.mapNotNull { s ->
            RelayUrlNormalizer.normalizeOrNull(s) ?: run {
                log("skipping invalid seed relay url: $s")
                null
            }
        }

    // Quartz's client-side NIP-42: replies to AUTH challenges with a signed
    // kind-22242 and renews the connection's subscriptions once accepted.
    private val authenticator = RelayAuthenticator(client) { _, template -> listOf(identity.signer.sign(template)) }

    private val state = SyncState.load(statePath)

    // Config extras + NIP-42 enrollments; stored 10040 authors join inside TrustSync.
    private val observers = ConcurrentHashMap.newKeySet<HexKey>().apply { addAll(extraObservers) }

    /** Add [pubkey] as an observer (a NIP-42 login on our relay); synced from the next pass on. */
    fun enroll(pubkey: HexKey) {
        if (observers.add(pubkey)) log("[observer] enrolled ${pubkey.take(8)}… - scores sync from the next pass")
    }

    /** One full pass of the scores plane. */
    suspend fun runOnce() {
        identity.ensurePublished(store)
        val progress = SyncProgress(log = log)
        gauges.forEach(progress::gauge)
        val syncer =
            RelaySyncer(
                client,
                store,
                state,
                log,
                idleTimeoutMs = opts.fetchTimeoutMs,
                verifyEvents = opts.verifyEvents,
                progress = progress,
                auth = authenticator,
            )
        coroutineScopeWithTicker(progress) {
            TrustSync(syncer, store, opts, log, progress).run(
                observers = HashSet(observers),
                indexRelays = identity.indexRelays(store),
                house = house,
                seedRelays = seeds,
            )
        }
    }

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

    /** Run [body] with the progress ticker printing; always save the cursors after. */
    private suspend fun coroutineScopeWithTicker(
        progress: SyncProgress,
        body: suspend () -> Unit,
    ) = coroutineScope {
        val ticker = launch { progress.run() }
        try {
            body()
        } finally {
            ticker.cancel()
            SyncState.save(statePath, state)
            log("[state] saved cursors for ${state.relays.size} relay(s)")
        }
    }

    override fun close() {
        authenticator.destroy()
        client.close()
    }
}
