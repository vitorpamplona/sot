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
package com.vitorpamplona.sot.v2.sync

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
 * The composition of the v2 sync side: owns the Nostr client, the NIP-42
 * authenticator (the identity's key answers upstream AUTH challenges), and
 * the persisted [SyncState]; each pass runs the [TrustSync] scores plane. It
 * CONSUMES an event store — the composition root creates it and shares it
 * with the relay, and the ranking projection is already wired UNDER the
 * store (`:v2:profile` decorates its EventIndex), so unlike v1 there is no
 * separate projection to babysit: everything inserted here ranks
 * immediately.
 *
 * [runOnce] is one pass; [runForever] is the server's background loop.
 * [enroll] is the product loop: the relay calls it when a user NIP-42
 * authenticates, so the first person to search through their own web of
 * trust is also the trigger that starts syncing it (their scores plane runs
 * on the next pass).
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
