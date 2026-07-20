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

import com.vitorpamplona.quartz.eventstore.store.VespaEventStore
import com.vitorpamplona.quartz.eventstore.vespa.InMemoryEventIndex
import com.vitorpamplona.quartz.nip01Core.relay.client.NostrClient
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.NormalizedRelayUrl
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.RelayUrlNormalizer
import com.vitorpamplona.quartz.nip01Core.relay.server.NostrServer
import com.vitorpamplona.quartz.nip01Core.relay.server.inprocess.InProcessWebSocket
import com.vitorpamplona.quartz.nip01Core.relay.server.policies.PassThroughPolicy
import com.vitorpamplona.quartz.nip01Core.relay.sockets.WebSocket
import com.vitorpamplona.quartz.nip01Core.relay.sockets.WebSocketListener
import com.vitorpamplona.quartz.nip01Core.relay.sockets.WebsocketBuilder
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import java.util.concurrent.ConcurrentHashMap

/**
 * An in-process Nostr NETWORK: any number of REAL relays (Quartz's
 * [NostrServer] — REQs, pagination, and NIP-77 negentropy all run the real
 * protocol), each keyed by its url and backed by the SAME store
 * implementation the product uses — [VespaEventStore] over the in-memory
 * reference index. One [client] dials them all; the [WebsocketBuilder]
 * routes each connection to its url's server, so the trust-sync chain's
 * relay ROUTING (index vs outbox vs provider) is actually exercised. A url
 * nothing seeded is simply an empty relay.
 *
 * [PassThroughPolicy] so tests decide what a relay serves; the signature
 * gate under test is the CLIENT side ([RelaySyncer.verifyEvents]).
 */
internal class InProcessNet : AutoCloseable {
    private val scope = CoroutineScope(Dispatchers.IO + SupervisorJob())
    private val relays = ConcurrentHashMap<String, Pair<NostrServer, VespaEventStore>>()

    fun url(u: String): NormalizedRelayUrl = RelayUrlNormalizer.normalize(u)

    /** The store behind [u]'s relay — seed it with what that relay should hold. */
    fun store(u: String): VespaEventStore = entry(url(u)).second

    val client =
        NostrClient(
            object : WebsocketBuilder {
                override fun build(
                    url: NormalizedRelayUrl,
                    out: WebSocketListener,
                ): WebSocket = InProcessWebSocket(entry(url).first, out)
            },
            scope,
        )

    private fun entry(url: NormalizedRelayUrl): Pair<NostrServer, VespaEventStore> =
        relays.getOrPut(url.url) {
            val store = VespaEventStore(InMemoryEventIndex(), relay = url)
            NostrServer(store, policyBuilder = { PassThroughPolicy() }) to store
        }

    override fun close() {
        client.close()
        scope.cancel()
        relays.values.forEach { it.first.close() } // also closes each relay's store
    }
}
