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
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.NormalizedRelayUrl
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.RelayUrlNormalizer
import com.vitorpamplona.quartz.nip01Core.relay.server.NostrServer
import com.vitorpamplona.quartz.nip01Core.relay.server.inprocess.InProcessWebSocket
import com.vitorpamplona.quartz.nip01Core.relay.server.policies.PassThroughPolicy
import com.vitorpamplona.quartz.nip01Core.relay.sockets.WebSocket
import com.vitorpamplona.quartz.nip01Core.relay.sockets.WebSocketListener
import com.vitorpamplona.quartz.nip01Core.relay.sockets.WebsocketBuilder
import com.vitorpamplona.quartz.nip01Core.store.sqlite.DefaultIndexingStrategy
import com.vitorpamplona.quartz.nip01Core.store.sqlite.EventStore
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import java.io.File

/**
 * A REAL Nostr relay running in-process, backed by its own SQLite store, plus
 * a [NostrClient] wired to it through Quartz's [InProcessWebSocket]: REQs,
 * pagination, and NIP-77 negentropy all run the real protocol on both sides —
 * just without a network. Seed [store] with the events the "relay" should hold;
 * every url [client] dials lands on this one server.
 *
 * [PassThroughPolicy] so tests decide what the relay serves, signed or not —
 * the signature gate under test is the CLIENT side ([RelaySyncer.verifyEvents]).
 */
internal class InProcessRelay(
    val url: NormalizedRelayUrl = RelayUrlNormalizer.normalize("wss://in-process.test"),
) : AutoCloseable {
    val store =
        EventStore(
            File
                .createTempFile("in-process-relay", ".db")
                .also {
                    it.delete()
                    it.deleteOnExit()
                }.path,
            url,
            DefaultIndexingStrategy(indexFullTextSearch = false),
        )

    private val server = NostrServer(store, policyBuilder = { PassThroughPolicy() })
    private val clientScope = CoroutineScope(Dispatchers.IO + SupervisorJob())

    val client =
        NostrClient(
            object : WebsocketBuilder {
                override fun build(
                    relay: NormalizedRelayUrl,
                    listener: WebSocketListener,
                ): WebSocket = InProcessWebSocket(server, listener)
            },
            clientScope,
        )

    override fun close() {
        client.close()
        clientScope.cancel()
        server.close() // also closes the relay's store
    }
}
