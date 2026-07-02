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
package com.vitorpamplona.sot.relay

import com.vitorpamplona.quartz.nip01Core.relay.normalizer.NormalizedRelayUrl
import com.vitorpamplona.quartz.nip01Core.relay.server.EventSourceServer
import com.vitorpamplona.quartz.nip01Core.relay.server.policies.OptionalAuthPolicy
import com.vitorpamplona.quartz.nip01Core.store.IEventStore
import com.vitorpamplona.quartz.nip11RelayInfo.relayInformation
import com.vitorpamplona.sot.config.Config
import com.vitorpamplona.sot.vespa.VespaSearch
import io.ktor.server.routing.Route
import io.ktor.server.websocket.webSocket
import io.ktor.websocket.Frame
import io.ktor.websocket.readText
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.launch

/**
 * The NIP-50 search relay surface. A library that the :server composition root
 * mounts on the shared Ktor app. Ranks profiles in Vespa (via :query-engine) and
 * streams back the original signed kind-0 events from the local EventStore, in
 * rank order. NIP-42 optional auth ([OptionalAuthPolicy]) picks the observer.
 */

private const val SOFTWARE = "https://github.com/vitorpamplona/sot"
private const val VERSION = "0.1"

/**
 * NIP-11 relay information document (served on `GET /` with Accept:
 * application/nostr+json), built with Quartz's `relayInformation {}` DSL.
 * Identity fields come from config (`.env`); the technical fields describe what
 * this server actually is — a read-only, optional-auth NIP-50 profile search.
 */
fun relayInfoJson(selfPubkey: String? = null): String =
    relayInformation {
        name = Config.serverName.ifBlank { "sot" }
        Config.serverDescription.takeIf { it.isNotBlank() }?.let { description = it }
        Config.serverIcon.takeIf { it.isNotBlank() }?.let { icon = it }
        Config.serverPubkey.takeIf { it.isNotBlank() }?.let { pubkey = it } // admin contact
        selfPubkey?.takeIf { it.isNotBlank() }?.let { self = it } // the relay's OWN key (SERVER_NSEC)
        software = SOFTWARE
        version = VERSION
        supports(1, 11, 42, 50) // NIP-01/11/42/50
        limitation {
            authRequired = false // NIP-42 optional (OptionalAuthPolicy)
            paymentRequired = false
            restrictedWrites = true // search-only: this relay doesn't accept event writes
            defaultLimit = 50 // SearchEventSource: f.limit ?: 50
            maxLimit = 400 // SearchEventSource: coerceIn(1, 400)
        }
    }.toJson()

/** Build the relay engine from the search core + local store; [relayUrl] is the public ws url for NIP-42. */
fun buildRelayServer(
    vespa: VespaSearch,
    store: IEventStore,
    defaultObserver: String,
    relayUrl: NormalizedRelayUrl,
): EventSourceServer =
    EventSourceServer(
        source = SearchEventSource(vespa, store, defaultObserver),
        policyBuilder = { OptionalAuthPolicy(relayUrl) },
    )

/** Mount the NIP-50 relay websocket on `/`. */
fun Route.nostrRelay(server: EventSourceServer) {
    webSocket("/") {
        // One writer coroutine drains an ordered queue to the socket; the engine's
        // send callback is non-suspend, so we bridge via the channel.
        val outCh = Channel<String>(Channel.UNLIMITED)
        val writer = launch { for (text in outCh) outgoing.send(Frame.Text(text)) }
        try {
            server.serve(
                send = { outCh.trySend(it) },
                incoming = { session ->
                    for (frame in incoming) {
                        if (frame is Frame.Text) session.receive(frame.readText())
                    }
                },
            )
        } finally {
            outCh.close()
            writer.cancel()
        }
    }
}
