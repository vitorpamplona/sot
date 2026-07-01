package com.vitorpamplona.sot.relay

import com.vitorpamplona.quartz.nip01Core.relay.normalizer.NormalizedRelayUrl
import com.vitorpamplona.quartz.nip01Core.relay.server.EventSourceServer
import com.vitorpamplona.quartz.nip01Core.relay.server.policies.OptionalAuthPolicy
import com.vitorpamplona.quartz.nip01Core.store.IEventStore
import com.vitorpamplona.sot.query.VespaSearch
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

/** NIP-11 relay information document (served on `GET /` with Accept: application/nostr+json). */
const val NIP11_JSON =
    """{"name":"sot","description":"NIP-50 profile search ranked by Nostr web-of-trust",""" +
        """"supported_nips":[1,11,42,50],"software":"https://github.com/vitorpamplona/sot","version":"0.1"}"""

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
