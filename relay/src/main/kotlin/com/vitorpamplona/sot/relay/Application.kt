package com.vitorpamplona.sot.relay

import com.vitorpamplona.quartz.nip01Core.relay.normalizer.RelayUrlNormalizer
import com.vitorpamplona.quartz.nip01Core.relay.server.EventSourceServer
import com.vitorpamplona.quartz.nip01Core.relay.server.policies.OptionalAuthPolicy
import com.vitorpamplona.quartz.nip01Core.store.sqlite.EventStore
import com.vitorpamplona.sot.query.VespaSearch
import io.ktor.http.ContentType
import io.ktor.server.application.install
import io.ktor.server.engine.embeddedServer
import io.ktor.server.netty.Netty
import io.ktor.server.response.respondText
import io.ktor.server.routing.get
import io.ktor.server.routing.routing
import io.ktor.server.websocket.WebSockets
import io.ktor.server.websocket.webSocket
import io.ktor.websocket.Frame
import io.ktor.websocket.readText
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.launch

/**
 * NIP-50 search relay. A Quartz [EventSourceServer] whose source ([SearchEventSource])
 * ranks profiles in Vespa (via :query-engine) and returns the original signed
 * kind-0 events from the indexer's local EventStore. NIP-42 optional auth
 * ([OptionalAuthPolicy]) picks the ranking observer. Transport is Ktor WebSockets.
 *
 * Env: EVENTS_DB (SQLite path, shared with the indexer), RELAY_URL (public ws url
 *      for NIP-42), RELAY_PORT, VESPA_URL, DEFAULT_OBSERVER (fallback observer for
 *      unauthenticated clients).
 */
private val DEFAULT_OBSERVER = System.getenv("DEFAULT_OBSERVER").orEmpty()

private val NIP11 =
    """{"name":"sot","description":"NIP-50 profile search ranked by Nostr web-of-trust",""" +
        """"supported_nips":[1,11,42,50],"software":"https://github.com/vitorpamplona/sot","version":"0.1"}"""

fun main() {
    val dbPath = System.getenv("EVENTS_DB") ?: "events.db"
    val relayUrl = RelayUrlNormalizer.normalize(System.getenv("RELAY_URL") ?: "ws://localhost:7777")
    val port = (System.getenv("RELAY_PORT") ?: "7777").toInt()

    val store = EventStore(dbName = dbPath, relay = null)
    val vespa = VespaSearch()
    val server =
        EventSourceServer(
            source = SearchEventSource(vespa, store, DEFAULT_OBSERVER),
            policyBuilder = { OptionalAuthPolicy(relayUrl) },
        )

    embeddedServer(Netty, port = port) {
        install(WebSockets)
        routing {
            get("/") {
                if ((call.request.headers["Accept"] ?: "").contains("application/nostr+json")) {
                    call.respondText(NIP11, ContentType.parse("application/nostr+json"))
                } else {
                    call.respondText("sot relay - open a WebSocket and send a NIP-50 search REQ")
                }
            }
            webSocket("/") {
                // One writer coroutine drains an ordered queue to the socket; the
                // engine's send callback is non-suspend, so we bridge via the channel.
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
    }.start(wait = true)
}
