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
package com.vitorpamplona.sot.cli

import com.vitorpamplona.quartz.eventstore.store.VespaEventStore
import com.vitorpamplona.quartz.eventstore.vespa.InMemoryEventIndex
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.RelayUrlNormalizer
import com.vitorpamplona.quartz.nip01Core.signers.NostrSignerSync
import com.vitorpamplona.sot.relay.SotRelayServer
import com.vitorpamplona.sot.relay.nostrRelay
import com.vitorpamplona.sot.relay.relayInfoJson
import io.ktor.http.ContentType
import io.ktor.server.application.install
import io.ktor.server.engine.embeddedServer
import io.ktor.server.netty.Netty
import io.ktor.server.response.respondText
import io.ktor.server.routing.get
import io.ktor.server.routing.routing
import io.ktor.server.websocket.WebSockets
import kotlinx.coroutines.runBlocking

/**
 * Web-UI development server: `./gradlew :cli:uiDemo` serves
 * `web/index.html` over the REAL relay engine backed by the in-memory
 * index — no Vespa, no docker, no network — seeded with a few signed events
 * of different kinds so every card renderer has something to show. NIP-42
 * auth works (sign with any key); ranking features don't (the in-memory
 * index matches text but doesn't score).
 */
object UiDemoServer {
    @JvmStatic
    fun main(args: Array<String>) {
        val port = args.firstOrNull()?.toIntOrNull() ?: 7788
        val relayUrl = RelayUrlNormalizer.normalize("ws://127.0.0.1:$port")
        val store = VespaEventStore(InMemoryEventIndex(), relay = relayUrl)
        runBlocking { seed(store) }
        val relaySrv =
            SotRelayServer(
                store = store,
                defaultObserver = null,
                relayUrl = relayUrl,
                onObserver = { println("[demo] enrolled observer $it") },
            )
        val ui =
            Thread
                .currentThread()
                .contextClassLoader
                .getResource("index.html")!!
                .readText()
        println("[demo] http://127.0.0.1:$port  (in-memory relay; ctrl-c to stop)")
        embeddedServer(Netty, host = "127.0.0.1", port = port) {
            install(WebSockets)
            routing {
                nostrRelay(relaySrv)
                get("/") {
                    val accept = call.request.headers["Accept"] ?: ""
                    if (accept.contains("application/nostr+json")) {
                        call.respondText(relayInfoJson(name = "sot-ui-demo"), ContentType.parse("application/nostr+json"))
                    } else {
                        call.respondText(ui, ContentType.Text.Html)
                    }
                }
            }
        }.start(wait = true)
    }

    private suspend fun seed(store: VespaEventStore) {
        val vitor = NostrSignerSync()
        val bob = NostrSignerSync()

        store.insert(
            vitor.sign(
                1_700_000_000,
                0,
                emptyArray(),
                """{"name":"vitor-demo","display_name":"Vitor (demo)","about":"Building a vespa-backed web-of-trust search.","nip05":"vitor@example.com","website":"https://example.com"}""",
            ),
        )
        store.insert(
            bob.sign(1_700_000_001, 0, emptyArray(), """{"name":"bob","about":"Sails and streams."}"""),
        )
        store.insert(
            bob.sign(1_700_000_100, 1, arrayOf(arrayOf("t", "search")), "Just tried the new vespa index for nostr search - the trust ranking is impressive."),
        )
        store.insert(
            vitor.sign(
                1_700_000_200,
                30023,
                arrayOf(
                    arrayOf("d", "sot-vespa"),
                    arrayOf("title", "Search over Trust, on Vespa"),
                    arrayOf("summary", "How sot stores every event in the search engine and ranks by your web of trust."),
                ),
                "Long-form body: the store IS the index. Vespa holds the events, the trust tensors, and the rank profiles...",
            ),
        )
        store.insert(
            vitor.sign(
                1_700_000_300,
                30617,
                arrayOf(arrayOf("d", "vespa-tools"), arrayOf("name", "vespa-tools"), arrayOf("description", "Utilities for the sot vespa deployment.")),
                "",
            ),
        )
        store.insert(
            bob.sign(
                1_700_000_400,
                1337,
                arrayOf(arrayOf("name", "vespa-query.kt"), arrayOf("l", "kotlin"), arrayOf("description", "Building the vespa YQL word groups.")),
                "fun wordGroup(w: String) = \"(...)\" // vespa fuzzy recall",
            ),
        )
    }
}
