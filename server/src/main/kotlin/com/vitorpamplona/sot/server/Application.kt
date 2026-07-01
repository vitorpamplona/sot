package com.vitorpamplona.sot.server

import com.vitorpamplona.quartz.nip01Core.relay.normalizer.RelayUrlNormalizer
import com.vitorpamplona.quartz.nip01Core.store.sqlite.DefaultIndexingStrategy
import com.vitorpamplona.quartz.nip01Core.store.sqlite.EventStore
import com.vitorpamplona.sot.config.Config
import com.vitorpamplona.sot.http.searchApi
import com.vitorpamplona.sot.vespa.VespaSearch
import com.vitorpamplona.sot.relay.buildRelayServer
import com.vitorpamplona.sot.relay.relayInfoJson
import com.vitorpamplona.sot.relay.nostrRelay
import io.ktor.http.ContentType
import io.ktor.http.HttpMethod
import io.ktor.serialization.kotlinx.json.json
import io.ktor.server.application.install
import io.ktor.server.engine.embeddedServer
import io.ktor.server.netty.Netty
import io.ktor.server.plugins.contentnegotiation.ContentNegotiation
import io.ktor.server.plugins.cors.routing.CORS
import io.ktor.server.response.respondText
import io.ktor.server.routing.get
import io.ktor.server.routing.routing
import io.ktor.server.websocket.WebSockets

/**
 * The sot server: one Ktor app on one port ([Config.serverPort]) serving
 * everything but Vespa —
 *   WS   /        -> NIP-50 relay        (from :relay)
 *   GET  /        -> NIP-11 (Accept: application/nostr+json) or the web UI
 *   GET  /search  -> JSON search API     (from :http)
 * The web UI is bundled from `web/index.html`; because it's same-origin with the
 * API, no CORS is needed for it (CORS stays only for other-origin/file:// callers).
 */
private val WEB_UI: String? by lazy {
    Thread.currentThread().contextClassLoader?.getResource("index.html")?.readText()
}

fun main() {
    val vespa = VespaSearch(Config.vespaUrl)
    // relay identity (from env/.env) drives NIP-62 relay-scoped vanish; and no
    // SQLite FTS (search is Vespa) — same store strategy as the indexer.
    val relayUrl = RelayUrlNormalizer.normalize(Config.relayUrl)
    val store = EventStore(Config.eventsDb, relayUrl, DefaultIndexingStrategy(indexFullTextSearch = false))
    val relaySrv = buildRelayServer(vespa, store, Config.defaultObserver, relayUrl)

    embeddedServer(Netty, port = Config.serverPort) {
        install(WebSockets)
        install(ContentNegotiation) { json() }
        install(CORS) { anyHost(); allowMethod(HttpMethod.Get) }
        routing {
            searchApi(vespa, Config.defaultObserver)
            nostrRelay(relaySrv)
            get("/") {
                val accept = call.request.headers["Accept"] ?: ""
                if (accept.contains("application/nostr+json")) {
                    call.respondText(relayInfoJson(), ContentType.parse("application/nostr+json"))
                } else {
                    WEB_UI?.let { call.respondText(it, ContentType.Text.Html) }
                        ?: call.respondText("sot server - open a WebSocket for NIP-50, or GET /search")
                }
            }
        }
    }.start(wait = true)
}
