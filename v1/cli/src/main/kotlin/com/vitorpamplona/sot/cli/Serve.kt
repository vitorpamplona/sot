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

import com.vitorpamplona.quartz.nip19Bech32.toNpub
import com.vitorpamplona.sot.config.Config
import com.vitorpamplona.sot.http.searchApi
import com.vitorpamplona.sot.indexer.SyncOptions
import com.vitorpamplona.sot.indexer.SyncService
import com.vitorpamplona.sot.relay.buildRelayServer
import com.vitorpamplona.sot.relay.nostrRelay
import com.vitorpamplona.sot.relay.relayInfoJson
import com.vitorpamplona.sot.store.openObservableStore
import com.vitorpamplona.sot.store.relayIdentity
import com.vitorpamplona.sot.vespa.VespaSearch
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
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlin.time.Duration.Companion.minutes
import kotlin.time.Duration.Companion.seconds

/*
 * `sot serve` — THE long-running process: one Ktor app on one port
 * ([Config.serverPort]) serving everything but Vespa —
 *   WS   /        -> NIP-50 relay        (from :relay)
 *   GET  /        -> NIP-11 (Accept: application/nostr+json) or the web UI
 *   GET  /search  -> JSON search API     (from :http)
 * plus, unless SYNC_INTERVAL=0, a background [SyncService] loop that keeps the
 * index fresh with incremental passes. Both sides share ONE event store, so
 * everything the sync inserts flows to Vespa through the same change feed.
 *
 * The web UI is bundled from `web/index.html`; because it's same-origin with the
 * API, no CORS is needed for it (CORS stays only for other-origin/file:// callers).
 */
private val WEB_UI: String? by lazy {
    Thread
        .currentThread()
        .contextClassLoader
        ?.getResource("index.html")
        ?.readText()
}

internal fun serve(args: List<String>) {
    ensureVespaIsUp(args)
    val identity = serverSigner()
    logLine("relay identity (NIP-11 self): ${identity.keyPair.pubKey.toNpub()}")
    val vespa = VespaSearch(Config.vespaUrl)
    // One shared event store (see :event-store): relay identity from env for NIP-62, no SQLite FTS.
    val relayUrl = relayIdentity()
    val store = openObservableStore()
    val relaySrv = buildRelayServer(vespa, store, Config.defaultObserver, relayUrl)

    val syncScope = CoroutineScope(Dispatchers.IO + SupervisorJob())
    val syncMinutes = Config.syncIntervalMinutes
    val sync =
        if (syncMinutes > 0) {
            logLine("background sync: every ${syncMinutes}m (SYNC_INTERVAL=0 to disable)")
            SyncService(store, Config.vespaUrl, Config.seedRelays, "${Config.eventsDb}.state.json", SyncOptions(), ::logLine, signer = identity)
                .also { s -> syncScope.launch { s.runForever(syncMinutes.minutes) } }
        } else {
            logLine("background sync: disabled (SYNC_INTERVAL=0)")
            null
        }

    // Ctrl-C / SIGTERM: stop syncing, give in-flight Vespa writes a short window
    // (whatever misses heals on the next start's pass), release everything.
    Runtime.getRuntime().addShutdownHook(
        Thread {
            syncScope.cancel()
            runBlocking { sync?.drain(10.seconds) }
            sync?.close()
            store.close()
        },
    )

    embeddedServer(Netty, port = Config.serverPort) {
        install(WebSockets)
        install(ContentNegotiation) { json() }
        install(CORS) {
            anyHost()
            allowMethod(HttpMethod.Get)
        }
        routing {
            searchApi(vespa, Config.defaultObserver)
            nostrRelay(relaySrv)
            get("/") {
                val accept = call.request.headers["Accept"] ?: ""
                if (accept.contains("application/nostr+json")) {
                    call.respondText(relayInfoJson(selfPubkey = identity.pubKey), ContentType.parse("application/nostr+json"))
                } else {
                    WEB_UI?.let { call.respondText(it, ContentType.Text.Html) }
                        ?: call.respondText("sot server - open a WebSocket for NIP-50, or GET /search")
                }
            }
        }
    }.start(wait = true)
}
