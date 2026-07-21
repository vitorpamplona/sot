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
import com.vitorpamplona.quartz.eventstore.relay.NostrRelayServer
import com.vitorpamplona.quartz.eventstore.relay.nostrRelay
import com.vitorpamplona.quartz.eventstore.relay.relayInfoJson
import io.ktor.http.ContentType
import io.ktor.server.application.install
import io.ktor.server.engine.embeddedServer
import io.ktor.server.netty.Netty
import io.ktor.server.response.respondText
import io.ktor.server.routing.get
import io.ktor.server.routing.routing
import io.ktor.server.websocket.WebSockets
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.launch
import kotlin.time.Duration.Companion.minutes

/*
 * `sot serve` — THE long-running process: one Ktor app on one port
 * ([Config.serverPort]).
 *   WS   /  -> the NIP-50 relay (:relay; full filters, search, COUNT, NIP-77)
 *   GET  /  -> NIP-11 (Accept: application/nostr+json), else the web UI
 * Unless SYNC_INTERVAL=0, it also runs the background sync loop (:sync). Relay
 * and sync share the ONE Vespa-backed store, and the trust projection sits
 * under it. So a user who NIP-42-authenticates gets enrolled as an observer,
 * their trust chain syncs on the next pass, and their searches rank by it.
 *
 * The web UI (bundled from `web/index.html`) is itself a Nostr client: it talks
 * NIP-50 to the SAME websocket endpoint. There is no http search API to serve;
 * the relay is the API.
 */
private val WEB_UI: String? by lazy {
    Thread
        .currentThread()
        .contextClassLoader
        ?.getResource("index.html")
        ?.readText()
}

internal fun serve(args: List<String>) {
    val house = requireHouse() // the trust root; refuse to serve without it
    ensureVespaIsUp(args)
    val identity = serverIdentity()
    logLine("relay identity (NIP-11 self): ${identity.signer.keyPair.pubKey.toNpub()}")

    val stack = openStack()
    val store = stack.store
    val sync = syncService(stack, identity, house)
    val relaySrv =
        NostrRelayServer(
            store = store,
            defaultObserver = house.pubkey,
            relayUrl = publicRelayUrl(),
            onObserver = sync::enroll,
        )

    val syncScope = CoroutineScope(Dispatchers.IO + SupervisorJob())
    val syncMinutes = Config.syncIntervalMinutes
    if (syncMinutes > 0) {
        logLine("background sync: every ${syncMinutes}m (SYNC_INTERVAL=0 to disable)")
        syncScope.launch { sync.runForever(syncMinutes.minutes) }
    } else {
        logLine("background sync: disabled (SYNC_INTERVAL=0)")
    }

    Runtime.getRuntime().addShutdownHook(
        Thread {
            syncScope.cancel()
            sync.close()
            relaySrv.close()
            store.close()
        },
    )

    embeddedServer(Netty, port = Config.serverPort) {
        install(WebSockets)
        routing {
            nostrRelay(relaySrv)
            get("/") {
                val accept = call.request.headers["Accept"] ?: ""
                if (accept.contains("application/nostr+json")) {
                    call.respondText(
                        relayInfoJson(
                            name = Config.serverName,
                            description = Config.serverDescription,
                            icon = Config.serverIcon,
                            contactPubkey = Config.serverPubkey,
                            selfPubkey = identity.pubkey,
                        ),
                        ContentType.parse("application/nostr+json"),
                    )
                } else {
                    WEB_UI?.let { call.respondText(it, ContentType.Text.Html) }
                        ?: call.respondText("${Config.serverName} - a NIP-50 search relay; connect a WebSocket here (${Config.relayUrl}).")
                }
            }
        }
    }.start(wait = true)
}

/** Timestamped, styled log line for the long-running commands. */
internal fun logLine(msg: String) = println(styleLogLine(msg))
