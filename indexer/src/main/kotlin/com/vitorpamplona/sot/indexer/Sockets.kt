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

import com.vitorpamplona.quartz.nip01Core.relay.normalizer.NormalizedRelayUrl
import com.vitorpamplona.quartz.nip01Core.relay.sockets.WebsocketBuilder
import com.vitorpamplona.quartz.nip01Core.relay.sockets.okhttp.BasicOkHttpWebSocket
import okhttp3.Dispatcher
import okhttp3.OkHttpClient
import java.net.Proxy

/**
 * Quartz's JVM websocket transport, backed by a single shared OkHttpClient.
 *
 * Relay connections go out *directly* (Proxy.NO_PROXY), not through the egress
 * proxy — the JVM's https.proxyHost system property would otherwise tunnel wss
 * through 127.0.0.1, which the relays aren't reachable behind. Direct wss egress
 * to public relays works in this environment.
 *
 * The dispatcher limits are lifted to Amethyst's values (1024 total, 1024
 * per-host): OkHttp defaults to 64 concurrent calls / 5 per host, which would
 * silently queue relay connects once the sync fans out to dozens of relays — the
 * cap has to sit above the sync concurrency, not below it.
 */
fun okHttpWebsocketBuilder(): WebsocketBuilder {
    val shared =
        OkHttpClient
            .Builder()
            .proxy(Proxy.NO_PROXY)
            .dispatcher(
                Dispatcher().apply {
                    maxRequests = 1024
                    maxRequestsPerHost = 1024
                },
            ).build()
    return BasicOkHttpWebSocket.Builder { _: NormalizedRelayUrl -> shared }
}
