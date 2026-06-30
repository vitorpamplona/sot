package com.vitorpamplona.vespasearch.indexer

import com.vitorpamplona.quartz.nip01Core.relay.normalizer.NormalizedRelayUrl
import com.vitorpamplona.quartz.nip01Core.relay.sockets.WebsocketBuilder
import com.vitorpamplona.quartz.nip01Core.relay.sockets.okhttp.BasicOkHttpWebSocket
import java.net.Proxy
import okhttp3.OkHttpClient

/**
 * Quartz's JVM websocket transport, backed by a single shared OkHttpClient.
 *
 * Relay connections go out *directly* (Proxy.NO_PROXY), not through the egress
 * proxy — the JVM's https.proxyHost system property would otherwise tunnel wss
 * through 127.0.0.1, which the relays aren't reachable behind. Direct wss egress
 * to public relays works in this environment.
 */
fun okHttpWebsocketBuilder(): WebsocketBuilder {
    val shared =
        OkHttpClient
            .Builder()
            .proxy(Proxy.NO_PROXY)
            .build()
    return BasicOkHttpWebSocket.Builder { _: NormalizedRelayUrl -> shared }
}
