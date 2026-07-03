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

import com.sun.net.httpserver.HttpServer
import com.vitorpamplona.quartz.nip01Core.metadata.MetadataEvent
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.RelayUrlNormalizer
import com.vitorpamplona.quartz.nip01Core.signers.NostrSignerSync
import com.vitorpamplona.quartz.nip01Core.store.sqlite.DefaultIndexingStrategy
import com.vitorpamplona.quartz.nip01Core.store.sqlite.EventStore
import com.vitorpamplona.quartz.nip42RelayAuth.RelayAuthEvent
import com.vitorpamplona.sot.vespa.VespaSearch
import kotlinx.coroutines.runBlocking
import java.io.File
import java.net.InetSocketAddress
import java.net.URLDecoder
import java.util.Collections
import kotlin.test.AfterTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue
import kotlin.test.fail

/**
 * The NIP-50 relay engine over the raw NIP-01 wire protocol (no sockets — a
 * direct [com.vitorpamplona.quartz.nip01Core.relay.server.RelaySession]):
 * a search REQ streams EVENT + EOSE, and NIP-42 auth switches the ranking
 * observer — the promise that an authenticated user searches through their OWN
 * web of trust. The canned Vespa records which observer each query ranked by.
 */
class NostrRelayProtocolTest {
    private val alice = "1".repeat(64)
    private val defaultObserver = "d".repeat(64)
    private val relayUrl = RelayUrlNormalizer.normalize("ws://localhost:7777")

    /** The `user_q` observer of every /search/ call, in order. */
    private val rankedBy = Collections.synchronizedList(mutableListOf<String>())

    private val vespaMock =
        HttpServer.create(InetSocketAddress("127.0.0.1", 0), 0).apply {
            createContext("/search/") { ex ->
                val userQ =
                    ex.requestURI.rawQuery
                        .split("&")
                        .map { URLDecoder.decode(it.substringBefore("="), "UTF-8") to URLDecoder.decode(it.substringAfter("="), "UTF-8") }
                        .first { it.first == "ranking.features.query(user_q)" }
                        .second
                rankedBy.add(userQ.removePrefix("{").substringBefore(":"))
                val body =
                    """{"root":{"children":[{"relevance":1.0,"fields":{"pubkey":"$alice","matchfeatures":{"user_score":5.0}}}]}}"""
                        .encodeToByteArray()
                ex.sendResponseHeaders(200, body.size.toLong())
                ex.responseBody.use { it.write(body) }
            }
            start()
        }

    private val store =
        EventStore(
            File.createTempFile("relay-protocol", ".db").also { it.delete() }.path,
            relayUrl,
            DefaultIndexingStrategy(indexFullTextSearch = false),
        )

    @AfterTest
    fun tearDown() {
        store.close()
        vespaMock.stop(0)
    }

    @Test
    fun `REQ streams ranked events, and NIP-42 auth switches the observer`() =
        runBlocking {
            store.insert(MetadataEvent("4".repeat(64), alice, 1_700_000_000L, emptyArray(), """{"name":"alice"}""", ""))

            val server = buildRelayServer(VespaSearch("http://127.0.0.1:${vespaMock.address.port}"), store, defaultObserver, relayUrl)
            val out = Collections.synchronizedList(mutableListOf<String>())
            val session = server.connect { out.add(it) }

            try {
                // The relay advertises NIP-42 on connect.
                val challenge = awaitMessage(out) { it.startsWith("""["AUTH",""") }.substringAfter("""["AUTH","""").substringBefore('"')

                // Unauthenticated search: ranked by the DEFAULT observer.
                session.receive("""["REQ","s1",{"kinds":[0],"search":"ali","limit":10}]""")
                awaitMessage(out) { it.startsWith("""["EOSE","s1"]""") }
                assertTrue(out.any { it.startsWith("""["EVENT","s1",""") && alice in it }, "the stored kind-0 streams back: $out")
                assertEquals(listOf(defaultObserver), rankedBy.toList())

                // Authenticate with a real signed kind-22242, then search again.
                val signer = NostrSignerSync()
                val auth = signer.sign(RelayAuthEvent.build(relayUrl, challenge))
                session.receive("""["AUTH",${auth.toJson()}]""")
                awaitMessage(out) { it.startsWith("""["OK","${auth.id}",true""") }

                session.receive("""["REQ","s2",{"kinds":[0],"search":"ali","limit":10}]""")
                awaitMessage(out) { it.startsWith("""["EOSE","s2"]""") }
                assertEquals(signer.pubKey, rankedBy.last(), "an authenticated user searches through their OWN web of trust")
            } finally {
                session.close()
                server.close()
            }
        }

    private fun awaitMessage(
        out: List<String>,
        match: (String) -> Boolean,
    ): String {
        val deadline = System.currentTimeMillis() + 10_000
        while (System.currentTimeMillis() < deadline) {
            synchronized(out) { out.firstOrNull(match) }?.let { return it }
            Thread.sleep(20)
        }
        fail("timed out waiting for a matching relay message; got: $out")
    }
}
