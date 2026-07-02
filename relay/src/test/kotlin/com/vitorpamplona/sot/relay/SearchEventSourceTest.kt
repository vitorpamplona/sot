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
import com.vitorpamplona.quartz.nip01Core.relay.filters.Filter
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.RelayUrlNormalizer
import com.vitorpamplona.quartz.nip01Core.relay.server.backend.RequestContext
import com.vitorpamplona.quartz.nip01Core.relay.server.policies.IRelayPolicy
import com.vitorpamplona.quartz.nip01Core.store.sqlite.DefaultIndexingStrategy
import com.vitorpamplona.quartz.nip01Core.store.sqlite.EventStore
import com.vitorpamplona.sot.vespa.VespaSearch
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.runBlocking
import java.io.File
import java.net.InetSocketAddress
import kotlin.test.AfterTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

/**
 * The NIP-50 source: a `search` filter ranks pubkeys in (a canned) Vespa and
 * streams the ORIGINAL kind-0 events from the local store, preserving the rank
 * order; filters without a search term are ignored.
 */
class SearchEventSourceTest {
    private val alice = "1".repeat(64)
    private val bob = "2".repeat(64)

    // Vespa ranks bob ABOVE alice - the store must not reorder them.
    private val vespaMock =
        HttpServer.create(InetSocketAddress("127.0.0.1", 0), 0).apply {
            createContext("/search/") { ex ->
                val body =
                    """
                    {"root":{"children":[
                      {"relevance":2.0,"fields":{"pubkey":"$bob","matchfeatures":{"user_score":9.0}}},
                      {"relevance":1.0,"fields":{"pubkey":"$alice","matchfeatures":{"user_score":1.0}}}
                    ]}}
                    """.trimIndent().encodeToByteArray()
                ex.sendResponseHeaders(200, body.size.toLong())
                ex.responseBody.use { it.write(body) }
            }
            start()
        }

    private val store =
        EventStore(
            File.createTempFile("search-source", ".db").also { it.delete() }.path,
            RelayUrlNormalizer.normalize("ws://localhost:7777"),
            DefaultIndexingStrategy(indexFullTextSearch = false),
        )

    private val source = SearchEventSource(VespaSearch("http://127.0.0.1:${vespaMock.address.port}"), store, defaultObserver = "")

    private val ctx =
        object : RequestContext {
            override val connectionId = 1L
            override val policy: IRelayPolicy get() = error("unused by SearchEventSource")
            override val authenticatedUsers = emptySet<String>()
        }

    @AfterTest
    fun tearDown() {
        store.close()
        vespaMock.stop(0)
    }

    @Test
    fun `a search REQ streams the stored kind-0s in Vespa's rank order`() =
        runBlocking {
            store.insert(MetadataEvent("4".repeat(64), alice, 1_700_000_000L, emptyArray(), """{"name":"alice"}""", ""))
            store.insert(MetadataEvent("5".repeat(64), bob, 1_700_000_001L, emptyArray(), """{"name":"bob"}""", ""))

            val events = source.events(ctx, listOf(Filter(kinds = listOf(MetadataEvent.KIND), search = "ali"))).toList()

            assertEquals(listOf(bob, alice), events.map { it.pubKey }, "rank order comes from Vespa, not the store")
            assertTrue(events.all { it is MetadataEvent }, "the ORIGINAL signed events stream back")
        }

    @Test
    fun `filters without a search term are ignored by the search relay`() =
        runBlocking {
            store.insert(MetadataEvent("6".repeat(64), alice, 1_700_000_002L, emptyArray(), """{"name":"alice"}""", ""))

            val events = source.events(ctx, listOf(Filter(kinds = listOf(MetadataEvent.KIND)))).toList()

            assertEquals(0, events.size)
        }

    @Test
    fun `hits without a stored kind-0 are skipped, not fabricated`() =
        runBlocking {
            store.insert(MetadataEvent("7".repeat(64), alice, 1_700_000_003L, emptyArray(), """{"name":"alice"}""", ""))
            // bob ranks first in Vespa but has no stored event.

            val events = source.events(ctx, listOf(Filter(search = "ali"))).toList()

            assertEquals(listOf(alice), events.map { it.pubKey })
        }
}
