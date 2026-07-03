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
package com.vitorpamplona.sot.vespa

import com.sun.net.httpserver.HttpServer
import kotlinx.coroutines.runBlocking
import java.net.InetSocketAddress
import java.net.URLDecoder
import kotlin.test.AfterTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

/**
 * The query side against a canned `/search/` endpoint: the request must carry
 * the YQL recall plus the observer-weighted ranking inputs, and the response
 * children must map to [SearchHit]s (trust from matchfeatures, zero-trust hits
 * dropped when asked).
 */
class VespaSearchTest {
    private val observer = "0".repeat(64)
    private val ranked = "1".repeat(64)
    private val unranked = "2".repeat(64)

    private var lastQuery: Map<String, String> = emptyMap()

    private val server =
        HttpServer.create(InetSocketAddress("127.0.0.1", 0), 0).apply {
            createContext("/search/") { ex ->
                lastQuery =
                    ex.requestURI.rawQuery
                        .split("&")
                        .associate { p ->
                            URLDecoder.decode(p.substringBefore("="), "UTF-8") to URLDecoder.decode(p.substringAfter("="), "UTF-8")
                        }
                val body =
                    """
                    {"root":{"children":[
                      {"relevance":2.5,"fields":{"pubkey":"$ranked","name":"vitor","matchfeatures":{"user_score":9.0}}},
                      {"relevance":1.0,"fields":{"pubkey":"$unranked","name":"other","matchfeatures":{"user_score":0.0}}}
                    ]}}
                    """.trimIndent().encodeToByteArray()
                ex.sendResponseHeaders(200, body.size.toLong())
                ex.responseBody.use { it.write(body) }
            }
            start()
        }

    private val search = VespaSearch("http://127.0.0.1:${server.address.port}")

    @AfterTest
    fun tearDown() = server.stop(0)

    @Test
    fun `sends recall words and observer-weighted ranking inputs`() =
        runBlocking {
            search.search("vitor pamplona", observer)

            assertEquals("{$observer:1.0}", lastQuery["ranking.features.query(user_q)"])
            assertEquals("vitor", lastQuery["w0"])
            assertEquals("pamplona", lastQuery["w1"])
            assertEquals("vitorpamplona", lastQuery["wj"], "multi-word queries also try the joined form")
            assertTrue(lastQuery["yql"].orEmpty().startsWith("select"), "yql: ${lastQuery["yql"]}")
            assertEquals("name_and_quality_score_only", lastQuery["ranking"])
        }

    @Test
    fun `maps children to hits with trust from matchfeatures`() =
        runBlocking {
            val hits = search.search("vitor", observer)

            assertEquals(2, hits.size)
            assertEquals(ranked, hits[0].pubkey)
            assertEquals(9.0, hits[0].trust)
            assertEquals(2.5, hits[0].relevance)
            assertEquals("vitor", hits[0].name)
            assertTrue("matchfeatures" !in hits[0].fields, "the tensor blob stays out of the string fields")
        }

    @Test
    fun `includeZeroScore=false drops untrusted hits`() =
        runBlocking {
            val hits = search.search("vitor", observer, SearchOptions(includeZeroScore = false))

            assertEquals(listOf(ranked), hits.map { it.pubkey })
        }

    @Test
    fun `a blank query never reaches Vespa`() =
        runBlocking {
            lastQuery = emptyMap()
            assertEquals(emptyList(), search.search("   ", observer))
            assertTrue(lastQuery.isEmpty())
        }
}
