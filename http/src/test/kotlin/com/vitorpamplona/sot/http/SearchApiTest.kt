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
package com.vitorpamplona.sot.http

import com.sun.net.httpserver.HttpServer
import com.vitorpamplona.sot.vespa.VespaSearch
import io.ktor.client.request.get
import io.ktor.client.statement.bodyAsText
import io.ktor.serialization.kotlinx.json.json
import io.ktor.server.application.install
import io.ktor.server.plugins.contentnegotiation.ContentNegotiation
import io.ktor.server.routing.routing
import io.ktor.server.testing.testApplication
import kotlinx.serialization.json.Json
import java.net.InetSocketAddress
import kotlin.test.AfterTest
import kotlin.test.Test
import kotlin.test.assertEquals

/**
 * The `GET /search` wire contract through a real Ktor test app, with Vespa
 * replaced by a canned HTTP endpoint: free text ranks via `/search/`, a
 * pubkey-shaped query becomes a direct doc lookup, and a blank query
 * short-circuits to an empty response.
 */
class SearchApiTest {
    private val alice = "1".repeat(64)

    // Canned Vespa: one ranked hit for any text search; alice's doc by id.
    private val vespaMock =
        HttpServer.create(InetSocketAddress("127.0.0.1", 0), 0).apply {
            createContext("/search/") { ex ->
                val body =
                    """{"root":{"children":[{"relevance":2.0,"fields":{"pubkey":"$alice","name":"alice","nip05":"alice@example.com","matchfeatures":{"user_score":5.0}}}]}}"""
                        .encodeToByteArray()
                ex.sendResponseHeaders(200, body.size.toLong())
                ex.responseBody.use { it.write(body) }
            }
            createContext("/document/v1/doc/doc/docid/") { ex ->
                val hit = ex.requestURI.path.endsWith(alice)
                val body = (if (hit) """{"fields":{"pubkey":"$alice","name":"alice"}}""" else """{"message":"not found"}""").encodeToByteArray()
                ex.sendResponseHeaders(if (hit) 200 else 404, body.size.toLong())
                ex.responseBody.use { it.write(body) }
            }
            start()
        }

    @AfterTest
    fun tearDown() = vespaMock.stop(0)

    private fun withApi(test: suspend (get: suspend (String) -> SearchResponse) -> Unit) =
        testApplication {
            application {
                install(ContentNegotiation) { json() }
                routing { searchApi(VespaSearch("http://127.0.0.1:${vespaMock.address.port}"), defaultObserver = "") }
            }
            test { url -> Json.decodeFromString(SearchResponse.serializer(), client.get(url).bodyAsText()) }
        }

    @Test
    fun `free text returns ranked results`() =
        withApi { get ->
            val resp = get("/search?text=alice")
            assertEquals("alice", resp.query)
            assertEquals(1, resp.numResults)
            assertEquals(alice, resp.results.single().pubkey)
            assertEquals(5.0, resp.results.single().trust)
            assertEquals("alice@example.com", resp.results.single().nip05)
        }

    @Test
    fun `a pubkey-shaped query is a direct doc lookup`() =
        withApi { get ->
            val resp = get("/search?text=$alice")
            assertEquals(1, resp.numResults)
            assertEquals(alice, resp.results.single().pubkey)

            val missing = get("/search?text=${"2".repeat(64)}")
            assertEquals(0, missing.numResults, "an unindexed pubkey is empty, not a text search")
        }

    @Test
    fun `a blank query short-circuits to an empty response`() =
        withApi { get ->
            val resp = get("/search?text=%20%20")
            assertEquals(0, resp.numResults)
            assertEquals(emptyList(), resp.results)
        }
}
