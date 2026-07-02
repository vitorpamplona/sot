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

import com.sun.net.httpserver.HttpServer
import com.vitorpamplona.quartz.nip01Core.metadata.MetadataEvent
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.RelayUrlNormalizer
import com.vitorpamplona.quartz.nip01Core.store.ObservableEventStore
import com.vitorpamplona.quartz.nip01Core.store.sqlite.DefaultIndexingStrategy
import com.vitorpamplona.quartz.nip01Core.store.sqlite.EventStore
import com.vitorpamplona.quartz.nip09Deletions.DeletionEvent
import com.vitorpamplona.quartz.nip85TrustedAssertions.list.TrustProviderListEvent
import com.vitorpamplona.quartz.nip85TrustedAssertions.users.ContactCardEvent
import com.vitorpamplona.sot.vespa.VespaClient
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.int
import kotlinx.serialization.json.jsonArray
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
import java.io.File
import java.net.InetSocketAddress
import java.net.URLDecoder
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.Executors
import kotlin.test.Test
import kotlin.test.fail

/**
 * End-to-end deletion semantics through a REAL event store and the REAL projection,
 * against a mock Vespa that implements the slice of the document + search API the
 * engine uses. Verifies that a kind:5 erases profile fields and score cells both
 * by raw event id (e-tag, resolved through the stored provenance ids) and by
 * address (a-tag).
 */
class DeletionFlowTest {
    // Distinct 64-hex actors / event ids.
    private val observer = "0".repeat(64)
    private val alice = "1".repeat(64) // kind:0 author
    private val service = "2".repeat(64) // 30382 signer for `observer`
    private val subject = "3".repeat(64) // who the 30382 scores

    private val idProfile = "4".repeat(64)
    private val idScore = "5".repeat(64)
    private val idProfile2 = "6".repeat(64)
    private val idScore2 = "7".repeat(64)

    private var now = 1_700_000_000L

    private fun next() = ++now

    private fun kind0(
        id: String,
        name: String,
    ) = MetadataEvent(id, alice, next(), emptyArray(), """{"name":"$name"}""", "")

    private fun list10040() = TrustProviderListEvent("a".repeat(64), observer, next(), arrayOf(arrayOf("30382:rank", service, "wss://scores.example.com/")), "", "")

    private fun card30382(
        id: String,
        rank: Int,
    ) = ContactCardEvent(id, service, next(), arrayOf(arrayOf("d", subject), arrayOf("rank", rank.toString())), "", "")

    private fun kind5(
        id: String,
        author: String,
        vararg tags: Array<String>,
    ) = DeletionEvent(id, author, next(), arrayOf(*tags), "", "")

    @Test
    fun `kind5 deletes profiles and scores by event id and by address`() =
        runBlocking {
            val mock = MockVespa()
            val db =
                File.createTempFile("deletion-flow", ".db").also {
                    it.delete()
                    it.deleteOnExit()
                }
            val store = ObservableEventStore(EventStore(db.path, RelayUrlNormalizer.normalize("ws://localhost:7777"), DefaultIndexingStrategy(indexFullTextSearch = false)))
            val writers = Executors.newFixedThreadPool(2)
            val projection = VespaProjection(store, VespaClient("http://127.0.0.1:${mock.port}"), writers) { }
            val scope = CoroutineScope(Dispatchers.IO + SupervisorJob())
            scope.launch { projection.run() }

            try {
                // Feed: provider list, profile, score.
                store.insert(list10040())
                store.insert(kind0(idProfile, "alice"))
                awaitTrue("profile indexed with provenance id") {
                    mock.docs[alice]?.get("event_id") == idProfile && mock.docs[alice]?.get("name") == "alice"
                }
                store.insert(card30382(idScore, 42))
                awaitTrue("score cell + provenance id indexed") {
                    mock.cells[subject]?.get(observer) == 42 && mock.scoreIds[subject]?.get(observer) == idScore
                }

                // Delete BY EVENT ID (e-tags).
                store.insert(kind5("b".repeat(64), alice, arrayOf("e", idProfile)))
                awaitTrue("profile blanked via e-tag id") {
                    mock.docs[alice]?.get("name") == "" && mock.docs[alice]?.get("event_id") == ""
                }
                store.insert(kind5("c".repeat(64), service, arrayOf("e", idScore)))
                awaitTrue("score cell removed via e-tag id") {
                    mock.cells[subject]?.containsKey(observer) != true && mock.scoreIds[subject]?.containsKey(observer) != true
                }

                // Re-feed, then delete BY ADDRESS (a-tags).
                store.insert(kind0(idProfile2, "alice2"))
                awaitTrue("profile re-indexed") { mock.docs[alice]?.get("name") == "alice2" }
                store.insert(card30382(idScore2, 43))
                awaitTrue("score re-indexed") { mock.cells[subject]?.get(observer) == 43 }

                store.insert(kind5("d".repeat(64), alice, arrayOf("a", "0:$alice:")))
                awaitTrue("profile blanked via a-tag address") { mock.docs[alice]?.get("name") == "" }
                store.insert(kind5("e".repeat(64), service, arrayOf("a", "30382:$service:$subject")))
                awaitTrue("score cell removed via a-tag address") { mock.cells[subject]?.containsKey(observer) != true }
            } finally {
                scope.cancel()
                writers.shutdown()
                store.close()
                mock.stop()
            }
        }

    private fun awaitTrue(
        what: String,
        cond: () -> Boolean,
    ) {
        val deadline = System.currentTimeMillis() + 10_000
        while (System.currentTimeMillis() < deadline) {
            if (cond()) return
            Thread.sleep(25)
        }
        fail("timed out waiting for: $what")
    }
}

/**
 * The slice of Vespa the engine talks to: partial-update PUTs on /document/v1
 * (assign fields, quality_scores tensor add/remove, score_event_ids{key}
 * assign/remove) and the two provenance lookups on /search/.
 */
private class MockVespa {
    val docs = ConcurrentHashMap<String, MutableMap<String, String>>()
    val cells = ConcurrentHashMap<String, MutableMap<String, Int>>()
    val scoreIds = ConcurrentHashMap<String, MutableMap<String, String>>()

    private val server =
        HttpServer.create(InetSocketAddress("127.0.0.1", 0), 0).apply {
            createContext("/") { ex ->
                val body =
                    try {
                        handle(ex.requestMethod, ex.requestURI.path, ex.requestURI.rawQuery, ex.requestBody.readBytes().decodeToString())
                    } catch (e: Exception) {
                        ex.sendResponseHeaders(500, 0)
                        ex.responseBody.close()
                        return@createContext
                    }
                val bytes = body.encodeToByteArray()
                ex.sendResponseHeaders(200, bytes.size.toLong())
                ex.responseBody.use { it.write(bytes) }
            }
            start()
        }

    val port: Int get() = server.address.port

    fun stop() = server.stop(0)

    private fun handle(
        method: String,
        path: String,
        rawQuery: String?,
        body: String,
    ): String {
        if (method == "PUT" && path.startsWith("/document/v1/doc/doc/docid/")) {
            applyUpdate(path.substringAfterLast("/"), body)
            return """{"id":"$path"}"""
        }
        if (method == "GET" && path == "/search/") {
            val yql = URLDecoder.decode(rawQuery.orEmpty().substringAfter("yql=").substringBefore("&"), "UTF-8")
            return search(yql)
        }
        error("unexpected request: $method $path")
    }

    private fun applyUpdate(
        docId: String,
        body: String,
    ) {
        val fields = Json.parseToJsonElement(body).jsonObject["fields"]!!.jsonObject
        for ((name, op) in fields) {
            val opObj = op.jsonObject
            when {
                name == "quality_scores" -> {
                    opObj["add"]?.jsonObject?.get("cells")?.jsonArray?.forEach { cell ->
                        val user =
                            cell.jsonObject["address"]!!
                                .jsonObject["user"]!!
                                .jsonPrimitive.content
                        val value = cell.jsonObject["value"]!!.jsonPrimitive.int
                        cells.getOrPut(docId) { ConcurrentHashMap() }[user] = value
                    }
                    opObj["remove"]?.jsonObject?.get("addresses")?.jsonArray?.forEach { addr ->
                        cells[docId]?.remove(addr.jsonObject["user"]!!.jsonPrimitive.content)
                    }
                }

                name.startsWith("score_event_ids{") -> {
                    val key = name.substringAfter("{").substringBefore("}")
                    val assigned = opObj["assign"]?.jsonPrimitive?.content
                    if (assigned != null) {
                        scoreIds.getOrPut(docId) { ConcurrentHashMap() }[key] = assigned
                    } else {
                        scoreIds[docId]?.remove(key)
                    }
                }

                else -> {
                    docs.getOrPut(docId) { ConcurrentHashMap() }[name] = opObj["assign"]!!.jsonPrimitive.content
                }
            }
        }
    }

    private fun search(yql: String): String {
        PROFILE_LOOKUP.find(yql)?.let { m ->
            val id = m.groupValues[1]
            val hit = docs.entries.firstOrNull { it.value["event_id"] == id } ?: return NO_HITS
            return """{"root":{"children":[{"fields":{"pubkey":"${hit.key}"}}]}}"""
        }
        SCORE_LOOKUP.find(yql)?.let { m ->
            val id = m.groupValues[1]
            for ((doc, map) in scoreIds) {
                val entries = map.entries.joinToString(",") { """"${it.key}":"${it.value}"""" }
                if (map.values.contains(id)) {
                    return """{"root":{"children":[{"fields":{"pubkey":"$doc","score_event_ids":{$entries}}}]}}"""
                }
            }
            return NO_HITS
        }
        error("unexpected yql: $yql")
    }

    private companion object {
        val PROFILE_LOOKUP = Regex("""event_id contains "([0-9a-f]{64})"""")
        val SCORE_LOOKUP = Regex("""sameElement\(value contains "([0-9a-f]{64})"\)""")
        const val NO_HITS = """{"root":{"children":[]}}"""
    }
}
