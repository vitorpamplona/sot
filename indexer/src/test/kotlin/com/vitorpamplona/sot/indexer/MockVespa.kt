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

import kotlinx.serialization.json.Json
import kotlinx.serialization.json.int
import kotlinx.serialization.json.jsonArray
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
import org.eclipse.jetty.http.HttpHeader
import org.eclipse.jetty.http2.server.HTTP2CServerConnectionFactory
import org.eclipse.jetty.io.Content
import org.eclipse.jetty.server.Handler
import org.eclipse.jetty.server.HttpConfiguration
import org.eclipse.jetty.server.HttpConnectionFactory
import org.eclipse.jetty.server.Request
import org.eclipse.jetty.server.Response
import org.eclipse.jetty.server.Server
import org.eclipse.jetty.server.ServerConnector
import org.eclipse.jetty.util.Callback
import java.net.URLDecoder
import java.util.concurrent.ConcurrentHashMap

/**
 * The slice of Vespa the engine talks to: partial-update PUTs on /document/v1
 * (assign fields, quality_scores tensor add/remove, score_event_ids{key}
 * assign/remove) and the provenance lookups on /search/.
 *
 * Runs on Jetty with HTTP/1.1 AND clear-text HTTP/2 on the same port: the
 * feed client writes over h2c (it refuses HTTP/1.1) while the query side and
 * the feed client's `?dryRun=true` handshake stay plain.
 */
internal class MockVespa {
    val docs = ConcurrentHashMap<String, MutableMap<String, String>>()
    val cells = ConcurrentHashMap<String, MutableMap<String, Int>>()
    val scoreIds = ConcurrentHashMap<String, MutableMap<String, String>>()

    private val server =
        Server().apply {
            val config = HttpConfiguration()
            addConnector(
                ServerConnector(this, HttpConnectionFactory(config), HTTP2CServerConnectionFactory(config)).apply {
                    host = "127.0.0.1"
                    port = 0
                },
            )
            handler =
                object : Handler.Abstract() {
                    override fun handle(
                        request: Request,
                        response: Response,
                        callback: Callback,
                    ): Boolean {
                        val body =
                            try {
                                val reqBody =
                                    Content.Source
                                        .asInputStream(request)
                                        .readBytes()
                                        .decodeToString()
                                handle(request.method, request.httpURI.path, request.httpURI.query, reqBody)
                            } catch (e: Exception) {
                                response.status = 500
                                callback.succeeded()
                                return true
                            }
                        response.status = 200
                        response.headers.put(HttpHeader.CONTENT_TYPE, "application/json")
                        Content.Sink.write(response, true, body, callback)
                        return true
                    }
                }
            start()
        }

    val port: Int get() = (server.connectors[0] as ServerConnector).localPort

    fun stop() = server.stop()

    private fun handle(
        method: String,
        path: String,
        rawQuery: String?,
        body: String,
    ): String {
        // The feed client opens with a no-op handshake request (?dryRun=true).
        if (rawQuery.orEmpty().contains("dryRun=true")) return "{}"
        if (method == "PUT" && path.startsWith("/document/v1/doc/doc/docid/")) {
            applyUpdate(path.substringAfterLast("/"), body)
            return """{"id":"$path"}"""
        }
        // Visit: every doc's provenance fields in one page (no continuation).
        if (method == "GET" && path == "/document/v1/doc/doc/docid") {
            val documents =
                (docs.keys + cells.keys + scoreIds.keys).joinToString(",") { pk ->
                    val ids = scoreIds[pk].orEmpty().entries.joinToString(",") { """"${it.key}":"${it.value}"""" }
                    """{"id":"id:doc:doc::$pk","fields":{"pubkey":"$pk","event_id":"${docs[pk]?.get("event_id").orEmpty()}","score_event_ids":{$ids}}}"""
                }
            return """{"pathId":"/document/v1/doc/doc/docid","documents":[$documents]}"""
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
        OBSERVER_LOOKUP.find(yql)?.let { m ->
            val observer = m.groupValues[1]
            val subjects = scoreIds.filterValues { it.containsKey(observer) }.keys
            val children = subjects.joinToString(",") { """{"fields":{"pubkey":"$it"}}""" }
            return """{"root":{"children":[$children]}}"""
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
        val OBSERVER_LOOKUP = Regex("""sameElement\(key contains "([0-9a-f]{64})"\)""")
        const val NO_HITS = """{"root":{"children":[]}}"""
    }
}
