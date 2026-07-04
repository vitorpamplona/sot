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

import ai.vespa.feed.client.DocumentId
import ai.vespa.feed.client.FeedClient
import ai.vespa.feed.client.FeedClientBuilder
import ai.vespa.feed.client.OperationParameters
import kotlinx.coroutines.future.await
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.buildJsonObject
import kotlinx.serialization.json.int
import kotlinx.serialization.json.jsonArray
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
import kotlinx.serialization.json.long
import kotlinx.serialization.json.put
import java.net.URI
import java.net.URLEncoder
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.time.Duration
import java.util.concurrent.Executors

/**
 * The real [EventIndex]: Vespa over HTTP. Writes go through Vespa's official
 * feed client (HTTP/2 multiplexed, per-doc ordering, retries built in) and are
 * AWAITED before returning — the store's read-your-writes contract needs the
 * ack, and proton makes an acked write visible to search. Reads are the plain
 * document API (get) and `/search/` (query), non-blocking via the JDK client's
 * async sends on virtual threads.
 *
 * Unlimited queries are capped at [maxHits] (the app package's query profile
 * must allow it); a full-corpus walk goes through [visitIds] — the document
 * API's visit, which streams past any cap.
 *
 * Counts read `totalCount`: exact for attribute-only recall, approximate under
 * a weakAnd search term — same caveat Vespa itself carries.
 */
class VespaEventIndex(
    private val baseUrl: String = System.getenv("VESPA_URL") ?: "http://localhost:8080",
    private val maxHits: Int = 10_000,
) : EventIndex {
    private val feed: FeedClient =
        FeedClientBuilder
            .create(URI.create(baseUrl))
            // Bulk ingest keeps thousands of puts in flight; the defaults
            // (one connection, a slow-ramping throttle window) cap effective
            // concurrency in the single digits and starve a local engine.
            .setConnectionsPerEndpoint(8)
            .setMaxStreamPerConnection(256)
            // The dynamic throttler STARTS at 2 x connections in flight and
            // ramps by throughput probing — but a batched writer (putAll
            // bursts with query gaps between chunks) never sustains the probe,
            // so it idles at the floor: ~20 in flight x ~20ms/write ≈ 1k/s
            // against an engine measured taking twice that. Start the window
            // high instead; the throttler still adapts DOWN if the engine
            // pushes back. (Impl-only knob — the API interface doesn't
            // expose it, so apply reflectively and shrug it off if a future
            // client version renames it.)
            .apply {
                runCatching {
                    javaClass.getMethod("setInitialInflightFactor", Int::class.java).invoke(this, 64)
                }
            }.setRetryStrategy(
                object : FeedClient.RetryStrategy {
                    // Bounded: a dead Vespa should surface as failed ops, not a hang.
                    override fun retries() = 5
                },
            ).build()

    private val http =
        HttpClient
            .newBuilder()
            .connectTimeout(Duration.ofSeconds(5))
            // Vespa is local; never route through the egress proxy.
            .proxy(java.net.ProxySelector.of(null))
            .executor(Executors.newVirtualThreadPerTaskExecutor())
            .build()

    override suspend fun get(id: String): EventDoc? {
        val resp = send("$baseUrl/document/v1/$NAMESPACE/$DOCTYPE/docid/$id")
        if (resp.statusCode() == 404) return null
        require(resp.statusCode() < 400) { "vespa get ${resp.statusCode()}: ${resp.body().take(300)}" }
        val fields = Json.parseToJsonElement(resp.body()).jsonObject["fields"]?.jsonObject ?: return null
        return EventDoc.fromSummary(fields)
    }

    override suspend fun put(doc: EventDoc) {
        feed
            .put(
                DocumentId.of(NAMESPACE, DOCTYPE, doc.id),
                buildJsonObject { put("fields", doc.indexFields()) }.toString(),
                OperationParameters.empty(),
            ).await()
    }

    /** All puts stay in flight together — the feed client multiplexes them over HTTP/2. */
    override suspend fun putAll(docs: List<EventDoc>) {
        docs
            .map { doc ->
                feed.put(
                    DocumentId.of(NAMESPACE, DOCTYPE, doc.id),
                    buildJsonObject { put("fields", doc.indexFields()) }.toString(),
                    OperationParameters.empty(),
                )
            }.forEach { it.await() }
    }

    override suspend fun remove(id: String) {
        feed.remove(DocumentId.of(NAMESPACE, DOCTYPE, id), OperationParameters.empty()).await()
    }

    override suspend fun search(query: EventQuery): List<EventDoc> {
        val root = queryRoot(query, hits = query.limit ?: maxHits) ?: return emptyList()
        return root["children"]
            ?.jsonArray
            ?.mapNotNull { child -> child.jsonObject["fields"]?.jsonObject?.let(::summaryOrNull) }
            ?: emptyList()
    }

    /**
     * The document-API visit: a streaming scan with a selection expression and
     * continuation tokens — no result cap, no ranking, exactly what a
     * full-corpus id walk needs. Queries a selection can't express fall back
     * to the (capped) search default.
     */
    override suspend fun visitIds(
        query: EventQuery,
        onPage: suspend (List<DocRef>) -> Unit,
    ) {
        val selection = EventSelection.build(query) ?: return super.visitIds(query, onPage)
        val base =
            "$baseUrl/document/v1/$NAMESPACE/$DOCTYPE/docid" +
                "?selection=${URLEncoder.encode(selection, "UTF-8")}" +
                "&wantedDocumentCount=$VISIT_PAGE&fieldSet=$DOCTYPE:created_at"
        var continuation: String? = null
        while (true) {
            val resp = send(continuation?.let { "$base&continuation=$it" } ?: base)
            require(resp.statusCode() < 400) { "vespa visit ${resp.statusCode()}: ${resp.body().take(300)}" }
            val json = Json.parseToJsonElement(resp.body()).jsonObject
            val page =
                json["documents"]?.jsonArray?.mapNotNull { d ->
                    val obj = d.jsonObject
                    val id = obj["id"]?.jsonPrimitive?.content?.substringAfterLast(":") ?: return@mapNotNull null
                    val at =
                        obj["fields"]
                            ?.jsonObject
                            ?.get("created_at")
                            ?.jsonPrimitive
                            ?.long ?: return@mapNotNull null
                    DocRef(id, at)
                } ?: emptyList()
            if (page.isNotEmpty()) onPage(page)
            continuation = json["continuation"]?.jsonPrimitive?.content ?: return
        }
    }

    override suspend fun count(query: EventQuery): Int {
        val root = queryRoot(query, hits = 0) ?: return 0
        return root["fields"]
            ?.jsonObject
            ?.get("totalCount")
            ?.jsonPrimitive
            ?.int ?: 0
    }

    /**
     * Run [query] against `/search/` (POST — a filter with hundreds of ids or
     * authors builds YQL far past any sane URL length); null when it provably
     * matches nothing (no YQL built).
     */
    private suspend fun queryRoot(
        query: EventQuery,
        hits: Int,
    ): JsonObject? {
        val vq = EventYql.build(query) ?: return null
        val body =
            buildJsonObject {
                put("yql", vq.yql)
                put("hits", hits.toString())
                put("ranking", vq.ranking)
                vq.params.forEach { (k, v) -> put(k, v) }
            }.toString()
        val req =
            HttpRequest
                .newBuilder(URI.create("$baseUrl/search/"))
                .timeout(Duration.ofSeconds(60))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(body))
                .build()
        val resp = http.sendAsync(req, HttpResponse.BodyHandlers.ofString()).await()
        require(resp.statusCode() < 400) { "vespa search ${resp.statusCode()}: ${resp.body().take(300)}" }
        return Json.parseToJsonElement(resp.body()).jsonObject["root"]?.jsonObject
    }

    /** Grouping/meta children have no event fields; skip anything that doesn't parse as a doc. */
    private fun summaryOrNull(fields: JsonObject): EventDoc? = runCatching { EventDoc.fromSummary(fields) }.getOrNull()

    private suspend fun send(url: String): HttpResponse<String> {
        val req =
            HttpRequest
                .newBuilder(URI.create(url))
                .timeout(Duration.ofSeconds(30))
                .GET()
                .build()
        return http.sendAsync(req, HttpResponse.BodyHandlers.ofString()).await()
    }

    /** Graceful: waits for in-flight feed operations before closing the connections. */
    override fun close() = feed.close(true)

    private companion object {
        const val NAMESPACE = "event"
        const val DOCTYPE = "event"

        /** Docs asked for per visit response (Vespa's per-request ceiling is 1024). */
        const val VISIT_PAGE = 1024
    }
}
