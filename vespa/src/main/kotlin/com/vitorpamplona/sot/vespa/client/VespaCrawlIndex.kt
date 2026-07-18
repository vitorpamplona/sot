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
package com.vitorpamplona.sot.vespa.client
import ai.vespa.feed.client.DocumentId
import ai.vespa.feed.client.FeedClient
import ai.vespa.feed.client.FeedClientBuilder
import ai.vespa.feed.client.OperationParameters
import com.vitorpamplona.quartz.nip01Core.core.HexKey
import com.vitorpamplona.sot.vespa.doc.CrawlDoc
import com.vitorpamplona.sot.vespa.doc.CrawlIndex
import kotlinx.coroutines.future.await
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.buildJsonObject
import kotlinx.serialization.json.intOrNull
import kotlinx.serialization.json.jsonArray
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
import kotlinx.serialization.json.put
import java.net.URI
import java.net.URLEncoder
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.time.Duration
import java.util.concurrent.Executors

/** The real [CrawlIndex]: the `crawl` document type over Vespa HTTP (same wiring as [VespaProfileIndex]). */
class VespaCrawlIndex(
    private val baseUrl: String = System.getenv("VESPA_URL") ?: "http://localhost:8080",
) : CrawlIndex {
    private val feed: FeedClient =
        FeedClientBuilder
            .create(URI.create(baseUrl))
            .setConnectionsPerEndpoint(8)
            .setMaxStreamPerConnection(256)
            .setRetryStrategy(
                object : FeedClient.RetryStrategy {
                    override fun retries() = 5
                },
            ).build()

    private val http =
        HttpClient
            .newBuilder()
            .connectTimeout(Duration.ofSeconds(5))
            .proxy(java.net.ProxySelector.of(null))
            .executor(Executors.newVirtualThreadPerTaskExecutor())
            .build()

    override suspend fun get(pubkey: HexKey): CrawlDoc? {
        val resp = send("$baseUrl/document/v1/$NAMESPACE/$DOCTYPE/docid/$pubkey")
        if (resp.statusCode() == 404) return null
        require(resp.statusCode() < 400) { "vespa crawl get ${resp.statusCode()}: ${resp.body().take(300)}" }
        val fields = Json.parseToJsonElement(resp.body()).jsonObject["fields"]?.jsonObject ?: return null
        return CrawlDoc.fromSummary(fields)
    }

    override suspend fun markSynced(
        authors: Collection<HexKey>,
        atSecs: Long,
    ) = stamp(authors, "content_synced_at", atSecs)

    override suspend fun markOutboxChecked(
        authors: Collection<HexKey>,
        atSecs: Long,
    ) = stamp(authors, "outbox_checked_at", atSecs)

    /**
     * Assign one long field for each author, creating the doc if absent. Two
     * distinct fields, so `content_synced_at` and `outbox_checked_at` never
     * overwrite each other. Pipelined; the feed client keeps per-document order.
     */
    private suspend fun stamp(
        authors: Collection<HexKey>,
        field: String,
        atSecs: Long,
    ) {
        authors
            .map { pk ->
                val fields =
                    buildJsonObject {
                        put("pubkey", buildJsonObject { put("assign", pk) })
                        put(field, buildJsonObject { put("assign", atSecs) })
                    }
                feed.update(
                    DocumentId.of(NAMESPACE, DOCTYPE, pk),
                    buildJsonObject { put("fields", fields) }.toString(),
                    feedParams().createIfNonExistent(true),
                )
            }.forEach { it.await() }
    }

    override suspend fun syncedSince(cutoffSecs: Long): Set<HexKey> = visitPubkeys("crawl.content_synced_at>=$cutoffSecs")

    override suspend fun outboxCheckedSet(): Set<HexKey> = visitPubkeys("crawl.outbox_checked_at>0")

    /** Visit every crawl doc matching [selection], collecting the pubkey field across all pages. */
    private suspend fun visitPubkeys(selection: String): Set<HexKey> {
        val sel = URLEncoder.encode(selection, "UTF-8")
        val fieldSet = URLEncoder.encode("$DOCTYPE:pubkey", "UTF-8")
        val base = "$baseUrl/document/v1/$NAMESPACE/$DOCTYPE/docid?selection=$sel&wantedDocumentCount=$VISIT_PAGE&fieldSet=$fieldSet"
        val out = HashSet<HexKey>()
        var continuation: String? = null
        while (true) {
            val resp = send(continuation?.let { "$base&continuation=$it" } ?: base)
            require(resp.statusCode() < 400) { "vespa crawl visit ${resp.statusCode()}: ${resp.body().take(300)}" }
            val json = Json.parseToJsonElement(resp.body()).jsonObject
            json["documents"]?.jsonArray?.forEach { d ->
                d.jsonObject["fields"]
                    ?.jsonObject
                    ?.get("pubkey")
                    ?.jsonPrimitive
                    ?.content
                    ?.let(out::add)
            }
            continuation = json["continuation"]?.jsonPrimitive?.content ?: return out
        }
    }

    override suspend fun syncedCount(): Int {
        val body =
            buildJsonObject {
                put("yql", "select * from $DOCTYPE where content_synced_at > 0 limit 0 | all(output(count()))")
                put("hits", "0")
            }.toString()
        val req =
            HttpRequest
                .newBuilder(URI.create("$baseUrl/search/"))
                .timeout(Duration.ofSeconds(30))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(body))
                .build()
        val resp = http.sendAsync(req, HttpResponse.BodyHandlers.ofString()).await()
        require(resp.statusCode() < 400) { "vespa crawl count ${resp.statusCode()}: ${resp.body().take(300)}" }
        val root = Json.parseToJsonElement(resp.body()).jsonObject["root"]?.jsonObject ?: return 0
        return countIn(root) ?: 0
    }

    /** The first `count()` grouping output anywhere under this node (see VespaEventIndex.countIn). */
    private fun countIn(node: kotlinx.serialization.json.JsonElement): Int? =
        when (node) {
            is kotlinx.serialization.json.JsonObject -> {
                node["fields"]
                    ?.jsonObject
                    ?.get("count()")
                    ?.jsonPrimitive
                    ?.intOrNull
                    ?: node["children"]?.let { countIn(it) }
            }

            is kotlinx.serialization.json.JsonArray -> {
                node.firstNotNullOfOrNull { countIn(it) }
            }

            else -> {
                null
            }
        }

    private suspend fun send(url: String): HttpResponse<String> {
        val req =
            HttpRequest
                .newBuilder(URI.create(url))
                .timeout(Duration.ofSeconds(30))
                .GET()
                .build()
        return http.sendAsync(req, HttpResponse.BodyHandlers.ofString()).await()
    }

    override fun close() = feed.close(true)

    private companion object {
        const val NAMESPACE = "crawl"
        const val DOCTYPE = "crawl"
        const val VISIT_PAGE = 1000

        fun feedParams(): OperationParameters = OperationParameters.empty().timeout(Duration.ofSeconds(30))
    }
}
