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
import com.vitorpamplona.sot.vespa.doc.EventDoc
import com.vitorpamplona.sot.vespa.query.EventQuery
import com.vitorpamplona.sot.vespa.query.EventSelection
import com.vitorpamplona.sot.vespa.query.EventYql
import com.vitorpamplona.sot.vespa.query.VespaQuery
import kotlinx.coroutines.delay
import kotlinx.coroutines.future.await
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonArray
import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.buildJsonObject
import kotlinx.serialization.json.intOrNull
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
 * AWAITED before returning. The store's read-your-writes contract needs the
 * ack, and proton makes an acked write visible to search. Reads use the plain
 * document API (get) and `/search/` (query), non-blocking via the JDK client's
 * async sends on virtual threads.
 *
 * Unlimited queries are capped at [maxHits] (the app package's query profile
 * must allow it). A full-corpus walk goes through [visitIds] instead: the
 * document API's visit, which streams past any cap.
 *
 * Counts use a grouping `count()` over the full match set (see
 * [EventYql.buildCount]) — NOT `root.totalCount`, which the recency `order by`'s
 * match-phase caps on a large corpus (a 10x+ undercount). Exact for
 * attribute-only recall; still approximate under a weakAnd search term, the same
 * caveat Vespa itself carries.
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
            // ramps up by probing throughput. A batched writer never sustains
            // that probe (putAll bursts with query gaps between chunks), so it
            // idles at the floor: ~20 in flight x ~20ms/write is about 1k/s,
            // against an engine measured at twice that. Start the window high
            // instead. The throttler still adapts DOWN if the engine pushes
            // back. This is an implementation-only knob the API interface
            // doesn't expose, so apply it reflectively and ignore it if a
            // future client version renames it.
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

    private fun putOp(doc: EventDoc) =
        feed.put(
            DocumentId.of(NAMESPACE, DOCTYPE, doc.id),
            buildJsonObject { put("fields", doc.indexFields()) }.toString(),
            feedParams(),
        )

    private fun removeOp(id: String) = feed.remove(DocumentId.of(NAMESPACE, DOCTYPE, id), feedParams())

    override suspend fun put(doc: EventDoc) {
        putOp(doc).await()
    }

    /** All puts stay in flight together — the feed client multiplexes them over HTTP/2. */
    override suspend fun putAll(docs: List<EventDoc>) {
        docs.map { putOp(it) }.forEach { it.await() }
    }

    override suspend fun remove(id: String) {
        removeOp(id).await()
    }

    /** All removes in flight together over HTTP/2, like [putAll]. */
    override suspend fun removeAll(ids: List<String>) {
        ids.map { removeOp(it) }.forEach { it.await() }
    }

    override suspend fun search(query: EventQuery): List<EventDoc> {
        val vq = EventYql.build(query) ?: return emptyList()
        val root = queryRoot(vq, hits = query.limit ?: maxHits) ?: return emptyList()
        return root["children"]
            ?.jsonArray
            ?.mapNotNull { child -> child.jsonObject["fields"]?.jsonObject?.let(::summaryOrNull) }
            ?: emptyList()
    }

    /**
     * The document-API visit: a streaming scan with a selection expression and
     * continuation tokens. It has no result cap and no ranking, which is
     * exactly what a full-corpus id walk needs. Queries a selection can't
     * express fall back to the (capped) search default.
     */
    override suspend fun visitIds(
        query: EventQuery,
        withDTag: Boolean,
        onPage: suspend (List<DocRef>) -> Boolean,
    ) {
        val selection = EventSelection.build(query) ?: return super.visitIds(query, withDTag, onPage)
        // Vespa fieldSet syntax is "<doctype>:<field>,<field>,…" — the doctype
        // prefixes the list ONCE, not each field (else: ILLEGAL_PARAMETERS).
        val fieldSet = "$DOCTYPE:created_at" + if (withDTag) ",tag_index" else ""
        val base =
            "$baseUrl/document/v1/$NAMESPACE/$DOCTYPE/docid" +
                "?selection=${URLEncoder.encode(selection, "UTF-8")}" +
                "&wantedDocumentCount=$VISIT_PAGE&fieldSet=${URLEncoder.encode(fieldSet, "UTF-8")}"
        var continuation: String? = null
        while (true) {
            val resp = send(continuation?.let { "$base&continuation=$it" } ?: base)
            require(resp.statusCode() < 400) { "vespa visit ${resp.statusCode()}: ${resp.body().take(300)}" }
            val json = Json.parseToJsonElement(resp.body()).jsonObject
            val page =
                json["documents"]?.jsonArray?.mapNotNull { d ->
                    val obj = d.jsonObject
                    val id = obj["id"]?.jsonPrimitive?.content?.substringAfterLast(":") ?: return@mapNotNull null
                    val fields = obj["fields"]?.jsonObject
                    val at = fields?.get("created_at")?.jsonPrimitive?.long ?: return@mapNotNull null
                    val dTag =
                        if (withDTag) {
                            fields["tag_index"]
                                ?.jsonArray
                                ?.firstNotNullOfOrNull { t ->
                                    t.jsonPrimitive.content
                                        .takeIf { it.startsWith("d:") }
                                        ?.substring(2)
                                }
                        } else {
                            null
                        }
                    DocRef(id, at, dTag)
                } ?: emptyList()
            if (page.isNotEmpty() && !onPage(page)) return
            continuation = json["continuation"]?.jsonPrimitive?.content ?: return
        }
    }

    override suspend fun count(query: EventQuery): Int {
        // A grouping count() over the full match set — NOT root.totalCount, which
        // Vespa caps under the recency `order by`'s match-phase (a 10x+ undercount
        // on large kinds). See [EventYql.buildCount].
        val root = queryRoot(EventYql.buildCount(query) ?: return 0, hits = 0) ?: return 0
        return countIn(root) ?: 0
    }

    override suspend fun countDistinctAuthors(query: EventQuery): Int {
        // `all(group(pubkey) output(count()))` counts the GROUPS — i.e. the
        // distinct pubkeys — not the docs. The count() lands one level deeper
        // than [count]'s (inside the group list), so both share [countIn]'s
        // recursive scan. See [EventYql.buildDistinctCount].
        val root = queryRoot(EventYql.buildDistinctCount(query, "pubkey") ?: return 0, hits = 0) ?: return 0
        return countIn(root) ?: 0
    }

    override suspend fun countByKind(query: EventQuery): Map<Int, Int> {
        // `all(group(kind) each(output(count())))` yields one leaf group per kind,
        // each carrying its `value` (the kind) and a `count()`. See [EventYql.buildKindHistogram].
        val root = queryRoot(EventYql.buildKindHistogram(query) ?: return emptyMap(), hits = 0) ?: return emptyMap()
        val out = LinkedHashMap<Int, Int>()
        kindCountsInto(root, out)
        return out
    }

    /** Collect every leaf group's (value -> count()) pair anywhere under this node. */
    private fun kindCountsInto(
        node: JsonElement,
        out: MutableMap<Int, Int>,
    ) {
        when (node) {
            is JsonObject -> {
                val value = node["value"]?.jsonPrimitive?.intOrNull
                val count =
                    node["fields"]
                        ?.jsonObject
                        ?.get("count()")
                        ?.jsonPrimitive
                        ?.intOrNull
                if (value != null && count != null) out[value] = count
                node["children"]?.let { kindCountsInto(it, out) }
            }

            is JsonArray -> {
                node.forEach { kindCountsInto(it, out) }
            }

            else -> {}
        }
    }

    /** The first `count()` grouping output anywhere under this node — flat for [count], nested under the group list for [countDistinctAuthors]. */
    private fun countIn(node: JsonElement): Int? =
        when (node) {
            is JsonObject -> {
                node["fields"]
                    ?.jsonObject
                    ?.get("count()")
                    ?.jsonPrimitive
                    ?.intOrNull
                    ?: node["children"]?.let { countIn(it) }
            }

            is JsonArray -> {
                node.firstNotNullOfOrNull { countIn(it) }
            }

            else -> {
                null
            }
        }

    override suspend fun distinctAuthors(query: EventQuery): Set<String> {
        val root = queryRoot(EventYql.buildDistinctAuthors(query) ?: return emptySet(), hits = 0) ?: return emptySet()
        // group(pubkey) nests a grouplist whose leaf `group:` nodes each carry a
        // pubkey as `value`. Walk the tree and collect every group value.
        val authors = LinkedHashSet<String>()

        fun collect(node: JsonObject) {
            (node["value"] as? JsonPrimitive)?.let { if (node["id"]?.jsonPrimitive?.content?.startsWith("group:") == true) authors += it.content }
            node["children"]?.jsonArray?.forEach { collect(it.jsonObject) }
        }
        collect(root)
        return authors
    }

    /**
     * Run [vq] against `/search/`. It POSTs because a filter with hundreds of
     * ids or authors builds YQL far past any sane URL length.
     */
    private suspend fun queryRoot(
        vq: VespaQuery,
        hits: Int,
    ): JsonObject? {
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
        // A busy engine sheds load transiently (504 "Summary data is
        // incomplete" under heavy concurrent summary fills). One failed page
        // must not kill a whole multi-hour sync, so 5xx gets brief retries.
        val resp = sendRetrying(req)
        require(resp.statusCode() < 400) { "vespa search ${resp.statusCode()}: ${resp.body().take(300)}" }
        return Json.parseToJsonElement(resp.body()).jsonObject["root"]?.jsonObject
    }

    /** Grouping/meta children have no event fields; skip anything that doesn't parse as a doc. */
    private fun summaryOrNull(fields: JsonObject): EventDoc? = runCatching { EventDoc.fromSummary(fields) }.getOrNull()

    private suspend fun send(url: String): HttpResponse<String> =
        sendRetrying(
            HttpRequest
                .newBuilder(URI.create(url))
                .timeout(Duration.ofSeconds(30))
                .GET()
                .build(),
        )

    /**
     * Send [req], briefly retrying transient overload: 5xx (the engine sheds
     * load under heavy concurrent summary fills) AND 429 (the document API
     * rejects past 256 enqueued requests — pushback, not failure). Shared by the
     * query, get, and visit paths. The full-corpus visit walk is exactly a place
     * where one 504/429 page must not abort the whole scan.
     */
    private suspend fun sendRetrying(req: HttpRequest): HttpResponse<String> {
        var resp = http.sendAsync(req, HttpResponse.BodyHandlers.ofString()).await()
        var attempt = 0
        while ((resp.statusCode() in 500..599 || resp.statusCode() == 429) && attempt++ < QUERY_RETRIES) {
            delay(500L * attempt)
            resp = http.sendAsync(req, HttpResponse.BodyHandlers.ofString()).await()
        }
        return resp
    }

    /**
     * One-line feed-client health for status lines: cumulative acks, the LIVE
     * in-flight window, and per-request HTTP latency. Together these tell "the
     * engine is slow" apart from "the client isn't pushing" at a glance. A
     * starved window shows tiny inflight at low latency; a saturated engine
     * shows a big window at high latency.
     */
    fun feedGauge(): String {
        val s = feed.stats()
        // Non-2xx responses get retried and usually succeed: pushback, not
        // loss (a big window ramping down shows a burst of 429s here). Only
        // transport exceptions are worth shouting about.
        val retried = s.responses() - s.successes()
        return "feed ok ${s.successes()} inflight ${s.inflight()} lat ${s.averageLatencyMillis()}ms" +
            (if (retried > 0) " retry $retried" else "") +
            if (s.exceptions() > 0) " EXC ${s.exceptions()}" else ""
    }

    /** Graceful: waits for in-flight feed operations before closing the connections. */
    override fun close() = feed.close(true)

    private companion object {
        const val NAMESPACE = "event"
        const val DOCTYPE = "event"

        /** Docs asked for per visit response (Vespa's per-request ceiling is 1024). */
        const val VISIT_PAGE = 1024

        /** Brief 5xx retries per query (transient engine load-shedding, not correctness). */
        const val QUERY_RETRIES = 3

        /**
         * Per-operation feed deadline. The feed client's retry strategy handles
         * transient errors, but a silently half-dead HTTP/2 connection (for
         * example, one severed by an engine restart) makes `await()` hang
         * FOREVER with no deadline, which deadlocks the single-writer store
         * behind it. A timeout turns that hang into a retryable failure.
         */
        fun feedParams(): OperationParameters = OperationParameters.empty().timeout(Duration.ofSeconds(30))
    }
}
