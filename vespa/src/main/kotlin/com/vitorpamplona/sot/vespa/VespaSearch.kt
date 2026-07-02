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

import kotlinx.coroutines.future.await
import kotlinx.coroutines.sync.Semaphore
import kotlinx.coroutines.sync.withPermit
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.contentOrNull
import kotlinx.serialization.json.doubleOrNull
import kotlinx.serialization.json.jsonArray
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
import java.net.URI
import java.net.URLEncoder
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.time.Duration
import java.util.concurrent.Executors

/** One ranked search result. [trust] is the observer's trust score for this doc. */
data class SearchHit(
    val pubkey: String,
    val relevance: Double?,
    val trust: Double?,
    val fields: Map<String, String>,
) {
    val name: String get() = fields["name"] ?: ""
    val displayName: String get() = fields["display_name"] ?: ""
}

data class SearchOptions(
    val hits: Int = 50,
    val rankProfile: String = "name_and_quality_score_only",
    val includeZeroScore: Boolean = true,
)

/**
 * The read side of the search core: builds the query (YQL + ranking features)
 * and calls Vespa's `/search/`, ranking by [observer]'s web-of-trust. Ported
 * from the upstream Python `vespa.py` `search()`; shared by the http service, the
 * search relay, and the CLI.
 *
 * Non-blocking: every Vespa round-trip goes out via the JDK client's async
 * `sendAsync` and is `await`ed, so a suspended search holds no thread while it
 * waits on the network. Thousands of searches can therefore be in flight at once
 * without a thread each. To keep that fan-out from opening an unbounded number of
 * sockets to Vespa (the JDK client speaks HTTP/1.1 to `http://` — no multiplexing),
 * concurrent round-trips are capped by a coroutine [Semaphore]; excess searches
 * suspend cheaply on the permit rather than blocking a thread. Tune the ceiling
 * with [maxConcurrentQueries].
 *
 * The default client completes responses on virtual threads (JDK 21), so response
 * assembly scales with in-flight requests rather than a fixed worker pool.
 */
class VespaSearch(
    private val baseUrl: String,
    maxConcurrentQueries: Int = DEFAULT_MAX_CONCURRENCY,
    private val http: HttpClient =
        HttpClient
            .newBuilder()
            .connectTimeout(Duration.ofSeconds(5))
            .executor(Executors.newVirtualThreadPerTaskExecutor())
            .build(),
) {
    /** Bounds concurrent Vespa round-trips so a burst of searches can't exhaust Vespa's connections. */
    private val gate = Semaphore(maxConcurrentQueries)

    suspend fun search(
        queryText: String,
        observer: String,
        opts: SearchOptions = SearchOptions(),
    ): List<SearchHit> {
        val words =
            queryText
                .trim()
                .split(WHITESPACE)
                .filter { it.isNotEmpty() }
                .take(MAX_QUERY_WORDS)
        if (words.isEmpty()) return emptyList()
        val joined = if (words.size >= 2) words.joinToString("") else null

        val children =
            getJson("$baseUrl/search/?" + encode(searchParams(words, joined, observer, opts)))["root"]
                ?.jsonObject
                ?.get("children")
                ?.jsonArray ?: return emptyList()

        return children
            .asSequence()
            .mapNotNull { toHit(it.jsonObject, opts) }
            .take(opts.hits)
            .toList()
    }

    /** Direct doc lookup by pubkey (hex), bypassing text search. Null if absent. */
    suspend fun getDocument(pubkey: String): SearchHit? {
        val resp = send("$baseUrl/document/v1/doc/doc/docid/$pubkey")
        if (resp.statusCode() == 404) return null
        // quality_scores is a tensor object (not a primitive) — drop it from the string map.
        val fields = body(resp)["fields"]?.jsonObject?.let { stringFields(it, drop = "quality_scores") } ?: return null
        return SearchHit(pubkey = fields["pubkey"] ?: pubkey, relevance = null, trust = null, fields = fields)
    }

    /** The Vespa `/search/` query parameters: recall (YQL) + the observer-weighted ranking inputs. */
    private fun searchParams(
        words: List<String>,
        joined: String?,
        observer: String,
        opts: SearchOptions,
    ): Map<String, String> {
        val wGram = if (words.minOf { it.length } <= 3) 20.0 else 5.0
        val hits = (if (opts.includeZeroScore) maxOf(opts.hits, 20) else maxOf(opts.hits * 5, 100)).coerceAtMost(MAX_VESPA_HITS)
        return buildMap {
            put("yql", ProfileQuery.build(words, joined))
            put("ranking", opts.rankProfile)
            put("ranking.features.query(user_q)", "{$observer:1.0}")
            put("ranking.features.query(w_gram)", wGram.toString())
            put("ranking.features.query(w_about)", "0.5")
            put("hits", hits.toString())
            words.forEachIndexed { i, w -> put("w$i", w) }
            if (joined != null) put("wj", joined)
        }
    }

    /** One Vespa result child -> [SearchHit]; null if it has no fields or is a filtered zero-score hit. */
    private fun toHit(
        node: JsonObject,
        opts: SearchOptions,
    ): SearchHit? {
        val fieldsObj = node["fields"]?.jsonObject ?: return null
        val trust =
            fieldsObj["matchfeatures"]
                ?.jsonObject
                ?.get("user_score")
                ?.jsonPrimitive
                ?.doubleOrNull
        if (!opts.includeZeroScore && (trust ?: 0.0) <= 0.0) return null
        val fields = stringFields(fieldsObj, drop = "matchfeatures")
        return SearchHit(
            pubkey = fields["pubkey"] ?: "",
            relevance = node["relevance"]?.jsonPrimitive?.doubleOrNull ?: 0.0,
            trust = trust,
            fields = fields,
        )
    }

    private suspend fun getJson(url: String): JsonObject = body(send(url))

    /** One Vespa GET, non-blocking and permit-gated: suspends (no thread held) until the response arrives. */
    private suspend fun send(url: String): HttpResponse<String> =
        gate.withPermit {
            http
                .sendAsync(
                    HttpRequest
                        .newBuilder(URI.create(url))
                        .timeout(Duration.ofSeconds(30))
                        .GET()
                        .build(),
                    HttpResponse.BodyHandlers.ofString(),
                ).await()
        }

    private fun body(resp: HttpResponse<String>): JsonObject {
        if (resp.statusCode() >= 400) throw RuntimeException("vespa ${resp.statusCode()}: ${resp.body().take(300)}")
        return Json.parseToJsonElement(resp.body()).jsonObject
    }

    private fun encode(params: Map<String, String>): String = params.entries.joinToString("&") { enc(it.key) + "=" + enc(it.value) }

    private fun enc(s: String) = URLEncoder.encode(s, "UTF-8")

    /** Vespa `fields` object -> String map, dropping [drop] (a non-primitive like a tensor). */
    private fun stringFields(
        fields: JsonObject,
        drop: String,
    ): Map<String, String> = fields.filterKeys { it != drop }.mapValues { (_, v) -> v.jsonPrimitive.contentOrNull ?: "" }

    private companion object {
        val WHITESPACE = Regex("\\s+")
        const val MAX_VESPA_HITS = 400 // Vespa default max-hits

        // Ceiling on concurrent Vespa round-trips per client. High enough to serve
        // thousands of overlapping searches (they queue on the permit, not a thread),
        // low enough not to swamp Vespa's HTTP/1.1 connection budget.
        const val DEFAULT_MAX_CONCURRENCY = 256
    }
}
