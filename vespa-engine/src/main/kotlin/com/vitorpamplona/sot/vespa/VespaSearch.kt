package com.vitorpamplona.sot.vespa

import java.net.URI
import java.net.URLEncoder
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.time.Duration
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.contentOrNull
import kotlinx.serialization.json.doubleOrNull
import kotlinx.serialization.json.jsonArray
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive

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
 * Blocking (JDK HttpClient) — call it off a worker/IO dispatcher from coroutines.
 */
class VespaSearch(
    private val baseUrl: String,
    private val http: HttpClient = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5)).build(),
) {
    fun search(queryText: String, observer: String, opts: SearchOptions = SearchOptions()): List<SearchHit> {
        val words = queryText.trim().split(WHITESPACE).filter { it.isNotEmpty() }.take(MAX_QUERY_WORDS)
        if (words.isEmpty()) return emptyList()
        val joined = if (words.size >= 2) words.joinToString("") else null

        val children =
            getJson("$baseUrl/search/?" + encode(searchParams(words, joined, observer, opts)))["root"]
                ?.jsonObject?.get("children")?.jsonArray ?: return emptyList()

        return children.asSequence()
            .mapNotNull { toHit(it.jsonObject, opts) }
            .take(opts.hits)
            .toList()
    }

    /** Direct doc lookup by pubkey (hex), bypassing text search. Null if absent. */
    fun getDocument(pubkey: String): SearchHit? {
        val resp = send("$baseUrl/document/v1/doc/doc/docid/$pubkey")
        if (resp.statusCode() == 404) return null
        // quality_scores is a tensor object (not a primitive) — drop it from the string map.
        val fields = body(resp)["fields"]?.jsonObject?.let { stringFields(it, drop = "quality_scores") } ?: return null
        return SearchHit(pubkey = fields["pubkey"] ?: pubkey, relevance = null, trust = null, fields = fields)
    }

    /** The Vespa `/search/` query parameters: recall (YQL) + the observer-weighted ranking inputs. */
    private fun searchParams(words: List<String>, joined: String?, observer: String, opts: SearchOptions): Map<String, String> {
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
    private fun toHit(node: JsonObject, opts: SearchOptions): SearchHit? {
        val fieldsObj = node["fields"]?.jsonObject ?: return null
        val trust = fieldsObj["matchfeatures"]?.jsonObject?.get("user_score")?.jsonPrimitive?.doubleOrNull
        if (!opts.includeZeroScore && (trust ?: 0.0) <= 0.0) return null
        val fields = stringFields(fieldsObj, drop = "matchfeatures")
        return SearchHit(
            pubkey = fields["pubkey"] ?: "",
            relevance = node["relevance"]?.jsonPrimitive?.doubleOrNull ?: 0.0,
            trust = trust,
            fields = fields,
        )
    }

    private fun getJson(url: String): JsonObject = body(send(url))

    private fun send(url: String): HttpResponse<String> =
        http.send(
            HttpRequest.newBuilder(URI.create(url)).timeout(Duration.ofSeconds(30)).GET().build(),
            HttpResponse.BodyHandlers.ofString(),
        )

    private fun body(resp: HttpResponse<String>): JsonObject {
        if (resp.statusCode() >= 400) throw RuntimeException("vespa ${resp.statusCode()}: ${resp.body().take(300)}")
        return Json.parseToJsonElement(resp.body()).jsonObject
    }

    private fun encode(params: Map<String, String>): String =
        params.entries.joinToString("&") { enc(it.key) + "=" + enc(it.value) }

    private fun enc(s: String) = URLEncoder.encode(s, "UTF-8")

    /** Vespa `fields` object -> String map, dropping [drop] (a non-primitive like a tensor). */
    private fun stringFields(fields: JsonObject, drop: String): Map<String, String> =
        fields.filterKeys { it != drop }.mapValues { (_, v) -> v.jsonPrimitive.contentOrNull ?: "" }

    private companion object {
        val WHITESPACE = Regex("\\s+")
        const val MAX_VESPA_HITS = 400 // Vespa default max-hits
    }
}
