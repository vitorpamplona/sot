package com.vitorpamplona.vespasearch.query

import java.net.URI
import java.net.URLEncoder
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.time.Duration
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.contentOrNull
import kotlinx.serialization.json.doubleOrNull
import kotlinx.serialization.json.jsonArray
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive

/** One ranked search result. [userScore] is the observer's trust score for this doc. */
data class SearchHit(
    val pubkey: String,
    val relevance: Double?,
    val userScore: Double?,
    val fields: Map<String, String>,
) {
    val name: String get() = fields["name"] ?: ""
    val displayName: String get() = fields["display_name"] ?: ""
}

data class SearchOptions(
    val hits: Int = 50,
    val rankProfile: String = "name_and_quality_score_only",
    val includeZeroScore: Boolean = true,
    /** Extra `ranking.features.query(...)` overrides, e.g. {"w_gram" to 15.0}. */
    val features: Map<String, Double> = emptyMap(),
)

/**
 * The read side of the search core: builds the query (YQL + ranking features)
 * and calls Vespa's `/search/`, ranking by [observer]'s web-of-trust. Ported
 * from the upstream Python `vespa.py` `search()`; shared by http-api, the
 * search relay, and the CLI.
 *
 * Blocking (JDK HttpClient) — call it off a worker/IO dispatcher from coroutines.
 */
class VespaSearch(
    private val baseUrl: String = System.getenv("VESPA_URL") ?: "http://localhost:8080",
    private val http: HttpClient = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5)).build(),
) {
    fun search(queryText: String, observer: String, opts: SearchOptions = SearchOptions()): List<SearchHit> {
        val words = queryText.trim().split(Regex("\\s+")).filter { it.isNotEmpty() }.take(MAX_QUERY_WORDS)
        if (words.isEmpty()) return emptyList()
        val joined = if (words.size >= 2) words.joinToString("") else null
        val shortest = words.minOf { it.length }
        val wGram = if (shortest <= 3) 20.0 else 5.0

        var vespaHits = maxOf(opts.hits, 20)
        if (!opts.includeZeroScore) vespaHits = maxOf(opts.hits * 5, 100)
        vespaHits = minOf(vespaHits, 400) // Vespa default max-hits

        val params = LinkedHashMap<String, String>()
        params["yql"] = Yql.build(words, joined)
        params["ranking"] = opts.rankProfile
        params["ranking.features.query(user_q)"] = "{$observer:1.0}"
        params["ranking.features.query(w_gram)"] = wGram.toString()
        params["ranking.features.query(w_about)"] = "0.5"
        params["hits"] = vespaHits.toString()
        opts.features.forEach { (k, v) -> params["ranking.features.query($k)"] = v.toString() }
        words.forEachIndexed { i, w -> params["w$i"] = w }
        if (joined != null) params["wj"] = joined

        val root = get(params)?.get("root")?.jsonObject ?: return emptyList()
        val children = root["children"]?.jsonArray ?: return emptyList()

        val out = ArrayList<SearchHit>()
        for (h in children) {
            val ho = h.jsonObject
            val fieldsObj = ho["fields"]?.jsonObject ?: continue
            val mf = fieldsObj["matchfeatures"]?.jsonObject
            val userScore = mf?.get("user_score")?.jsonPrimitive?.doubleOrNull
            if (!opts.includeZeroScore && (userScore ?: 0.0) <= 0.0) continue
            val fields =
                fieldsObj.filterKeys { it != "matchfeatures" }
                    .mapValues { (_, v) -> v.jsonPrimitive.contentOrNull ?: "" }
            out.add(
                SearchHit(
                    pubkey = fields["pubkey"] ?: "",
                    relevance = ho["relevance"]?.jsonPrimitive?.doubleOrNull ?: 0.0,
                    userScore = userScore,
                    fields = fields,
                ),
            )
            if (out.size >= opts.hits) break
        }
        return out
    }

    /** Direct doc lookup by pubkey (hex), bypassing text search. Null if absent. */
    fun getDocument(pubkey: String): SearchHit? {
        val req =
            HttpRequest.newBuilder(URI.create("$baseUrl/document/v1/doc/doc/docid/$pubkey"))
                .timeout(Duration.ofSeconds(30)).GET().build()
        val resp = http.send(req, HttpResponse.BodyHandlers.ofString())
        if (resp.statusCode() == 404) return null
        if (resp.statusCode() >= 400) throw RuntimeException("vespa ${resp.statusCode()}: ${resp.body().take(300)}")
        val fieldsObj = Json.parseToJsonElement(resp.body()).jsonObject["fields"]?.jsonObject ?: return null
        // quality_scores is a tensor object (not a primitive) — drop it from the string map.
        val fields =
            fieldsObj.filterKeys { it != "quality_scores" }
                .mapValues { (_, v) -> v.jsonPrimitive.contentOrNull ?: "" }
        return SearchHit(pubkey = fields["pubkey"] ?: pubkey, relevance = null, userScore = null, fields = fields)
    }

    private fun get(params: Map<String, String>) =
        runCatching {
            val qs = params.entries.joinToString("&") { enc(it.key) + "=" + enc(it.value) }
            val req =
                HttpRequest.newBuilder(URI.create("$baseUrl/search/?$qs"))
                    .timeout(Duration.ofSeconds(30)).GET().build()
            val resp = http.send(req, HttpResponse.BodyHandlers.ofString())
            if (resp.statusCode() >= 400) throw RuntimeException("vespa ${resp.statusCode()}: ${resp.body().take(300)}")
            Json.parseToJsonElement(resp.body()).jsonObject
        }.getOrThrow()

    private fun enc(s: String) = URLEncoder.encode(s, "UTF-8")
}
