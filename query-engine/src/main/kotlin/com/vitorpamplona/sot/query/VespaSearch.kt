package com.vitorpamplona.sot.query

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
    private val baseUrl: String = com.vitorpamplona.sot.config.Config.vespaUrl,
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
        words.forEachIndexed { i, w -> params["w$i"] = w }
        if (joined != null) params["wj"] = joined

        val root = get(params)?.get("root")?.jsonObject ?: return emptyList()
        val children = root["children"]?.jsonArray ?: return emptyList()

        val out = ArrayList<SearchHit>()
        for (h in children) {
            val ho = h.jsonObject
            val fieldsObj = ho["fields"]?.jsonObject ?: continue
            val mf = fieldsObj["matchfeatures"]?.jsonObject
            val trust = mf?.get("user_score")?.jsonPrimitive?.doubleOrNull
            if (!opts.includeZeroScore && (trust ?: 0.0) <= 0.0) continue
            val fields = stringFields(fieldsObj, drop = "matchfeatures")
            out.add(
                SearchHit(
                    pubkey = fields["pubkey"] ?: "",
                    relevance = ho["relevance"]?.jsonPrimitive?.doubleOrNull ?: 0.0,
                    trust = trust,
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
        val fields = stringFields(fieldsObj, drop = "quality_scores")
        return SearchHit(pubkey = fields["pubkey"] ?: pubkey, relevance = null, trust = null, fields = fields)
    }

    private fun get(params: Map<String, String>): JsonObject {
        val qs = params.entries.joinToString("&") { enc(it.key) + "=" + enc(it.value) }
        val req =
            HttpRequest.newBuilder(URI.create("$baseUrl/search/?$qs"))
                .timeout(Duration.ofSeconds(30)).GET().build()
        val resp = http.send(req, HttpResponse.BodyHandlers.ofString())
        if (resp.statusCode() >= 400) throw RuntimeException("vespa ${resp.statusCode()}: ${resp.body().take(300)}")
        return Json.parseToJsonElement(resp.body()).jsonObject
    }

    private fun enc(s: String) = URLEncoder.encode(s, "UTF-8")

    /** Vespa `fields` object -> String map, dropping [drop] (a non-primitive like a tensor). */
    private fun stringFields(fields: JsonObject, drop: String): Map<String, String> =
        fields.filterKeys { it != drop }.mapValues { (_, v) -> v.jsonPrimitive.contentOrNull ?: "" }
}
