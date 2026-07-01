package com.vitorpamplona.sot.http

import com.vitorpamplona.quartz.nip19Bech32.Nip19Parser
import com.vitorpamplona.quartz.nip19Bech32.entities.NPub
import com.vitorpamplona.quartz.utils.Hex
import com.vitorpamplona.sot.query.SearchHit
import com.vitorpamplona.sot.query.SearchOptions
import com.vitorpamplona.sot.query.VespaSearch
import io.ktor.server.response.respond
import io.ktor.server.routing.Route
import io.ktor.server.routing.get
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext
import kotlinx.serialization.Serializable

/**
 * The JSON search API surface — `GET /search`. A library route that the :server
 * composition root mounts on the shared Ktor app. Resolves the observer + query
 * and delegates all search logic to [VespaSearch] in :query-engine.
 */
private val CONTROL = Regex("[\\x00-\\x1f\\x7f]")
private const val RESULTS_LIMIT = 400

@Serializable
data class Result(
    val pubkey: String,
    val relevance: Double? = null,
    val trust: Double? = null,
    val name: String = "",
    val displayName: String = "",
    val about: String = "",
    val nip05: String = "",
    val picture: String = "",
)

@Serializable
data class SearchResponse(val query: String, val numResults: Int, val results: List<Result>)

private fun SearchHit.toResult() =
    Result(
        pubkey = pubkey,
        relevance = relevance,
        trust = trust,
        name = fields["name"] ?: "",
        displayName = fields["display_name"] ?: "",
        about = fields["about"] ?: "",
        nip05 = fields["nip05"] ?: "",
        picture = fields["picture"] ?: "",
    )

/** hex pubkey, npub1..., or null. */
private fun resolvePubkey(text: String): String? {
    if (Hex.isHex64(text)) return text.lowercase()
    if (text.startsWith("npub1")) return (Nip19Parser.uriToRoute(text)?.entity as? NPub)?.hex
    return null
}

/** Mount `GET /search`. [defaultObserver] ranks the results when no `observer` is passed. */
fun Route.searchApi(vespa: VespaSearch, defaultObserver: String) {
    get("/search") {
        val text = (call.request.queryParameters["text"] ?: "").let { CONTROL.replace(it, "").trim() }
        if (text.isEmpty()) {
            call.respond(SearchResponse(text, 0, emptyList()))
            return@get
        }
        val observer = call.request.queryParameters["observer"]?.let { resolvePubkey(it) } ?: defaultObserver
        val maxHits = (call.request.queryParameters["maxHits"]?.toIntOrNull() ?: 100).coerceIn(1, RESULTS_LIMIT)
        val onlyRanked = call.request.queryParameters["onlyRanked"]?.toBoolean() ?: true
        // A hex/npub query is a direct doc lookup; free text is a ranked search.
        val queryPk = resolvePubkey(text)

        val hits =
            withContext(Dispatchers.IO) {
                if (queryPk != null) {
                    listOfNotNull(vespa.getDocument(queryPk))
                } else {
                    vespa.search(text, observer, SearchOptions(hits = maxHits, includeZeroScore = !onlyRanked))
                }
            }
        call.respond(SearchResponse(text, hits.size, hits.map { it.toResult() }))
    }
}
