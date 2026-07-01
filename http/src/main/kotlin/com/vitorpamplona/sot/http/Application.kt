package com.vitorpamplona.sot.http

import com.vitorpamplona.quartz.nip19Bech32.Nip19Parser
import com.vitorpamplona.quartz.nip19Bech32.entities.NPub
import com.vitorpamplona.quartz.utils.Hex
import com.vitorpamplona.sot.query.SearchHit
import com.vitorpamplona.sot.query.SearchOptions
import com.vitorpamplona.sot.query.VespaSearch
import io.ktor.serialization.kotlinx.json.json
import io.ktor.server.application.install
import io.ktor.server.engine.embeddedServer
import io.ktor.server.netty.Netty
import io.ktor.server.plugins.contentnegotiation.ContentNegotiation
import io.ktor.server.plugins.cors.routing.CORS
import io.ktor.http.HttpMethod
import io.ktor.server.request.uri
import io.ktor.server.response.respond
import io.ktor.server.routing.get
import io.ktor.server.routing.routing
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext
import kotlinx.serialization.Serializable

/**
 * HTTP search API exposing `GET /search`. Thin: it resolves the observer + query
 * and delegates all search logic to [VespaSearch] in :query-engine. Search
 * improvements happen in query-engine (and vespa/doc.sd), shared with the relay
 * and CLI.
 */
private val CONTROL = Regex("[\\x00-\\x1f\\x7f]")

// Default web-of-trust observer when the caller doesn't pass one, from the
// DEFAULT_OBSERVER env var. Empty means no default (every quality score is 0).
private val DEFAULT_OBSERVER = System.getenv("DEFAULT_OBSERVER").orEmpty()

private const val RESULTS_LIMIT = 400

@Serializable
data class Result(
    val pubkey: String,
    val relevance: Double? = null,
    val qualityScore: Double? = null,
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
        qualityScore = userScore,
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

fun main() {
    val vespa = VespaSearch()
    val port = (System.getenv("HTTP_PORT") ?: "8081").toInt()

    embeddedServer(Netty, port = port) {
        install(ContentNegotiation) { json() }
        // Allow the static web/ UI (any origin, incl. file:// -> "null") to call us.
        install(CORS) {
            anyHost()
            allowMethod(HttpMethod.Get)
        }
        routing {
            get("/search") {
                val text = (call.request.queryParameters["text"] ?: "").let { CONTROL.replace(it, "").trim() }
                if (text.isEmpty()) {
                    call.respond(SearchResponse(text, 0, emptyList()))
                    return@get
                }
                val observer = call.request.queryParameters["observer"]?.let { resolvePubkey(it) } ?: DEFAULT_OBSERVER
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
    }.start(wait = true)
}
