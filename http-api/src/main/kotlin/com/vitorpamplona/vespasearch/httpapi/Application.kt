package com.vitorpamplona.vespasearch.httpapi

import com.vitorpamplona.quartz.nip19Bech32.Nip19Parser
import com.vitorpamplona.quartz.nip19Bech32.entities.NPub
import com.vitorpamplona.quartz.utils.Hex
import com.vitorpamplona.vespasearch.query.SearchHit
import com.vitorpamplona.vespasearch.query.SearchOptions
import com.vitorpamplona.vespasearch.query.VespaSearch
import io.ktor.serialization.kotlinx.json.json
import io.ktor.server.application.install
import io.ktor.server.engine.embeddedServer
import io.ktor.server.netty.Netty
import io.ktor.server.plugins.contentnegotiation.ContentNegotiation
import io.ktor.server.request.uri
import io.ktor.server.response.respond
import io.ktor.server.routing.get
import io.ktor.server.routing.routing
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext
import kotlinx.serialization.Serializable

/**
 * HTTP search API — the Kotlin/Ktor replacement for brainstorm_server's
 * `/byText` router. Thin: it resolves the observer + query and delegates all
 * search logic to [VespaSearch] in :common-query. Search improvements happen in
 * common-query (and vespa-app/doc.sd), shared with the relay and CLI.
 */
private val CONTROL = Regex("[\\x00-\\x1f\\x7f]")

// Default observer when the caller doesn't pass one (matches the upstream default).
private val DEFAULT_OBSERVER =
    System.getenv("PERIODIC_GRAPERANK_PUBKEY")
        ?: "be7bf5de068c1d842ed34a7c270507ec940f5ea51671cfd062a95e9d09420d0a"

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
    val port = (System.getenv("HTTP_API_PORT") ?: "8081").toInt()

    embeddedServer(Netty, port = port) {
        install(ContentNegotiation) { json() }
        routing {
            get("/search/byText") {
                val text = (call.request.queryParameters["text"] ?: "").let { CONTROL.replace(it, "").trim() }
                if (text.isEmpty()) {
                    call.respond(SearchResponse(text, 0, emptyList()))
                    return@get
                }
                val observer = call.request.queryParameters["observer"]?.let { resolvePubkey(it) } ?: DEFAULT_OBSERVER
                val maxHits = (call.request.queryParameters["maxHits"]?.toIntOrNull() ?: 100).coerceIn(1, RESULTS_LIMIT)
                val onlyRanked = call.request.queryParameters["onlyRanked"]?.toBoolean() ?: true

                val hits =
                    withContext(Dispatchers.IO) {
                        // A hex/npub query is a direct doc lookup; free text is a ranked search.
                        val pk = resolvePubkey(text)
                        if (pk != null) {
                            listOfNotNull(vespa.getDocument(pk))
                        } else {
                            vespa.search(text, observer, SearchOptions(hits = maxHits, includeZeroScore = !onlyRanked))
                        }
                    }
                call.respond(SearchResponse(text, hits.size, hits.map { it.toResult() }))
            }
        }
    }.start(wait = true)
}
