package com.vitorpamplona.sot.http

import com.vitorpamplona.sot.query.SearchHit
import com.vitorpamplona.sot.query.SearchOptions
import com.vitorpamplona.sot.query.VespaSearch
import io.ktor.server.response.respond
import io.ktor.server.routing.Route
import io.ktor.server.routing.get
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext

/**
 * The JSON search API surface — `GET /search`. A library route the :server
 * composition root mounts on the shared Ktor app. This file is just the route:
 * pubkey parsing lives in Pubkey.kt, the response model in SearchResponse.kt, and
 * all ranking is delegated to [VespaSearch] in :query-engine.
 *
 * Params: `text` (required), `observer` (hex/npub/nprofile/nip05, defaults to
 * [defaultObserver]), `maxHits` (1..[MAX_HITS]), `onlyRanked` (drop zero-trust
 * results, default true).
 */

// Strip ASCII control chars (C0 range 0x00-0x1f and DEL 0x7f) from the query so
// they can't leak into the Vespa YQL or log lines.
private val CONTROL_CHARS = Regex("[\\x00-\\x1f\\x7f]")
private const val DEFAULT_HITS = 100
private const val MAX_HITS = 400

/** Mount `GET /search`. [defaultObserver] ranks results when no `observer` is passed. */
fun Route.searchApi(vespa: VespaSearch, defaultObserver: String) {
    get("/search") {
        val params = call.request.queryParameters
        val text = CONTROL_CHARS.replace(params["text"].orEmpty(), "").trim()
        if (text.isEmpty()) {
            call.respond(SearchResponse(text, 0, emptyList()))
            return@get
        }
        val maxHits = (params["maxHits"]?.toIntOrNull() ?: DEFAULT_HITS).coerceIn(1, MAX_HITS)
        val onlyRanked = params["onlyRanked"]?.toBoolean() ?: true

        val hits =
            withContext(Dispatchers.IO) {
                // Resolve identifiers off the event loop (NIP-05 may hit the network).
                val observer = params["observer"]?.let { resolvePubkey(it) } ?: defaultObserver
                // A pubkey-shaped query (hex/npub/nprofile/nip05) is a direct doc
                // lookup; free text is a ranked search.
                resolvePubkey(text)?.let { listOfNotNull(vespa.getDocument(it)) }
                    ?: vespa.search(text, observer, SearchOptions(hits = maxHits, includeZeroScore = !onlyRanked))
            }
        call.respond(SearchResponse(text, hits.size, hits.map(SearchHit::toResult)))
    }
}
