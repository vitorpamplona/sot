package com.vitorpamplona.sot.http

import com.vitorpamplona.sot.query.SearchHit
import kotlinx.serialization.Serializable

/**
 * The `GET /search` response model and its mapping from the query-engine's
 * [SearchHit]. This is the wire contract the web UI and other clients depend on.
 */

/** One profile in a [SearchResponse]. */
@Serializable
data class Result(
    val pubkey: String,
    val relevance: Double? = null, // text-match score
    val trust: Double? = null, // observer's web-of-trust score for this profile
    val name: String = "",
    val displayName: String = "",
    val about: String = "",
    val nip05: String = "",
    val picture: String = "",
)

/** The full `GET /search` JSON response. */
@Serializable
data class SearchResponse(val query: String, val numResults: Int, val results: List<Result>)

/** Project a query-engine [SearchHit] into an API [Result]. */
internal fun SearchHit.toResult() =
    Result(
        pubkey = pubkey,
        relevance = relevance,
        trust = trust,
        name = fields["name"].orEmpty(),
        displayName = fields["display_name"].orEmpty(),
        about = fields["about"].orEmpty(),
        nip05 = fields["nip05"].orEmpty(),
        picture = fields["picture"].orEmpty(),
    )
