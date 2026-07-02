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
package com.vitorpamplona.sot.http

import com.vitorpamplona.sot.vespa.SearchHit
import kotlinx.serialization.Serializable

/*
 * The `GET /search` response model and its mapping from the search core's
 * SearchHit. This is the wire contract the web UI and other clients depend on.
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
data class SearchResponse(
    val query: String,
    val numResults: Int,
    val results: List<Result>,
)

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
