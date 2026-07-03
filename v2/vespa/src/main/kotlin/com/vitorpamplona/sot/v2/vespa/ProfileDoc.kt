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
package com.vitorpamplona.sot.v2.vespa

import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.buildJsonObject
import kotlinx.serialization.json.double
import kotlinx.serialization.json.int
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
import kotlinx.serialization.json.putJsonObject

/**
 * One pubkey's ranking state — the `profile` GLOBAL parent document every
 * event references (`author_ref`) and imports for trust-weighted ranking.
 * NOT an event: the trust projection derives it from stored kind-30382s and
 * rewrites it whole on every change (recompute, not cell surgery), so it is
 * rebuildable from the event corpus at any time.
 *
 * Tensor cells are keyed by OBSERVER pubkey (Brainstorm's shapes):
 * [qualityScores] = rank (influence*100, 0..100), [followerCounts] =
 * verified-follower count.
 */
data class ProfileDoc(
    val pubkey: String,
    val qualityScores: Map<String, Int> = emptyMap(),
    val followerCounts: Map<String, Double> = emptyMap(),
) {
    /** No cells at all — the projection removes the doc instead of storing it. */
    fun isEmpty(): Boolean = qualityScores.isEmpty() && followerCounts.isEmpty()

    /** The document's field map (mapped tensors in Vespa's short object form). */
    fun indexFields(): JsonObject =
        buildJsonObject {
            put("pubkey", JsonPrimitive(pubkey))
            putJsonObject("quality_scores") { qualityScores.forEach { (observer, rank) -> put(observer, JsonPrimitive(rank)) } }
            putJsonObject("follower_counts") { followerCounts.forEach { (observer, count) -> put(observer, JsonPrimitive(count)) } }
        }

    companion object {
        /** Parse a document-API `fields` object (the [indexFields] shape) back into a doc. */
        fun fromSummary(fields: JsonObject): ProfileDoc =
            ProfileDoc(
                pubkey = fields.getValue("pubkey").jsonPrimitive.content,
                qualityScores = fields["quality_scores"]?.jsonObject?.mapValues { it.value.jsonPrimitive.int } ?: emptyMap(),
                followerCounts = fields["follower_counts"]?.jsonObject?.mapValues { it.value.jsonPrimitive.double } ?: emptyMap(),
            )
    }
}

/**
 * The engine port for the profile parent documents. Same consistency contract
 * as [EventIndex]: an acked [put] is visible to ranking.
 */
interface ProfileIndex : AutoCloseable {
    suspend fun get(pubkey: String): ProfileDoc?

    suspend fun put(profile: ProfileDoc)

    suspend fun remove(pubkey: String)
}
