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
package com.vitorpamplona.sot.vespa

import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.buildJsonObject
import kotlinx.serialization.json.double
import kotlinx.serialization.json.int
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
import kotlinx.serialization.json.putJsonObject

/**
 * One pubkey's ranking state: the `profile` GLOBAL parent document every event
 * references (`author_ref`) and imports for trust-weighted ranking. It is NOT
 * an event. The trust projection derives it from stored kind-30382s and
 * rewrites it whole on every change (recompute, not cell surgery), so it can be
 * rebuilt from the event corpus at any time.
 *
 * Tensor cells are keyed by OBSERVER pubkey: [qualityScores] = rank
 * (influence*100, 0..100), [followerCounts] = verified-follower count.
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
        /**
         * Parse a document-API `fields` object back into a doc. Mapped tensors
         * arrive in TWO shapes: the short object form we feed (`{obs: v}`) and
         * the verbose form document-API GETs render (`{"type": …, "cells": {obs: v}}`).
         */
        fun fromSummary(fields: JsonObject): ProfileDoc =
            ProfileDoc(
                pubkey = fields.getValue("pubkey").jsonPrimitive.content,
                qualityScores = cells(fields["quality_scores"])?.mapValues { it.value.jsonPrimitive.int } ?: emptyMap(),
                followerCounts = cells(fields["follower_counts"])?.mapValues { it.value.jsonPrimitive.double } ?: emptyMap(),
            )

        private fun cells(field: JsonElement?): Map<String, JsonElement>? = field?.jsonObject?.let { it["cells"]?.jsonObject ?: it }
    }
}

/**
 * One score card's contribution to [subject]'s parent doc: the [observer]'s
 * cells, applied as a partial UPDATE with no read and no full-doc rewrite. Null
 * fields leave the corresponding tensor untouched.
 */
data class ProfileCells(
    val subject: String,
    val observer: String,
    val quality: Int?,
    val followers: Double?,
)

/**
 * The engine port for the profile parent documents. Same consistency contract
 * as [EventIndex]: an acked [put] is visible to ranking.
 */
interface ProfileIndex : AutoCloseable {
    suspend fun get(pubkey: String): ProfileDoc?

    suspend fun put(profile: ProfileDoc)

    /** Bulk [put]; implementations may pipeline (see [EventIndex.putAll]). */
    suspend fun putAll(profiles: List<ProfileDoc>) = profiles.forEach { put(it) }

    /**
     * Upsert single tensor cells on the subjects' parents, creating missing
     * parents. This is the insert path's ZERO-READ alternative to a full [put]
     * (Vespa's tensor `add` update). The caller must only send values that are
     * current-best for their (subject, observer); the store's supersession
     * provides exactly that at insert time. Same-subject updates apply in list
     * order. The default implementation is read-modify-write (the in-memory
     * spec).
     */
    suspend fun updateCells(updates: List<ProfileCells>) =
        updates.forEach { u ->
            val cur = get(u.subject) ?: ProfileDoc(u.subject)
            put(
                cur.copy(
                    qualityScores = u.quality?.let { cur.qualityScores + (u.observer to it) } ?: cur.qualityScores,
                    followerCounts = u.followers?.let { cur.followerCounts + (u.observer to it) } ?: cur.followerCounts,
                ),
            )
        }

    suspend fun remove(pubkey: String)
}
