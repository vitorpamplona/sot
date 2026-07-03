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

import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonArray
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.buildJsonObject
import kotlinx.serialization.json.int
import kotlinx.serialization.json.jsonArray
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
import kotlinx.serialization.json.long

/**
 * One live Nostr event as a Vespa `event` document — the unit of the relay
 * mirror (docid = [id]). The NIP-01 fields are held LOSSLESSLY: the signature
 * is over the canonical serialization of these exact values, so [toEventJson]
 * reconstructs a complete event clients can re-verify — no raw-blob duplicate.
 *
 * [tags] is the exact tag array; the queryable `tag_index` field ([tagIndex])
 * is a derived, lossy view (single-letter tag names only, first value only)
 * used for `#x` filter recall and never for reconstruction.
 *
 * Plain data, no Nostr library types: the store maps its events into this,
 * and verifies signatures BEFORE constructing one — everything in the index
 * is assumed already verified.
 *
 * [owner] and [searchText] are DERIVED fields the store computes with Nostr
 * knowledge this module doesn't have: the owner is the pubkey Nostr semantics
 * key off (the gift-wrap recipient for kind 1059, else the author), and
 * searchText is the kind-specific indexable text of searchable kinds (null =
 * invisible to NIP-50 search, like SQLite's FTS table).
 */
data class EventDoc(
    val id: String,
    val pubkey: String,
    val createdAt: Long,
    val kind: Int,
    val tags: List<List<String>>,
    val content: String,
    val sig: String,
    /** Optional provenance (where this doc was first synced from); never semantics. */
    val scope: String,
    val owner: String = pubkey,
    val searchText: String? = null,
) {
    /**
     * The queryable `"<letter>:<value>"` pairs: one per tag whose name is a
     * single ASCII letter (the space NIP-01 `#x` filters can address) and that
     * has a value. Everything else still round-trips through [tags].
     */
    fun tagIndex(): List<String> =
        tags.mapNotNull { tag ->
            val name = tag.getOrNull(0) ?: return@mapNotNull null
            val value = tag.getOrNull(1) ?: return@mapNotNull null
            if (name.length == 1 && (name[0] in 'a'..'z' || name[0] in 'A'..'Z')) "$name:$value" else null
        }

    /** The NIP-40 expiration timestamp, derived from the exact tags; null = never expires. */
    fun expiresAt(): Long? = tags.firstOrNull { it.size >= 2 && it[0] == "expiration" }?.get(1)?.toLongOrNull()

    /** The document's field map — one shape for both feeding and summary parsing ([fromSummary]). */
    fun indexFields(): JsonObject =
        buildJsonObject {
            put("id", JsonPrimitive(id))
            put("pubkey", JsonPrimitive(pubkey))
            put("created_at", JsonPrimitive(createdAt))
            put("kind", JsonPrimitive(kind))
            put("tags", JsonPrimitive(tagsAsJson().toString()))
            put("tag_index", JsonArray(tagIndex().map(::JsonPrimitive)))
            put("content", JsonPrimitive(content))
            put("sig", JsonPrimitive(sig))
            put("scope", JsonPrimitive(scope))
            put("owner", JsonPrimitive(owner))
            searchText?.let { put("search_text", JsonPrimitive(it)) }
            // Always written: an absent numeric attribute reads as 0 in Vespa,
            // which would make "not yet expired" range queries impossible.
            put("expires_at", JsonPrimitive(expiresAt() ?: NO_EXPIRATION))
        }

    /** The complete NIP-01 event JSON, rebuilt from the exact stored values. */
    fun toEventJson(): String =
        buildJsonObject {
            put("id", JsonPrimitive(id))
            put("pubkey", JsonPrimitive(pubkey))
            put("created_at", JsonPrimitive(createdAt))
            put("kind", JsonPrimitive(kind))
            put("tags", tagsAsJson())
            put("content", JsonPrimitive(content))
            put("sig", JsonPrimitive(sig))
        }.toString()

    private fun tagsAsJson(): JsonArray = JsonArray(tags.map { tag -> JsonArray(tag.map(::JsonPrimitive)) })

    companion object {
        /** The `expires_at` value of an event without a NIP-40 expiration: far enough to outlive every range check. */
        const val NO_EXPIRATION = Long.MAX_VALUE

        /** Parse a raw NIP-01 event JSON into a doc for [scope]. Throws on a malformed event. */
        fun fromEventJson(
            raw: String,
            scope: String,
        ): EventDoc {
            val o = Json.parseToJsonElement(raw).jsonObject
            return EventDoc(
                id = o.getValue("id").jsonPrimitive.content,
                pubkey = o.getValue("pubkey").jsonPrimitive.content,
                createdAt = o.getValue("created_at").jsonPrimitive.long,
                kind = o.getValue("kind").jsonPrimitive.int,
                tags = o.getValue("tags").jsonArray.map { tag -> tag.jsonArray.map { it.jsonPrimitive.content } },
                content = o.getValue("content").jsonPrimitive.content,
                sig = o.getValue("sig").jsonPrimitive.content,
                scope = scope,
            )
        }

        /** Parse a Vespa summary/visit `fields` object (the [indexFields] shape) back into a doc. */
        fun fromSummary(fields: JsonObject): EventDoc {
            val pubkey = fields.getValue("pubkey").jsonPrimitive.content
            return EventDoc(
                id = fields.getValue("id").jsonPrimitive.content,
                pubkey = pubkey,
                createdAt = fields.getValue("created_at").jsonPrimitive.long,
                kind = fields.getValue("kind").jsonPrimitive.int,
                tags =
                    Json
                        .parseToJsonElement(fields.getValue("tags").jsonPrimitive.content)
                        .jsonArray
                        .map { tag -> tag.jsonArray.map { it.jsonPrimitive.content } },
                content = fields["content"]?.jsonPrimitive?.content ?: "",
                sig = fields.getValue("sig").jsonPrimitive.content,
                scope = fields["scope"]?.jsonPrimitive?.content ?: "",
                owner = fields["owner"]?.jsonPrimitive?.content ?: pubkey,
                searchText = fields["search_text"]?.jsonPrimitive?.content,
            )
        }
    }
}
