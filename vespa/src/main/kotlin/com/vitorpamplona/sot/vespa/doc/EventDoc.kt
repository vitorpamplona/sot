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
package com.vitorpamplona.sot.vespa.doc
import com.vitorpamplona.quartz.nip01Core.core.Event
import com.vitorpamplona.quartz.nip01Core.core.HexKey
import com.vitorpamplona.quartz.nip01Core.tags.dTag.dTag
import com.vitorpamplona.quartz.nip40Expiration.expiration
import com.vitorpamplona.sot.vespa.isSingleLetterTagName
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonArray
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.buildJsonObject
import kotlinx.serialization.json.int
import kotlinx.serialization.json.jsonArray
import kotlinx.serialization.json.jsonPrimitive
import kotlinx.serialization.json.long
import kotlinx.serialization.json.put

/**
 * One live Nostr event as a Vespa `event` document (docid = [id]). The NIP-01
 * fields are held LOSSLESSLY. The signature is over the canonical serialization
 * of these exact values, so [toEventJson] can rebuild a complete event that
 * clients re-verify. There is no separate raw-blob copy.
 *
 * [tags] is the exact tag array. The queryable `tag_index` field ([tagIndex])
 * is a derived, lossy view: single-letter tag names only, first value only. It
 * is used for `#x` filter recall and never for reconstruction.
 *
 * The store maps its events into this and verifies signatures BEFORE
 * constructing one, so everything in the index is assumed already verified.
 * NIP-01 serialization and the tag-derived reads go through the Quartz [Event]
 * this doc reconstructs ([toEventJson], [expiresAt], [dTag]).
 *
 * [owner] and [search] are DERIVED fields the store computes with Nostr
 * knowledge. The owner is the pubkey Nostr semantics key off: the gift-wrap
 * recipient for kind 1059, else the author. [search] is the kind-specific
 * decomposition of searchable kinds into the schema's search fields. All-null
 * means the event is invisible to NIP-50 search.
 */
data class EventDoc(
    val id: HexKey,
    val pubkey: HexKey,
    val createdAt: Long,
    val kind: Int,
    val tags: List<List<String>>,
    val content: String,
    val sig: String,
    val owner: HexKey = pubkey,
    val search: SearchFields = SearchFields.NONE,
) {
    /** [tags] as Quartz's `String[][]` shape, for its tag-array helpers and event reconstruction. */
    private fun tagsArray(): Array<Array<String>> = Array(tags.size) { tags[it].toTypedArray() }

    /**
     * The queryable `"<letter>:<value>"` pairs. One per tag whose name is a
     * single ASCII letter (the names NIP-01 `#x` filters can address) and that
     * has a value. Everything else still round-trips through [tags].
     */
    fun tagIndex(): List<String> =
        tags.mapNotNull { tag ->
            val name = tag.getOrNull(0) ?: return@mapNotNull null
            val value = tag.getOrNull(1) ?: return@mapNotNull null
            if (isSingleLetterTagName(name)) "$name:$value" else null
        }

    /** The NIP-40 expiration timestamp (Quartz's reader); null = never expires. */
    fun expiresAt(): Long? = tagsArray().expiration()

    /** The `d` tag value, or "" when absent — the addressable bucketing key (missing == empty). */
    fun dTagOrEmpty(): String = tagsArray().dTag()

    /** The first `d` tag's value — an addressable event's identity key; null/blank = none. */
    fun dTag(): String? = dTagOrEmpty().takeIf { it.isNotEmpty() }

    /** The document's field map — one shape for both feeding and summary parsing ([fromSummary]). */
    fun indexFields(): JsonObject =
        buildJsonObject {
            put("id", id)
            put("pubkey", pubkey)
            put("created_at", createdAt)
            put("kind", kind)
            put("tags", tagsAsJson().toString())
            put("tag_index", JsonArray(tagIndex().map(::JsonPrimitive)))
            put("content", content)
            put("sig", sig)
            put("owner", owner)
            // Reference to the author's ranking state (the global profile
            // parent), which controls how any kind ranks by the observer's
            // trust in its author. It is purely pubkey-derived, so it's stamped
            // here rather than by an extractor.
            put("author_ref", "id:profile:profile::$pubkey")
            for ((field, value) in search.fields()) put(field, value)
            // Always written. An absent numeric attribute reads as 0 in Vespa,
            // which would make "not yet expired" range queries impossible.
            put("expires_at", expiresAt() ?: NO_EXPIRATION)
        }

    /** The complete NIP-01 event JSON, rebuilt from the exact stored values via Quartz's canonical serializer. */
    fun toEventJson(): String = Event(id, pubkey, createdAt, kind, tagsArray(), content, sig).toJson()

    private fun tagsAsJson(): JsonArray = JsonArray(tags.map { tag -> JsonArray(tag.map(::JsonPrimitive)) })

    companion object {
        /** The `expires_at` value for an event with no NIP-40 expiration: far enough out to outlive every range check. */
        const val NO_EXPIRATION = Long.MAX_VALUE

        /** Parse a raw NIP-01 event JSON into a doc (Quartz's parser). Throws on a malformed event. */
        fun fromEventJson(raw: String): EventDoc =
            Event.fromJson(raw).let { e ->
                EventDoc(
                    id = e.id,
                    pubkey = e.pubKey,
                    createdAt = e.createdAt,
                    kind = e.kind,
                    tags = e.tags.map { it.toList() },
                    content = e.content,
                    sig = e.sig,
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
                owner = fields["owner"]?.jsonPrimitive?.content ?: pubkey,
                search = SearchFields.fromFields { fields[it]?.jsonPrimitive?.content },
            )
        }
    }
}
