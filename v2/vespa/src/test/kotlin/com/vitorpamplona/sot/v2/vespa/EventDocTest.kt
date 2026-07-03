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
import kotlinx.serialization.json.jsonArray
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith

class EventDocTest {
    // Lossiness traps on purpose: a 3-element tag (relay hint), a non-single-letter
    // tag, a valueless tag, and content with quotes/newlines/emoji/backslash.
    private val doc =
        EventDoc(
            id = "a".repeat(64),
            pubkey = "b".repeat(64),
            createdAt = 1_700_000_000L,
            kind = 30382,
            tags =
                listOf(
                    listOf("d", "c".repeat(64)),
                    listOf("e", "f".repeat(64), "wss://relay.example.com", "root"),
                    listOf("rank", "87"),
                    listOf("alt"),
                    listOf("P", "d".repeat(64)),
                ),
            content = "line one\n\"quoted\" \\backslash\\ emoji 🫥 tab\there",
            sig = "e".repeat(128),
            scope = "wss://relay.example.com|kind=30382",
        )

    @Test
    fun `tagIndex keeps only single-letter tags with a value`() {
        assertEquals(listOf("d:${"c".repeat(64)}", "e:${"f".repeat(64)}", "P:${"d".repeat(64)}"), doc.tagIndex())
    }

    @Test
    fun `event json round-trips losslessly`() {
        assertEquals(doc, EventDoc.fromEventJson(doc.toEventJson(), doc.scope))
    }

    @Test
    fun `summary fields round-trip losslessly`() {
        assertEquals(doc, EventDoc.fromSummary(doc.indexFields()))
    }

    @Test
    fun `reconstructed event carries the exact nip01 fields`() {
        val o = Json.parseToJsonElement(doc.toEventJson()).jsonObject
        assertEquals(doc.id, o.getValue("id").jsonPrimitive.content)
        assertEquals(doc.pubkey, o.getValue("pubkey").jsonPrimitive.content)
        assertEquals(
            doc.createdAt,
            o
                .getValue("created_at")
                .jsonPrimitive.content
                .toLong(),
        )
        assertEquals(
            doc.kind,
            o
                .getValue("kind")
                .jsonPrimitive.content
                .toInt(),
        )
        assertEquals(doc.content, o.getValue("content").jsonPrimitive.content)
        assertEquals(doc.sig, o.getValue("sig").jsonPrimitive.content)
        // The 3-element tag survives whole — tag_index would have dropped the relay hint.
        val eTag =
            o
                .getValue("tags")
                .jsonArray[1]
                .jsonArray
                .map { it.jsonPrimitive.content }
        assertEquals(listOf("e", "f".repeat(64), "wss://relay.example.com", "root"), eTag)
    }

    @Test
    fun `index fields store tags as exact json text and tag_index as the derived view`() {
        val fields = doc.indexFields()
        val storedTags = Json.parseToJsonElement(fields.getValue("tags").jsonPrimitive.content)
        assertEquals(doc.tags, storedTags.jsonArray.map { tag -> tag.jsonArray.map { it.jsonPrimitive.content } })
        assertEquals(doc.tagIndex(), fields.getValue("tag_index").jsonArray.map { it.jsonPrimitive.content })
    }

    @Test
    fun `parses a raw nostr event`() {
        val raw =
            """{"id":"${"1".repeat(64)}","pubkey":"${"2".repeat(64)}","created_at":1700000001,""" +
                """"kind":0,"tags":[["p","${"3".repeat(64)}"]],"content":"{\"name\":\"vitor\"}","sig":"${"4".repeat(128)}"}"""
        val parsed = EventDoc.fromEventJson(raw, scope = "s")
        assertEquals(0, parsed.kind)
        assertEquals(1_700_000_001L, parsed.createdAt)
        assertEquals("""{"name":"vitor"}""", parsed.content)
        assertEquals(listOf(listOf("p", "3".repeat(64))), parsed.tags)
        // ...and what we would serve back is the same event.
        assertEquals(parsed, EventDoc.fromEventJson(parsed.toEventJson(), "s"))
    }

    @Test
    fun `malformed event json throws`() {
        assertFailsWith<Exception> { EventDoc.fromEventJson("""{"id":"x"}""", "s") }
    }
}
