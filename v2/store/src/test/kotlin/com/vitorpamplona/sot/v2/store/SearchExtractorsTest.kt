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
package com.vitorpamplona.sot.v2.store

import com.vitorpamplona.quartz.nip01Core.core.Event
import com.vitorpamplona.quartz.nip01Core.metadata.MetadataEvent
import com.vitorpamplona.quartz.nip10Notes.TextNoteEvent
import com.vitorpamplona.quartz.nip17Dm.messages.ChatMessageEvent
import com.vitorpamplona.quartz.nip23LongContent.LongTextNoteEvent
import com.vitorpamplona.sot.v2.vespa.SearchFields
import kotlin.test.Test
import kotlin.test.assertEquals

class SearchExtractorsTest {
    private val alice = "a1".repeat(32)

    @Test
    fun `kind 0 decomposes into the Brainstorm profile group`() {
        val content = """{"name":"vitor","display_name":"Vitor P","about":"builds nostr","nip05":"vitor@vitorpamplona.com","lud16":"me@wallet.com","website":"https://vitorpamplona.com","picture":"https://x/y.jpg"}"""
        val fields = SearchExtractors.extract(MetadataEvent("1".repeat(64), alice, 1L, emptyArray(), content, ""))
        assertEquals(
            SearchFields(
                name = "vitor",
                displayName = "Vitor P",
                about = "builds nostr",
                nip05 = "vitor@vitorpamplona.com",
                lud16 = "me@wallet.com",
                website = "https://vitorpamplona.com",
            ),
            fields,
        )
    }

    @Test
    fun `long-form decomposes into title, summary plus hashtags, content`() {
        val tags = arrayOf(arrayOf("d", "post"), arrayOf("title", "My Post"), arrayOf("summary", "tl;dr"), arrayOf("t", "nostr"), arrayOf("t", "search"))
        val fields = SearchExtractors.extract(LongTextNoteEvent("2".repeat(64), alice, 1L, tags, "the whole article", ""))
        assertEquals(SearchFields(primary = "My Post", secondary = "tl;dr\nnostr search", text = "the whole article"), fields)
    }

    @Test
    fun `notes use the NIP-14 subject and hashtags`() {
        val tags = arrayOf(arrayOf("subject", "meetup"), arrayOf("t", "brazil"))
        val fields = SearchExtractors.extract(TextNoteEvent("3".repeat(64), alice, 1L, tags, "see you there", ""))
        assertEquals(SearchFields(primary = "meetup", secondary = "brazil", text = "see you there"), fields)
    }

    @Test
    fun `unmapped searchable kinds fall back to indexableContent in the tertiary tier`() {
        val fields = SearchExtractors.extract(ChatMessageEvent("4".repeat(64), alice, 1L, emptyArray(), "hello group", ""))
        assertEquals(SearchFields(text = "hello group"), fields)
    }

    @Test
    fun `non-searchable kinds stay invisible`() {
        assertEquals(SearchFields.NONE, SearchExtractors.extract(Event("5".repeat(64), alice, 1L, 7, emptyArray(), "+", "")))
    }
}
