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

import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertNull
import kotlin.test.assertTrue

class EventYqlTest {
    private val hexA = "a".repeat(64)
    private val hexB = "b".repeat(64)

    @Test
    fun `no constraints is a match-all ordered by recency`() {
        val q = EventYql.build(EventQuery())!!
        assertEquals("select * from event where true order by created_at desc", q.yql)
        assertEquals(EventYql.RANK_UNRANKED, q.ranking)
        assertTrue(q.params.isEmpty())
    }

    @Test
    fun `full filter maps every field`() {
        val q =
            EventYql.build(
                EventQuery(
                    kinds = listOf(0, 30382),
                    authors = listOf(hexA),
                    tags = mapOf("p" to listOf(hexB)),
                    since = 100,
                    until = 200,
                    limit = 50,
                ),
            )!!
        assertEquals(
            "select * from event where kind in (0, 30382) and pubkey in (\"$hexA\") " +
                "and (tag_index contains \"p:$hexB\") and created_at >= 100 and created_at <= 200 " +
                "order by created_at desc limit 50",
            q.yql,
        )
    }

    @Test
    fun `search term goes out-of-band and switches ranking on`() {
        val q = EventYql.build(EventQuery(kinds = listOf(0), search = "vitor pamplona"))!!
        assertEquals("select * from event where kind in (0) and ({defaultIndex:\"default\"}userInput(@search))", q.yql)
        assertEquals(mapOf("search" to "vitor pamplona"), q.params)
        assertEquals(EventYql.RANK_TEXT, q.ranking)
        assertFalse("order by" in q.yql, "ranked queries must not force recency order")
    }

    @Test
    fun `owners and expiry map to their attributes`() {
        val q = EventYql.build(EventQuery(owners = listOf(hexA), expiresBefore = 500))!!
        assertEquals("select * from event where owner in (\"$hexA\") and expires_at < 500 order by created_at desc", q.yql)
        assertNull(EventYql.build(EventQuery(owners = listOf("not-hex"))), "no valid owner")
    }

    @Test
    fun `tagsAll requires every value`() {
        val q = EventYql.build(EventQuery(tagsAll = mapOf("t" to listOf("a", "b"))))!!
        assertEquals(
            "select * from event where (tag_index contains \"t:a\" and tag_index contains \"t:b\") order by created_at desc",
            q.yql,
        )
    }

    @Test
    fun `tag values are OR within a name and AND across names`() {
        val q = EventYql.build(EventQuery(tags = mapOf("p" to listOf(hexA, hexB), "t" to listOf("nostr"))))!!
        assertEquals(
            "select * from event where (tag_index contains \"p:$hexA\" or tag_index contains \"p:$hexB\") " +
                "and (tag_index contains \"t:nostr\") order by created_at desc",
            q.yql,
        )
    }

    @Test
    fun `invalid hex entries are dropped but valid ones survive`() {
        val q = EventYql.build(EventQuery(ids = listOf("nope", hexA, hexA.uppercase())))!!
        assertEquals("select * from event where id in (\"$hexA\") order by created_at desc", q.yql)
    }

    @Test
    fun `unsatisfiable constraints build nothing`() {
        assertNull(EventYql.build(EventQuery(authors = listOf("not-hex"))), "no valid author")
        assertNull(EventYql.build(EventQuery(ids = listOf("55"))), "short id")
        assertNull(EventYql.build(EventQuery(tags = mapOf("pp" to listOf("x")))), "multi-letter tag name")
        assertNull(EventYql.build(EventQuery(tags = mapOf("§" to listOf("x")))), "non-ascii tag name")
        assertNull(EventYql.build(EventQuery(tags = mapOf("p" to emptyList()))), "present-but-empty tag values")
        assertNull(EventYql.build(EventQuery(limit = 0)), "limit 0")
    }

    @Test
    fun `caller-supplied strings cannot break out of their literal`() {
        val q = EventYql.build(EventQuery(tags = mapOf("t" to listOf("""x" or true or tag_index contains "y"""))))!!
        assertEquals(
            "select * from event where (tag_index contains \"t:x\\\" or true or tag_index contains \\\"y\") " +
                "order by created_at desc",
            q.yql,
        )
        val newline = EventYql.build(EventQuery(tags = mapOf("t" to listOf("a\nb\\c"))))!!
        assertTrue("tag_index contains \"t:a\\nb\\\\c\"" in newline.yql)
    }
}
