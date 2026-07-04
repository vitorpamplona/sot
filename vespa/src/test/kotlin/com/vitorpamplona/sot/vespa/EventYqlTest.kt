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
    fun `search words go out-of-band and switch the default ranking on`() {
        val q = EventYql.build(EventQuery(kinds = listOf(0), search = "vitor pamplona"))!!
        assertTrue(q.yql.startsWith("select * from event where kind in (0) and ((("), q.yql)
        assertEquals("vitor", q.params["w0"])
        assertEquals("pamplona", q.params["w1"])
        assertEquals("vitorpamplona", q.params["wj"], "two words get the joined-CamelCase variant")
        assertFalse("wp0" in q.params, "adjacent pairs only appear from three words up")
        assertEquals("2.0", q.params["ranking.features.query(w_gram)"], "no short word: normal gram weight")
        assertEquals(EventYql.RANK_SEARCH, q.ranking)
        assertFalse("order by" in q.yql, "ranked queries must not force recency order")
    }

    @Test
    fun `word groups carry the per-field clauses, labels, and gram nets`() {
        val q = EventYql.build(EventQuery(search = "pamplona"))!!
        // Primary-role fields: labeled exact/prefix clauses.
        assertTrue("({defaultIndex:\"name\",label:\"mtch_exact\"}userInput(@w0))" in q.yql, q.yql)
        assertTrue("({defaultIndex:\"name\",prefix:true,label:\"mtch_prefix\"}userInput(@w0))" in q.yql)
        assertTrue("({defaultIndex:\"search_primary\",label:\"mtch_exact\"}userInput(@w0))" in q.yql)
        // Affiliation role: only the exact clause is labeled.
        assertTrue("({defaultIndex:\"about\",label:\"mtch_affil\"}userInput(@w0))" in q.yql)
        assertTrue("({defaultIndex:\"about\",prefix:true}userInput(@w0))" in q.yql)
        // Recall role: unlabeled.
        assertTrue("({defaultIndex:\"search_text\"}userInput(@w0))" in q.yql)
        // 8 chars: one edit of fuzz, not two.
        assertTrue("({defaultIndex:\"name\",fuzzy:{maxEditDistance:1,prefixLength:2},label:\"mtch_fz1\"}userInput(@w0))" in q.yql)
        assertFalse("maxEditDistance:2" in q.yql)
        // Trigram safety nets: OR against the name grams, AND against about_gram.
        assertTrue("name_gram contains \"pam\"" in q.yql)
        assertTrue("display_name_gram contains \"pam\"" in q.yql)
        assertTrue("search_primary_gram contains \"pam\"" in q.yql)
        assertTrue("(about_gram contains \"pam\" and about_gram contains \"amp\"" in q.yql)
    }

    @Test
    fun `fuzzy budget is length-gated`() {
        val short = EventYql.build(EventQuery(search = "bob"))!!
        assertFalse("fuzzy" in short.yql, "under 4 chars: exact and prefix only")
        assertEquals("8.0", short.params["ranking.features.query(w_gram)"], "short word leans on the gram net")
        assertFalse("wj" in short.params, "a single word has no joined variant")

        val long = EventYql.build(EventQuery(search = "decentralization"))!!
        assertTrue("fuzzy:{maxEditDistance:2,prefixLength:2}" in long.yql, "9+ chars: two edits")
    }

    @Test
    fun `three or more words add adjacent-pair variants and cap at six`() {
        val q = EventYql.build(EventQuery(search = "john carvalho dev one two three seven eight"))!!
        assertEquals("johncarvalho", q.params["wp0"])
        assertEquals("carvalhodev", q.params["wp1"])
        assertEquals("johncarvalhodevonetwothree", q.params["wj"], "variants are built from the capped words")
        assertEquals("three", q.params["w5"])
        assertFalse("w6" in q.params, "words beyond ${EventYql.MAX_QUERY_WORDS} are dropped")
    }

    @Test
    fun `ranking override without a term is a trust-ordered match-all`() {
        val q = EventYql.build(EventQuery(ranking = EventYql.RANK_DESC, minRank = 2.0, observer = hexA))!!
        assertEquals("select * from event where true", q.yql)
        assertEquals(EventYql.RANK_DESC, q.ranking)
        assertEquals("{$hexA:1.0}", q.params["ranking.features.query(user_q)"])
        assertEquals("2.0", q.params["ranking.features.query(min_rank)"])
        assertFalse("order by" in q.yql, "a rank profile owns the order")
    }

    @Test
    fun `observer is ranking context only — never emitted for unranked recall`() {
        val unranked = EventYql.build(EventQuery(kinds = listOf(1), observer = hexA))!!
        assertTrue(unranked.params.isEmpty(), "no term, no profile: pure NIP-01 recall")
        assertEquals(EventYql.RANK_UNRANKED, unranked.ranking)

        val ranked = EventYql.build(EventQuery(search = "vitor", observer = hexA))!!
        assertEquals("{$hexA:1.0}", ranked.params["ranking.features.query(user_q)"])
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
