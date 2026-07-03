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

import kotlinx.coroutines.runBlocking
import kotlin.test.AfterTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNull

/**
 * [VespaEventIndex] against [MockVespaEngine]: every operation goes over real
 * HTTP (h2c feed writes, HTTP/1.1 reads), the mock parses the YQL back into an
 * [EventQuery], and results must agree with a directly-driven
 * [InMemoryEventIndex] — builder, wire format, and matching semantics all
 * checked in one loop.
 */
class VespaEventIndexTest {
    private val mock = MockVespaEngine()
    private val index = VespaEventIndex(mock.url)
    private val reference = InMemoryEventIndex()

    @AfterTest
    fun tearDown() {
        index.close()
        mock.stop()
    }

    private var seq = 0

    private fun doc(
        kind: Int = 1,
        pubkey: String = "a1".repeat(32),
        at: Long = (1000 + seq).toLong(),
        tags: List<List<String>> = emptyList(),
        content: String = "",
        owner: String = pubkey,
        search: SearchFields = SearchFields.NONE,
    ) = EventDoc(
        id = (++seq).toString(16).padStart(64, '0'),
        pubkey = pubkey,
        createdAt = at,
        kind = kind,
        tags = tags,
        content = content,
        sig = "e".repeat(128),
        owner = owner,
        search = search,
    )

    private fun seed(vararg docs: EventDoc) =
        runBlocking {
            for (d in docs) {
                index.put(d)
                reference.put(d)
            }
        }

    /** The wire answer must equal the in-memory spec's answer, in order. */
    private fun check(query: EventQuery) =
        runBlocking {
            assertEquals(reference.search(query).map { it.id }, index.search(query).map { it.id }, "query: $query")
        }

    @Test
    fun `put get remove round-trip over the wire`() =
        runBlocking {
            val d =
                doc(
                    kind = 30382,
                    tags = listOf(listOf("d", "b2".repeat(32)), listOf("e", "f".repeat(64), "wss://relay.example.com", "root")),
                    content = "line\n\"quoted\" 🫥",
                    search = SearchFields(name = "findable", primary = "also findable"),
                )
            index.put(d)
            assertEquals(d, index.get(d.id))
            index.remove(d.id)
            assertNull(index.get(d.id))
            assertNull(index.get("0".repeat(64)))
        }

    @Test
    fun `search agrees with the in-memory spec across the filter surface`() {
        val bob = "b2".repeat(32)
        seed(
            doc(kind = 0, search = SearchFields(name = "vitor", about = "pamplona dev")),
            doc(kind = 1, tags = listOf(listOf("p", bob)), content = "hi bob"),
            doc(kind = 1, pubkey = bob, at = 5000),
            doc(kind = 30382, pubkey = bob, tags = listOf(listOf("d", "x"), listOf("t", "nostr"), listOf("t", "search"))),
            doc(kind = 1, owner = bob, tags = listOf(listOf("expiration", "2000"))),
            // Escaping round-trip: the tag value must survive YQL quoting + parsing.
            doc(kind = 1, tags = listOf(listOf("t", "quo\"te\\and\nnewline"))),
        )
        check(EventQuery())
        check(EventQuery(kinds = listOf(1)))
        check(EventQuery(authors = listOf(bob)))
        check(EventQuery(owners = listOf(bob)))
        check(EventQuery(tags = mapOf("p" to listOf(bob))))
        check(EventQuery(tags = mapOf("t" to listOf("nostr", "missing"))))
        check(EventQuery(tagsAll = mapOf("t" to listOf("nostr", "search"))))
        check(EventQuery(tags = mapOf("t" to listOf("quo\"te\\and\nnewline"))))
        check(EventQuery(since = 1002, until = 1005))
        check(EventQuery(kinds = listOf(1), limit = 2))
        check(EventQuery(notExpiredAt = 3000))
        check(EventQuery(expiresBefore = 3000))
        check(EventQuery(search = "vitor"))
        check(EventQuery(kinds = listOf(0, 1), tags = mapOf("p" to listOf(bob)), until = 9000))
    }

    @Test
    fun `count reads totalCount past the hits page`() =
        runBlocking {
            seed(*(1..7).map { doc(kind = 7) }.toTypedArray())
            assertEquals(7, index.count(EventQuery(kinds = listOf(7))))
            // A limit'd search returns the page, the count stays total.
            assertEquals(3, index.search(EventQuery(kinds = listOf(7), limit = 3)).size)
        }

    @Test
    fun `match-nothing queries never reach the wire`() =
        runBlocking {
            seed(doc())
            assertEquals(emptyList(), index.search(EventQuery(authors = listOf("not-hex"))))
            assertEquals(0, index.count(EventQuery(limit = 0)))
        }
}
