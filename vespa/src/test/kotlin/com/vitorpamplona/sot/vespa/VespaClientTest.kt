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

import kotlin.test.AfterTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNull
import kotlin.test.assertTrue

/**
 * VespaClient's write payloads and provenance lookups against [MockVespa],
 * which implements the document + search API slice the client speaks (updates
 * over h2c through the feed client, queries over HTTP/1.1).
 */
class VespaClientTest {
    private val alice = "1".repeat(64)
    private val subject = "3".repeat(64)
    private val observer = "0".repeat(64)
    private val idProfile = "4".repeat(64)
    private val idScore = "5".repeat(64)

    private val mock = MockVespa()
    private val client = VespaClient("http://127.0.0.1:${mock.port}")

    @AfterTest
    fun tearDown() {
        client.close()
        mock.stop()
    }

    @Test
    fun `upsertProfile assigns every index field and the provenance id`() {
        client.upsertProfile(Profile(alice, name = "alice", nip05 = "alice@example.com", eventId = idProfile)).join()

        val doc = mock.docs[alice].orEmpty()
        assertEquals("alice", doc["name"])
        assertEquals("alice@example.com", doc["nip05"])
        assertEquals(idProfile, doc["event_id"])
        // Absent optionals are blanked, not skipped - an upsert must overwrite old values.
        assertEquals("", doc["about"])
        assertEquals("", doc["website"])
    }

    @Test
    fun `blankProfile clears the fields but keeps the doc`() {
        client.upsertProfile(Profile(alice, name = "alice", eventId = idProfile)).join()
        client.blankProfile(alice).join()

        val doc = mock.docs[alice].orEmpty()
        assertEquals("", doc["name"])
        assertEquals("", doc["event_id"])
        assertTrue(mock.docs.containsKey(alice), "the doc survives so its score cells do too")
    }

    @Test
    fun `upsertScore writes the tensor cell and its source id, removeScore removes both`() {
        client.upsertScore(subject, observer, rank = 42, eventId = idScore).join()
        assertEquals(42, mock.cells[subject]?.get(observer))
        assertEquals(idScore, mock.scoreIds[subject]?.get(observer))

        client.removeScore(subject, observer).join()
        assertNull(mock.cells[subject]?.get(observer))
        assertNull(mock.scoreIds[subject]?.get(observer))
    }

    @Test
    fun `provenance lookups resolve event ids back to docs`() {
        client.upsertProfile(Profile(alice, name = "alice", eventId = idProfile)).join()
        client.upsertScore(subject, observer, rank = 7, eventId = idScore).join()

        assertEquals(alice, client.findProfileByEventId(idProfile))
        assertNull(client.findProfileByEventId("9".repeat(64)))

        assertEquals(subject to observer, client.findScoreByEventId(idScore))
        assertNull(client.findScoreByEventId("9".repeat(64)))

        assertEquals(listOf(subject), client.findSubjectsByObserver(observer))
    }

    @Test
    fun `visitDocs walks every doc with its provenance fields`() {
        client.upsertProfile(Profile(alice, name = "alice", eventId = idProfile)).join()
        client.upsertScore(subject, observer, rank = 7, eventId = idScore).join()

        val page = client.visitDocs()
        assertNull(page.continuation)
        val byPubkey = page.docs.associateBy { it.pubkey }
        assertEquals(setOf(alice, subject), byPubkey.keys)
        assertEquals(idProfile, byPubkey[alice]?.eventId)
        assertEquals(mapOf(observer to idScore), byPubkey[subject]?.scoreEventIds)
    }
}
