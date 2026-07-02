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
package com.vitorpamplona.sot.indexer

import com.vitorpamplona.quartz.nip01Core.metadata.MetadataEvent
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.RelayUrlNormalizer
import com.vitorpamplona.quartz.nip01Core.store.ObservableEventStore
import com.vitorpamplona.quartz.nip01Core.store.sqlite.DefaultIndexingStrategy
import com.vitorpamplona.quartz.nip01Core.store.sqlite.EventStore
import com.vitorpamplona.quartz.nip85TrustedAssertions.list.TrustProviderListEvent
import com.vitorpamplona.quartz.nip85TrustedAssertions.users.ContactCardEvent
import com.vitorpamplona.sot.vespa.MockVespa
import com.vitorpamplona.sot.vespa.VespaClient
import kotlinx.coroutines.runBlocking
import java.io.File
import java.util.concurrent.ConcurrentHashMap
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNull

/**
 * Anti-entropy semantics against a drifted mock index: every difference class —
 * a doc the index never got, a stale profile, a stale score id, a cell whose
 * event is gone, and profile fields whose kind:0 is gone — is found and, with
 * repair on, converges to the store's truth. The store is REAL (SQLite); the
 * index is [MockVespa].
 */
class IndexVerifierTest {
    private val observer = "0".repeat(64)
    private val alice = "1".repeat(64) // kind:0 author whose doc the index is missing
    private val bob = "b".repeat(64) // kind:0 author whose doc is stale
    private val ghost = "e".repeat(64) // in the index only; the store never saw them
    private val service = "2".repeat(64) // 30382 signer for `observer`
    private val subject = "3".repeat(64) // scored by `service`; cell id is stale

    private val idAlice = "4".repeat(64)
    private val idBob = "5".repeat(64)
    private val idBobOld = "6".repeat(64)
    private val idScore = "7".repeat(64)
    private val idScoreOld = "8".repeat(64)

    private var now = 1_700_000_000L

    private fun next() = ++now

    private fun kind0(
        id: String,
        author: String,
        name: String,
    ) = MetadataEvent(id, author, next(), emptyArray(), """{"name":"$name"}""", "")

    private fun list10040() = TrustProviderListEvent("a".repeat(64), observer, next(), arrayOf(arrayOf("30382:rank", service, "wss://scores.example.com/")), "", "")

    private fun card30382(
        id: String,
        rank: Int?,
    ) = ContactCardEvent(
        id,
        service,
        next(),
        listOfNotNull(arrayOf("d", subject), rank?.let { arrayOf("rank", it.toString()) }).toTypedArray(),
        "",
        "",
    )

    @Test
    fun `verify finds and repairs every difference class`() =
        runBlocking {
            val mock = MockVespa()
            val db =
                File.createTempFile("index-verifier", ".db").also {
                    it.delete()
                    it.deleteOnExit()
                }
            val store = ObservableEventStore(EventStore(db.path, RelayUrlNormalizer.normalize("ws://localhost:7777"), DefaultIndexingStrategy(indexFullTextSearch = false)))
            val vespa = VespaClient("http://127.0.0.1:${mock.port}")

            try {
                // The store's truth: alice + bob profiles, one score on `subject`.
                store.insert(list10040())
                store.insert(kind0(idAlice, alice, "alice"))
                store.insert(kind0(idBob, bob, "bob"))
                store.insert(card30382(idScore, 42))

                // The drifted index: alice's doc is missing entirely; bob's profile is
                // stale; subject's score cell was written from a superseded event;
                // ghost has profile fields with no kind:0 behind them and a score
                // cell from an event the store doesn't hold.
                mock.docs[bob] = ConcurrentHashMap(mapOf("event_id" to idBobOld, "name" to "old-bob"))
                mock.docs[ghost] = ConcurrentHashMap(mapOf("event_id" to "9".repeat(64), "name" to "ghost"))
                mock.cells[subject] = ConcurrentHashMap(mapOf(observer to 7))
                mock.scoreIds[subject] = ConcurrentHashMap(mapOf(observer to idScoreOld))
                mock.cells[ghost] = ConcurrentHashMap(mapOf(observer to 1))
                mock.scoreIds[ghost] = ConcurrentHashMap(mapOf(observer to "f".repeat(64)))

                // Read-only pass: reports, changes nothing.
                val dryRun = IndexVerifier(store, vespa, repair = false, log = { }).verify()
                assertEquals(5, dryRun.diffs, "expected one diff per difference class: ${dryRun.summary()}")
                assertEquals(0, dryRun.repairs)
                assertEquals("old-bob", mock.docs[bob]?.get("name"))

                // Repair pass: the index converges to the store.
                val repaired = IndexVerifier(store, vespa, repair = true, log = { }).verify()
                assertEquals(5, repaired.diffs, repaired.summary())
                assertEquals(5, repaired.repairs, repaired.summary())

                assertEquals(idAlice, mock.docs[alice]?.get("event_id"), "missing doc re-fed")
                assertEquals("alice", mock.docs[alice]?.get("name"))
                assertEquals(idBob, mock.docs[bob]?.get("event_id"), "stale profile refreshed")
                assertEquals("bob", mock.docs[bob]?.get("name"))
                assertEquals(idScore, mock.scoreIds[subject]?.get(observer), "stale score cell rewritten")
                assertEquals(42, mock.cells[subject]?.get(observer))
                assertEquals("", mock.docs[ghost]?.get("event_id"), "orphaned profile blanked")
                assertEquals("", mock.docs[ghost]?.get("name"))
                assertNull(mock.cells[ghost]?.get(observer), "orphaned score cell removed")
                assertNull(mock.scoreIds[ghost]?.get(observer))

                // A verified index verifies clean.
                val clean = IndexVerifier(store, vespa, repair = false, log = { }).verify()
                assertEquals(0, clean.diffs, clean.summary())
            } finally {
                vespa.close()
                store.close()
                mock.stop()
            }
        }
}
