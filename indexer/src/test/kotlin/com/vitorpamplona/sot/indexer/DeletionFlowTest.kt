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
import com.vitorpamplona.quartz.nip09Deletions.DeletionEvent
import com.vitorpamplona.quartz.nip62RequestToVanish.RequestToVanishEvent
import com.vitorpamplona.quartz.nip85TrustedAssertions.list.TrustProviderListEvent
import com.vitorpamplona.quartz.nip85TrustedAssertions.users.ContactCardEvent
import com.vitorpamplona.sot.vespa.VespaClient
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import java.io.File
import kotlin.test.Test
import kotlin.test.fail

/**
 * End-to-end deletion semantics through a REAL event store and the REAL projection,
 * against a mock Vespa that implements the slice of the document + search API the
 * engine uses. Verifies that a kind:5 erases profile fields and score cells both
 * by raw event id (e-tag, resolved through the stored provenance ids) and by
 * address (a-tag).
 */
class DeletionFlowTest {
    // Distinct 64-hex actors / event ids.
    private val observer = "0".repeat(64)
    private val alice = "1".repeat(64) // kind:0 author
    private val service = "2".repeat(64) // 30382 signer for `observer`
    private val subject = "3".repeat(64) // who the 30382 scores

    private val idProfile = "4".repeat(64)
    private val idScore = "5".repeat(64)
    private val idProfile2 = "6".repeat(64)
    private val idScore2 = "7".repeat(64)

    private var now = 1_700_000_000L

    private fun next() = ++now

    private fun kind0(
        id: String,
        name: String,
    ) = MetadataEvent(id, alice, next(), emptyArray(), """{"name":"$name"}""", "")

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

    private fun kind5(
        id: String,
        author: String,
        vararg tags: Array<String>,
    ) = DeletionEvent(id, author, next(), arrayOf(*tags), "", "")

    @Test
    fun `kind5 deletes profiles and scores by event id and by address`() =
        runBlocking {
            val mock = MockVespa()
            val db =
                File.createTempFile("deletion-flow", ".db").also {
                    it.delete()
                    it.deleteOnExit()
                }
            val store = ObservableEventStore(EventStore(db.path, RelayUrlNormalizer.normalize("ws://localhost:7777"), DefaultIndexingStrategy(indexFullTextSearch = false)))
            val vespa = VespaClient("http://127.0.0.1:${mock.port}")
            val projection = VespaProjection(store, vespa) { }
            val scope = CoroutineScope(Dispatchers.IO + SupervisorJob())
            scope.launch { projection.run() }

            try {
                // Feed: provider list, profile, score.
                store.insert(list10040())
                store.insert(kind0(idProfile, "alice"))
                awaitTrue("profile indexed with provenance id") {
                    mock.docs[alice]?.get("event_id") == idProfile && mock.docs[alice]?.get("name") == "alice"
                }
                store.insert(card30382(idScore, 42))
                awaitTrue("score cell + provenance id indexed") {
                    mock.cells[subject]?.get(observer) == 42 && mock.scoreIds[subject]?.get(observer) == idScore
                }

                // Delete BY EVENT ID (e-tags).
                store.insert(kind5("b".repeat(64), alice, arrayOf("e", idProfile)))
                awaitTrue("profile blanked via e-tag id") {
                    mock.docs[alice]?.get("name") == "" && mock.docs[alice]?.get("event_id") == ""
                }
                store.insert(kind5("c".repeat(64), service, arrayOf("e", idScore)))
                awaitTrue("score cell removed via e-tag id") {
                    mock.cells[subject]?.containsKey(observer) != true && mock.scoreIds[subject]?.containsKey(observer) != true
                }

                // Re-feed, then delete BY ADDRESS (a-tags).
                store.insert(kind0(idProfile2, "alice2"))
                awaitTrue("profile re-indexed") { mock.docs[alice]?.get("name") == "alice2" }
                store.insert(card30382(idScore2, 43))
                awaitTrue("score re-indexed") { mock.cells[subject]?.get(observer) == 43 }

                store.insert(kind5("d".repeat(64), alice, arrayOf("a", "0:$alice:")))
                awaitTrue("profile blanked via a-tag address") { mock.docs[alice]?.get("name") == "" }
                store.insert(kind5("e".repeat(64), service, arrayOf("a", "30382:$service:$subject")))
                awaitTrue("score cell removed via a-tag address") { mock.cells[subject]?.containsKey(observer) != true }

                // Retraction by SUPERSESSION: a newer 30382 for the subject without a rank tag.
                store.insert(card30382("9".repeat(64), 44))
                awaitTrue("score re-indexed again") { mock.cells[subject]?.get(observer) == 44 }
                store.insert(card30382("8".repeat(64), rank = null))
                awaitTrue("score cell removed via rank-less supersession") {
                    mock.cells[subject]?.containsKey(observer) != true && mock.scoreIds[subject]?.containsKey(observer) != true
                }

                // NIP-62 vanish by the service key: sweeps its observer's cells + blanks its profile.
                store.insert(card30382("ab".repeat(32), 45))
                awaitTrue("score re-indexed for vanish") { mock.cells[subject]?.get(observer) == 45 }
                store.insert(RequestToVanishEvent("cd".repeat(32), service, next(), arrayOf(arrayOf("relay", "ALL_RELAYS")), "", ""))
                awaitTrue("observer cells swept via kind 62") {
                    mock.cells[subject]?.containsKey(observer) != true && mock.docs[service]?.get("name") == ""
                }
            } finally {
                scope.cancel()
                vespa.close()
                store.close()
                mock.stop()
            }
        }

    private fun awaitTrue(
        what: String,
        cond: () -> Boolean,
    ) {
        val deadline = System.currentTimeMillis() + 10_000
        while (System.currentTimeMillis() < deadline) {
            if (cond()) return
            Thread.sleep(25)
        }
        fail("timed out waiting for: $what")
    }
}
