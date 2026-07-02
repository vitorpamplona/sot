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
import com.vitorpamplona.quartz.nip62RequestToVanish.RequestToVanishEvent
import com.vitorpamplona.quartz.nip85TrustedAssertions.list.TrustProviderListEvent
import com.vitorpamplona.quartz.nip85TrustedAssertions.users.ContactCardEvent
import com.vitorpamplona.sot.vespa.MockVespa
import com.vitorpamplona.sot.vespa.VespaClient
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import java.io.File
import kotlin.test.AfterTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.fail

/**
 * Projection behaviors beyond the happy path: observer resolution when the
 * 10040 predates the subscription (the store fallback) or doesn't exist (the
 * unresolved counter), a NIP-62 sweep that needs more than one 400-subject
 * page, and a Vespa write outage that must not wedge the pending gauge.
 */
class ProjectionEdgeCasesTest {
    private val observer = "0".repeat(64)
    private val service = "2".repeat(64)

    private var now = 1_700_000_000L

    private fun next() = ++now

    private fun hex(
        prefix: Char,
        i: Int,
    ) = "$prefix${"%063x".format(i)}"

    private fun list10040() = TrustProviderListEvent("a".repeat(64), observer, next(), arrayOf(arrayOf("30382:rank", service, "wss://scores.example.com/")), "", "")

    private fun card30382(
        id: String,
        subject: String,
        author: String = service,
        rank: Int = 1,
    ) = ContactCardEvent(id, author, next(), arrayOf(arrayOf("d", subject), arrayOf("rank", rank.toString())), "", "")

    private val mock = MockVespa()
    private val db =
        File.createTempFile("projection-edges", ".db").also {
            it.delete()
            it.deleteOnExit()
        }
    private val store = ObservableEventStore(EventStore(db.path, RelayUrlNormalizer.normalize("ws://localhost:7777"), DefaultIndexingStrategy(indexFullTextSearch = false)))
    private val vespa = VespaClient("http://127.0.0.1:${mock.port}")
    private val projection = VespaProjection(store, vespa) { }
    private val scope = CoroutineScope(Dispatchers.IO + SupervisorJob())

    @AfterTest
    fun tearDown() {
        scope.cancel()
        vespa.close()
        store.close()
        mock.stop()
    }

    @Test
    fun `a 30382 that predates the projection's 10040 knowledge still resolves via the store`() =
        runBlocking {
            // The 10040 is already IN the store before the projection subscribes -
            // its Insert was never seen, so the in-memory map starts empty.
            store.insert(list10040())
            scope.launch { projection.run() }
            projection.awaitSubscribed()

            val subject = "3".repeat(64)
            store.insert(card30382("5".repeat(64), subject))

            awaitTrue("score resolved through the store fallback") { mock.cells[subject]?.get(observer) == 1 }
        }

    @Test
    fun `a 30382 from a service no 10040 delegates to is counted unresolved, not indexed`() =
        runBlocking {
            scope.launch { projection.run() }
            projection.awaitSubscribed()

            val unknownService = "9".repeat(64)
            val subject = "3".repeat(64)
            store.insert(card30382("6".repeat(64), subject, author = unknownService))

            awaitTrue("the card is processed") { projection.unresolved.get() == 1 }
            assertEquals(null, mock.cells[subject]?.get(observer), "no observer, no cell")
        }

    @Test
    fun `a vanish sweep erases more score cells than one 400-hit page returns`() =
        runBlocking {
            scope.launch { projection.run() }
            projection.awaitSubscribed()
            store.insert(list10040())

            val subjects = (1..450).map { hex('3', it) }
            store.batchInsert(subjects.mapIndexed { i, s -> card30382(hex('e', i + 1), s) })
            awaitTrue("all 450 cells indexed") { subjects.all { mock.cells[it]?.get(observer) == 1 } }

            // The service vanishes: every cell keyed by its observer must go, page by page.
            store.insert(RequestToVanishEvent("cd".repeat(32), service, next(), arrayOf(arrayOf("relay", "ALL_RELAYS")), "", ""))

            awaitTrue("every cell swept across multiple pages") { subjects.none { mock.cells[it]?.containsKey(observer) == true } }
        }

    @Test
    fun `a Vespa write outage neither wedges the pending gauge nor kills the projection`() =
        runBlocking {
            scope.launch { projection.run() }
            projection.awaitSubscribed()

            mock.failUpdates = true
            val alice = "1".repeat(64)
            store.insert(MetadataEvent("4".repeat(64), alice, next(), emptyArray(), """{"name":"alice"}""", ""))

            // The failed write's future must complete (retries exhausted) and drain.
            projection.awaitIdle(idleMs = 500, maxMs = 60_000)
            assertEquals(null, mock.docs[alice]?.get("name"), "the outage really dropped the write")

            // Vespa comes back; the projection is still alive and mirroring.
            mock.failUpdates = false
            val bob = "b".repeat(64)
            store.insert(MetadataEvent("5".repeat(64), bob, next(), emptyArray(), """{"name":"bob"}""", ""))
            awaitTrue("writes flow again after the outage") { mock.docs[bob]?.get("name") == "bob" }
        }

    private fun awaitTrue(
        what: String,
        cond: () -> Boolean,
    ) {
        val deadline = System.currentTimeMillis() + 20_000
        while (System.currentTimeMillis() < deadline) {
            if (cond()) return
            Thread.sleep(25)
        }
        fail("timed out waiting for: $what")
    }
}
