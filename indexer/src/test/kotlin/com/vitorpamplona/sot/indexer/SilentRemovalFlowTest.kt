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

import com.vitorpamplona.quartz.nip01Core.relay.filters.Filter
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.RelayUrlNormalizer
import com.vitorpamplona.quartz.nip01Core.store.ObservableEventStore
import com.vitorpamplona.quartz.nip01Core.store.sqlite.DefaultIndexingStrategy
import com.vitorpamplona.quartz.nip01Core.store.sqlite.EventStore
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
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.fail

/**
 * The fourth deletion mechanism, END TO END: a provider silently wipes a score
 * from its relay (no kind:5, no supersession, no vanish — just gone), and a
 * full [runSync] pass must notice via the reconcile diff, delete the event from
 * the local store, and let the change feed erase the score cell from Vespa.
 * Everything is real except the network: an in-process relay and a mock Vespa.
 */
class SilentRemovalFlowTest {
    private val observer = "0".repeat(64)
    private val service = "2".repeat(64)
    private val subjectKept = "3".repeat(64)
    private val subjectWiped = "4".repeat(64)
    private val idKept = "5".repeat(64)
    private val idWiped = "6".repeat(64)

    private var now = 1_700_000_000L

    private fun next() = ++now

    private fun list10040() = TrustProviderListEvent("a".repeat(64), observer, next(), arrayOf(arrayOf("30382:rank", service, "wss://in-process.test")), "", "")

    private fun card30382(
        id: String,
        subject: String,
        rank: Int,
    ) = ContactCardEvent(id, service, next(), arrayOf(arrayOf("d", subject), arrayOf("rank", rank.toString())), "", "")

    @Test
    fun `a silently wiped provider score disappears from the store and from Vespa`() =
        runBlocking {
            val relay = InProcessRelay()
            val mock = MockVespa()
            val db =
                File.createTempFile("silent-removal", ".db").also {
                    it.delete()
                    it.deleteOnExit()
                }
            val store = ObservableEventStore(EventStore(db.path, RelayUrlNormalizer.normalize("ws://localhost:7777"), DefaultIndexingStrategy(indexFullTextSearch = false)))
            val vespa = VespaClient("http://127.0.0.1:${mock.port}")
            val projection = VespaProjection(store, vespa) { }
            val scope = CoroutineScope(Dispatchers.IO + SupervisorJob())
            scope.launch { projection.run() }
            projection.awaitSubscribed()

            val statePath = File.createTempFile("silent-removal", ".state.json").also { it.deleteOnExit() }.path
            val state = SyncState.load(statePath)
            val opts = SyncOptions(discover = false, verifyEvents = false, fetchTimeoutMs = 15_000)

            try {
                // The provider publishes two scores; a full pass lands both in Vespa.
                relay.store.batchInsert(listOf(list10040(), card30382(idKept, subjectKept, 1), card30382(idWiped, subjectWiped, 2)))
                runSync(relay.client, store, state, statePath, listOf(relay.url), opts) { }
                awaitTrue("both scores indexed") {
                    mock.cells[subjectKept]?.get(observer) == 1 && mock.cells[subjectWiped]?.get(observer) == 2
                }

                // The provider wipes one row from its relay. No event, no trace.
                relay.store.delete(Filter(ids = listOf(idWiped)))

                runSync(relay.client, store, state, statePath, listOf(relay.url), opts) { }

                awaitTrue("the wiped score is erased from Vespa") {
                    mock.cells[subjectWiped]?.containsKey(observer) != true && mock.scoreIds[subjectWiped]?.containsKey(observer) != true
                }
                assertEquals(1, mock.cells[subjectKept]?.get(observer), "the surviving score is untouched")
                assertEquals(
                    listOf(idKept),
                    store.query<ContactCardEvent>(Filter(kinds = listOf(ContactCardEvent.KIND))).map { it.id },
                    "the source of truth dropped the wiped event",
                )
            } finally {
                scope.cancel()
                vespa.close()
                store.close()
                relay.close()
                mock.stop()
            }
        }

    private fun awaitTrue(
        what: String,
        cond: () -> Boolean,
    ) {
        val deadline = System.currentTimeMillis() + 15_000
        while (System.currentTimeMillis() < deadline) {
            if (cond()) return
            Thread.sleep(25)
        }
        fail("timed out waiting for: $what")
    }
}
