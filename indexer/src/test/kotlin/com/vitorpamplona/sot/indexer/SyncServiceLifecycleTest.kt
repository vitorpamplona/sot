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
import com.vitorpamplona.sot.vespa.MockVespa
import kotlinx.coroutines.runBlocking
import java.io.File
import kotlin.test.Test
import kotlin.test.fail
import kotlin.time.Duration.Companion.seconds

/**
 * [SyncService] lifecycle without a network: a pass over zero relays completes,
 * the projection subscription stays live BETWEEN passes (an event inserted into
 * the shared store by any other path still reaches Vespa), and drain/close shut
 * down cleanly.
 */
class SyncServiceLifecycleTest {
    @Test
    fun `projection lives across the service lifetime and drains cleanly`() =
        runBlocking {
            val mock = MockVespa()
            val db =
                File.createTempFile("sync-service", ".db").also {
                    it.delete()
                    it.deleteOnExit()
                }
            val store = ObservableEventStore(EventStore(db.path, RelayUrlNormalizer.normalize("ws://localhost:7777"), DefaultIndexingStrategy(indexFullTextSearch = false)))
            val service =
                SyncService(
                    store = store,
                    vespaUrl = "http://127.0.0.1:${mock.port}",
                    seedRelays = emptyList(),
                    statePath = "${db.path}.state.json",
                    opts = SyncOptions(),
                    log = { },
                )

            try {
                service.runOnce() // zero relays: every phase no-ops, no network

                // Not synced — inserted directly, as the relay (or a live
                // subscription) would; the standing projection must mirror it.
                val alice = "1".repeat(64)
                store.insert(MetadataEvent("4".repeat(64), alice, 1_700_000_000L, emptyArray(), """{"name":"alice"}""", ""))
                awaitTrue("event inserted between passes reaches Vespa") { mock.docs[alice]?.get("name") == "alice" }

                service.drain(10.seconds)
            } finally {
                service.close()
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
