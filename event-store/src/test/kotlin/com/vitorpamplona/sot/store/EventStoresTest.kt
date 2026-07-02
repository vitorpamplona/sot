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
package com.vitorpamplona.sot.store

import com.vitorpamplona.quartz.nip01Core.core.Event
import com.vitorpamplona.quartz.nip01Core.metadata.MetadataEvent
import com.vitorpamplona.quartz.nip01Core.relay.filters.Filter
import kotlinx.coroutines.runBlocking
import java.io.File
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

/**
 * The one opener every component must use: the store it returns carries the
 * relay identity from Config (NIP-62 vanish scoping depends on it) and accepts
 * plain inserts/queries end to end on a real SQLite file.
 */
class EventStoresTest {
    private fun tempDb(): String =
        File
            .createTempFile("event-stores", ".db")
            .also {
                it.delete()
                it.deleteOnExit()
            }.path

    @Test
    fun `openObservableStore carries the configured relay identity`() {
        val store = openObservableStore(tempDb())
        try {
            assertEquals(relayIdentity(), store.relay, "a null/foreign relay silently breaks NIP-62 vanish")
            assertTrue(
                store.relay
                    ?.url
                    .orEmpty()
                    .contains("localhost:7777"),
                "RELAY_URL default: ${store.relay?.url}",
            )
        } finally {
            store.close()
        }
    }

    @Test
    fun `insert and query round-trip`() =
        runBlocking {
            val store = openObservableStore(tempDb())
            try {
                val alice = "1".repeat(64)
                store.insert(MetadataEvent("4".repeat(64), alice, 1_700_000_000L, emptyArray(), """{"name":"alice"}""", ""))

                val found = store.query<Event>(Filter(kinds = listOf(MetadataEvent.KIND), authors = listOf(alice)))
                assertEquals(1, found.size)
                assertEquals(alice, found.single().pubKey)
            } finally {
                store.close()
            }
        }
}
