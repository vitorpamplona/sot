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

import com.vitorpamplona.quartz.nip01Core.relay.normalizer.RelayUrlNormalizer
import java.io.File
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNull
import kotlin.test.assertTrue

class SyncStateTest {
    private val relay = RelayUrlNormalizer.normalize("wss://relay.example.com")
    private val other = RelayUrlNormalizer.normalize("wss://other.example.com")

    @Test
    fun `cursors are scoped per relay and per scope key`() {
        val state = SyncState()
        state.markSynced(relay, "0", 111)
        state.markSynced(relay, "30382:abc", 222)

        assertEquals(111, state.cursor(relay, "0"))
        assertEquals(222, state.cursor(relay, "30382:abc"))
        assertNull(state.cursor(relay, "30382:def"), "one provider's cursor must not since-filter another")
        assertNull(state.cursor(other, "0"), "cursors never leak across relays")
    }

    @Test
    fun `save and load round-trip cursors, capability, and the relay pool`() {
        val path = File.createTempFile("sync-state", ".json").also { it.deleteOnExit() }.path
        val state = SyncState()
        state.markSynced(relay, "0", 123)
        state.relay(relay).negentropyCapable = false
        state.relayPool.add(other)
        SyncState.save(path, state)

        val loaded = SyncState.load(path)
        assertEquals(123, loaded.cursor(relay, "0"))
        assertEquals(false, loaded.relay(relay).negentropyCapable)
        assertTrue(other in loaded.relayPool)
        // The wire format stays plain url strings (typed urls serialize through
        // RelayUrlSerializer), so state files from before the typed keys still load.
        assertTrue(relay.url in File(path).readText())
    }

    @Test
    fun `a missing or corrupt state file starts fresh instead of failing the sync`() {
        assertEquals(SyncState(), SyncState.load("/does/not/exist.json"))

        val corrupt = File.createTempFile("sync-state", ".json").also { it.deleteOnExit() }
        corrupt.writeText("not json {")
        assertEquals(SyncState(), SyncState.load(corrupt.path))
    }
}
