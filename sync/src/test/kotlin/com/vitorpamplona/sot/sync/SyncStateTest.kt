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
package com.vitorpamplona.sot.sync

import com.vitorpamplona.quartz.nip01Core.relay.normalizer.RelayUrlNormalizer
import java.io.File
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNull
import kotlin.test.assertTrue

class SyncStateTest {
    private fun tempPath(): String = File.createTempFile("sync-state", ".json").apply { deleteOnExit() }.absolutePath

    /** The last-pass summary survives a save/load round-trip, alongside the relay cursors. */
    @Test
    fun `records and reloads the last pass summary`() {
        val path = tempPath()
        val state = SyncState()
        state.markSynced(RelayUrlNormalizer.normalize("wss://relay.test"), scope = "1", atSecs = 1_700_000_000)
        state.recordPass(endedAtSecs = 1_700_000_100, durationSecs = 42, received = 5_000, inserted = 3_200)
        SyncState.save(path, state)

        val loaded = SyncState.load(path)
        val pass = loaded.lastPass!!
        assertEquals(1_700_000_100, pass.endedAtSecs)
        assertEquals(42, pass.durationSecs)
        assertEquals(5_000, pass.received)
        assertEquals(3_200, pass.inserted)
        assertEquals(1_700_000_000, loaded.cursor(RelayUrlNormalizer.normalize("wss://relay.test"), "1"))
    }

    /** A fresh state (or a pre-lastPass file) reads back with a null summary, not a crash. */
    @Test
    fun `absent last pass reads back null`() {
        val path = tempPath()
        SyncState.save(path, SyncState())
        assertNull(SyncState.load(path).lastPass)

        // An older file with no lastPass key still loads (ignoreUnknownKeys tolerance both ways).
        File(path).writeText("""{"relays":{}}""")
        assertNull(SyncState.load(path).lastPass)
    }

    /** recordPass overwrites: only the most recent pass is kept. */
    @Test
    fun `recordPass keeps only the latest`() {
        val state = SyncState()
        state.recordPass(endedAtSecs = 1, durationSecs = 1, received = 1, inserted = 1)
        state.recordPass(endedAtSecs = 2, durationSecs = 9, received = 99, inserted = 88)
        assertEquals(2, state.lastPass!!.endedAtSecs)
        assertTrue(state.lastPass!!.inserted == 88L)
    }
}
