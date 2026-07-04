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

import com.vitorpamplona.quartz.nip01Core.metadata.MetadataEvent
import com.vitorpamplona.quartz.nip09Deletions.DeletionEvent
import com.vitorpamplona.quartz.nip10Notes.TextNoteEvent
import com.vitorpamplona.quartz.nip23LongContent.LongTextNoteEvent
import kotlin.test.Test
import kotlin.test.assertFalse
import kotlin.test.assertTrue

/**
 * The records plane must sync EXACTLY what the store can index — the
 * `SearchableEvent` kinds — no more, no less. This guards the registry
 * derivation against a Quartz upgrade that reshuffles the factory.
 */
class IndexableKindsTest {
    @Test
    fun `the searchable staples are present`() {
        val kinds = IndexableKinds.kinds
        assertTrue(TextNoteEvent.KIND in kinds, "kind 1 (notes) is searchable")
        assertTrue(MetadataEvent.KIND in kinds, "kind 0 (profiles) is searchable")
        assertTrue(LongTextNoteEvent.KIND in kinds, "kind 30023 (long-form) is searchable")
        assertTrue(kinds.size > 20, "the searchable set is broad, not a handful (${kinds.size})")
    }

    @Test
    fun `non-searchable control kinds are excluded`() {
        // Deletions are interpreted, not indexed; the records plane adds them separately.
        assertFalse(DeletionEvent.KIND in IndexableKinds.kinds, "kind 5 (deletion) is not searchable content")
    }
}
