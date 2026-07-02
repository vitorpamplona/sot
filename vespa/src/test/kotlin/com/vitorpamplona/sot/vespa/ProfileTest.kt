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

import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class ProfileTest {
    @Test
    fun `indexFields covers every mutable schema field, including provenance`() {
        val fields = Profile("pk", name = "n", displayName = "dn", eventId = "e").indexFields()

        assertEquals(
            listOf("name", "display_name", "about", "picture", "banner", "nip05", "lud06", "lud16", "website", "event_id"),
            fields.keys.toList(),
            "field order/coverage changed - update the Vespa schema and this test together",
        )
        assertEquals("n", fields["name"])
        assertEquals("dn", fields["display_name"])
        assertEquals("e", fields["event_id"])
        assertTrue("pubkey" !in fields, "pubkey is the doc id, not a mutable field")
    }

    @Test
    fun `a bare Profile maps every field to null - the blanking upsert`() {
        assertTrue(Profile("pk").indexFields().values.all { it == null })
    }
}
