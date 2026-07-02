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
package com.vitorpamplona.sot.relay

import kotlinx.serialization.json.Json
import kotlinx.serialization.json.boolean
import kotlinx.serialization.json.int
import kotlinx.serialization.json.jsonArray
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

/** The NIP-11 wire contract: what a client learns before connecting. */
class RelayInfoTest {
    @Test
    fun `the NIP-11 document advertises search, optional auth, and no writes`() {
        val info = Json.parseToJsonElement(relayInfoJson()).jsonObject

        assertEquals("sot", info["name"]?.jsonPrimitive?.content)
        val nips = info["supported_nips"]!!.jsonArray.map { it.jsonPrimitive.int }
        assertTrue(nips.containsAll(listOf(1, 11, 42, 50)), "NIP-01/11/42/50 are the contract: $nips")

        val limitation = info["limitation"]!!.jsonObject
        assertEquals(false, limitation["auth_required"]?.jsonPrimitive?.boolean, "NIP-42 is optional")
        assertEquals(true, limitation["restricted_writes"]?.jsonPrimitive?.boolean, "search-only: no event writes")
        assertEquals(400, limitation["max_limit"]?.jsonPrimitive?.int)
    }
}
