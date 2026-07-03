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
package com.vitorpamplona.sot.cli

import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue

class ArgsTest {
    @Test
    fun `flag returns the value after the name, or the default`() {
        val args = listOf("search", "--hits", "10", "--only-ranked")

        assertEquals("10", flag(args, "--hits", "50"))
        assertEquals("50", flag(args, "--max", "50"))
        assertEquals("x", flag(listOf("--hits"), "--hits", "x"), "a trailing value-less flag falls back to the default")
    }

    @Test
    fun `has detects boolean flags`() {
        assertTrue(has(listOf("index", "--reconcile"), "--reconcile"))
        assertFalse(has(listOf("index"), "--reconcile"))
    }

    @Test
    fun `positionalArgs skips flags and the values of valued flags`() {
        val args = listOf("vitor pamplona", "--hits", "10", "--only-ranked", "second")

        assertEquals(
            listOf("vitor pamplona", "second"),
            positionalArgs(args, valuedFlags = setOf("--hits")),
        )
    }

    @Test
    fun `an unregistered valued flag would swallow a positional - the valuedFlags set is the contract`() {
        // "--hits" not registered: its value "10" is (wrongly, by contract) a positional.
        assertEquals(
            listOf("10", "query"),
            positionalArgs(listOf("--hits", "10", "query"), valuedFlags = emptySet()),
        )
    }
}
