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

import java.io.File
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class ConfigTest {
    @Test
    fun `dotenv parsing strips exports, quotes, and comments`() {
        val f = File.createTempFile("sot-config", ".env")
        f.writeText(
            """
            # a comment
            export SERVER_NAME="quoted name"
            SERVER_PORT=1234

            HOUSE_NPUB='single'
            not-a-kv-line
            """.trimIndent(),
        )
        val env = Config.loadDotenv(f.path)
        assertEquals("quoted name", env["SERVER_NAME"])
        assertEquals("1234", env["SERVER_PORT"])
        assertEquals("single", env["HOUSE_NPUB"])
        assertEquals(3, env.size)
    }

    @Test
    fun `renderDotenv carries answers and keeps every key documented`() {
        val text = Config.renderDotenv(mapOf("SERVER_NAME" to "custom"))
        assertTrue("SERVER_NAME=custom" in text)
        for ((key, _, doc) in Config.KEYS) {
            assertTrue("\n$key=" in "\n$text", "key $key present")
            assertTrue(doc in text, "doc for $key present")
        }
        // The parser reads back what the renderer wrote.
        val f = File.createTempFile("sot-config", ".env").apply { writeText(text) }
        assertEquals("custom", Config.loadDotenv(f.path)["SERVER_NAME"])
    }
}
