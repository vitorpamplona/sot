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
package com.vitorpamplona.sot.config

import java.io.File
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class ConfigTest {
    private fun dotenv(content: String): Map<String, String> {
        val f = File.createTempFile("config-test", ".env").also { it.deleteOnExit() }
        f.writeText(content)
        return Config.loadDotenv(f.path)
    }

    @Test
    fun `dotenv parsing - comments, export prefixes, quotes, blanks`() {
        val env =
            dotenv(
                """
                # a comment
                VESPA_URL=http://vespa:8080

                export SERVER_PORT=9999
                SERVER_NAME="quoted name"
                SERVER_ICON='single'
                NOT_A_PAIR
                =empty-key
                """.trimIndent(),
            )

        assertEquals("http://vespa:8080", env["VESPA_URL"])
        assertEquals("9999", env["SERVER_PORT"], "export prefix is stripped")
        assertEquals("quoted name", env["SERVER_NAME"])
        assertEquals("single", env["SERVER_ICON"])
        assertEquals(setOf("VESPA_URL", "SERVER_PORT", "SERVER_NAME", "SERVER_ICON"), env.keys)
    }

    @Test
    fun `a missing dotenv file resolves to no overrides`() {
        assertEquals(emptyMap(), Config.loadDotenv("/does/not/exist/.env"))
    }

    @Test
    fun `sampleDotenv round-trips - sot init output parses back to every default`() {
        val parsed = dotenv(Config.sampleDotenv())
        for ((key, default, _) in Config.KEYS) {
            assertEquals(default, parsed[key], "key $key")
        }
    }

    @Test
    fun `unset keys fall back to their registered defaults`() {
        // The test JVM defines none of these; nothing in a .env either (SOT_ENV unset).
        assertEquals("http://localhost:8080", Config.vespaUrl)
        assertEquals(7777, Config.serverPort)
        assertEquals(15, Config.syncIntervalMinutes)
        assertEquals("", Config.env("NO_SUCH_KEY"))
    }

    @Test
    fun `seedRelays splits and trims the comma-separated default`() {
        val seeds = Config.seedRelays
        assertEquals(7, seeds.size)
        assertTrue(seeds.all { it.startsWith("wss://") && it == it.trim() }, "$seeds")
    }
}
