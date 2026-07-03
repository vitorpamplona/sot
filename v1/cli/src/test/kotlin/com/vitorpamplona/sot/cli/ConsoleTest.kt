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

import com.vitorpamplona.sot.vespa.SearchHit
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue

/**
 * The console styling is a presentation layer, so its one hard promise is what it
 * does with colour OFF: since tests run without a TTY, [Ansi.enabled] is false —
 * exactly the piped/redirected/NO_COLOR case. These assertions pin the plain path
 * (the colour path is escape codes over the same text). If a future change flips
 * colour on unexpectedly, the ESC assertions here fail loudly.
 */
class ConsoleTest {
    private val esc = '\u001b'

    private fun hit(
        name: String,
        trust: Double?,
        rel: Double?,
        nip05: String = "",
    ) = SearchHit("deadbeef", rel, trust, mapOf("display_name" to name, "nip05" to nip05))

    @Test
    fun `colour is off without a terminal - the piped contract`() {
        assertFalse(Ansi.enabled, "no TTY under test: output must be plain")
    }

    @Test
    fun `styleLogLine is identity when colour is off - sync logs stay byte-for-byte`() {
        for (
        line in
        listOf(
            "=== phase 1: 10040 / 5 / 0 from 127 relay(s) ===",
            "[42/127] relay.damus.io  kind0=+234  (1s)",
            "  ~ relays 42/127 | recv 1.2M new 987k",
            "  ! purplepag.es pages sync timed out",
            "DONE - profiles=48210 scores=1240381",
            "next sync pass in 30m",
        )
        ) {
            assertEquals(line, styleLogLine(line))
        }
    }

    @Test
    fun `search render is plain, aligned, and free of escape codes`() {
        val out = renderResults("vitor", "algo_x", "npub1..", listOf(hit("Vitor Pamplona", 920.0, 1.0, "vitor@v.com"), hit("Jack", 610.0, 0.83)))
        assertFalse(out.contains(esc), "no ANSI escapes when colour is off")
        assertTrue("name / display" in out && "trust" in out, "header present")
        assertTrue("Vitor Pamplona" in out && "vitor@v.com" in out, "rows present")
        // Data rows only: 5 leading spaces, the rank, then its 2-space pad (the
        // "N results" footer has a single space after its number, so it's excluded).
        val rows = out.lines().filter { Regex("^ {5}\\d+ {2}").containsMatchIn(it) }
        assertEquals(2, rows.size)
        // A row with a nip05 must be at least as wide as the header's start-of-nip05, i.e. columns didn't collapse.
        assertTrue(rows.all { it.indexOf("@") == -1 || it.indexOf("@") > 40 }, "nip05 sits in its column, not jammed left: $rows")
    }

    @Test
    fun `search render handles the empty and singular cases`() {
        assertTrue("no matches" in renderResults("zzz", "a", "o", emptyList()))
        val one = renderResults("solo", "a", "o", listOf(hit("Solo", 5.0, 0.9)))
        assertTrue("1 result\n" in one && "1 results" !in one, "singular result label")
    }

    @Test
    fun `null trust renders as a dash, not a crash`() {
        val out = renderResults("q", "a", "o", listOf(hit("No Trust", null, 0.2)))
        assertTrue("No Trust" in out)
        assertTrue(Regex("No Trust\\s+-\\s").containsMatchIn(out), "null trust shows a dash: $out")
    }

    @Test
    fun `warn and err fall back to a greppable bang when colour is off`() {
        assertEquals("! nope", warnGlyph() + "nope")
    }
}
