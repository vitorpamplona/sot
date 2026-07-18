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

import com.vitorpamplona.sot.vespa.InMemoryCrawlIndex
import kotlinx.coroutines.runBlocking
import kotlin.test.Test
import kotlin.test.assertEquals

/** The crawl ledger's refresh selection: stalest-first, budget-capped, never-synced excluded. */
class CrawlLedgerTest {
    @Test
    fun `dueForRefresh returns the stalest synced authors first, capped and excluding never-synced`() =
        runBlocking {
            val crawl = InMemoryCrawlIndex()
            crawl.markSynced(listOf("aa"), atSecs = 100) // stalest
            crawl.markSynced(listOf("bb"), atSecs = 200)
            crawl.markSynced(listOf("cc"), atSecs = 300)
            crawl.markSynced(listOf("dd"), atSecs = 5_000) // fresh, past the cutoff
            crawl.markOutboxChecked(listOf("ee"), atSecs = 200) // routed but never synced

            // Cutoff excludes dd (too fresh) and ee (never synced); budget of 2 keeps the two stalest.
            val due = crawl.dueForRefresh(cutoffSecs = 1_000, limit = 2)
            assertEquals(listOf("aa", "bb"), due, "stalest-first, capped at the budget")

            assertEquals(listOf("aa", "bb", "cc"), crawl.dueForRefresh(cutoffSecs = 1_000, limit = 10), "all due, still stalest-first")
            assertEquals(emptyList(), crawl.dueForRefresh(cutoffSecs = 1_000, limit = 0), "a zero budget disables the slice")
        }
}
