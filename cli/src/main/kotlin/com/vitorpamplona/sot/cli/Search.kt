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

import com.vitorpamplona.sot.config.Config
import com.vitorpamplona.sot.vespa.SearchOptions
import com.vitorpamplona.sot.vespa.VespaSearch
import kotlinx.coroutines.runBlocking

private val SEARCH_VALUED_FLAGS = setOf("--observer", "--hits", "--algo", "--vespa")

/**
 * `sot search "<query>"` — rank profiles in Vespa via :query-engine and print
 * them. Reuses the exact search core the http service and relay use, so the
 * terminal output matches production.
 */
internal fun search(args: List<String>) {
    val query = positionalArgs(args, SEARCH_VALUED_FLAGS).firstOrNull()
    if (query == null) {
        println("usage: search \"<query>\" [--observer <hex|npub|nprofile|nip05>] [--hits N] [--algo <profile>] [--only-ranked]")
        return
    }
    val observer = observerOrWarn(args)
    val hits = flag(args, "--hits", "20").toInt()
    val algo = flag(args, "--algo", "name_and_quality_score_only")
    val vespaUrl = flag(args, "--vespa", Config.vespaUrl)
    val onlyRanked = has(args, "--only-ranked")

    val obsLabel = if (observer.isEmpty()) "(none)" else observer.take(12) + ".."
    println("query=$query algo=$algo observer=$obsLabel")

    val results =
        runBlocking {
            VespaSearch(vespaUrl).search(
                query,
                observer,
                SearchOptions(hits = hits, rankProfile = algo, includeZeroScore = !onlyRanked),
            )
        }

    println("results=${results.size}")
    println("-".repeat(92))
    println("%2s  %11s  %6s  %-30s %s".format("#", "relevance", "trust", "name / display_name", "nip05"))
    println("-".repeat(92))
    results.forEachIndexed { i, h ->
        val label = (h.displayName.ifBlank { h.name }).take(30)
        val rel = h.relevance?.let { "%.2f".format(it) } ?: "-"
        val tr = h.trust?.let { "%.0f".format(it) } ?: "-"
        println("%2d  %11s  %6s  %-30s %s".format(i + 1, rel, tr, label, h.fields["nip05"] ?: ""))
    }
}
