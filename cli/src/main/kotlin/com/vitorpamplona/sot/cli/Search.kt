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
import com.vitorpamplona.sot.vespa.SearchHit
import com.vitorpamplona.sot.vespa.SearchOptions
import com.vitorpamplona.sot.vespa.VespaSearch

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

    val results =
        VespaSearch(vespaUrl).search(
            query,
            observer,
            SearchOptions(hits = hits, rankProfile = algo, includeZeroScore = !onlyRanked),
        )

    print(renderResults(query, algo, obsLabel, results))
}

/**
 * The whole `sot search` render as one string (banner + table + footer), pure so
 * it can be eyeballed and tested without a live Vespa. A little banner up top —
 * the query in lights, the knobs dimmed under it — then the ranked table.
 */
internal fun renderResults(
    query: String,
    algo: String,
    obsLabel: String,
    results: List<SearchHit>,
): String =
    buildString {
        appendLine()
        appendLine("  ${Ansi.cyan("⌕")}  ${Ansi.bold(query)}")
        appendLine("     ${Ansi.dim("algo $algo · observer $obsLabel")}")

        if (results.isEmpty()) {
            appendLine("     ${Ansi.dim("no matches")}")
            appendLine()
            return@buildString
        }

        // Trust bars scale to the strongest hit in THIS result set, so the column
        // reads as a comparison even when the absolute scores are tiny.
        val maxTrust = results.mapNotNull { it.trust }.maxOrNull()?.takeIf { it > 0 } ?: 1.0

        appendLine()
        appendLine(Ansi.dim("     %-3s  %-30s  %-8s  %-9s  %s".format("#", "name / display", "trust", "relevance", "nip05")))
        appendLine(Ansi.gray("     " + "─".repeat(72)))
        results.forEachIndexed { i, h ->
            val rank = Ansi.gray("%-3d".format(i + 1))
            val label = Ansi.bold((h.displayName.ifBlank { h.name }).take(30).padEnd(30))
            val trust = trustCell(h.trust, maxTrust)
            val rel = Ansi.dim((h.relevance?.let { "%.2f".format(it) } ?: "-").padEnd(9))
            val nip05 = Ansi.blue(h.fields["nip05"] ?: "")
            appendLine("     $rank  $label  $trust  $rel  $nip05")
        }
        appendLine()
        appendLine(Ansi.dim("     ${results.size} result${if (results.size == 1) "" else "s"}"))
        appendLine()
    }

/** A `▁▂▃▄▅` bar + the number, coloured by strength, padded to the column's plain width. */
private fun trustCell(
    trust: Double?,
    max: Double,
): String {
    if (trust == null) return Ansi.dim("-".padEnd(8))
    val bars = "▁▁▂▃▄▅▆▇█"
    val ratio = (trust / max).coerceIn(0.0, 1.0)
    val bar = bars[(ratio * (bars.length - 1)).toInt()].toString().repeat(3)
    val num = "%.0f".format(trust)
    val plain = "$bar $num" // colour is invisible width; pad the plain form first
    val painted =
        when {
            ratio >= 0.66 -> Ansi.green(plain)
            ratio >= 0.20 -> Ansi.cyan(plain)
            else -> Ansi.dim(plain)
        }
    // pad to 8 visible columns by appending spaces AFTER the reset
    return painted + " ".repeat((8 - plain.length).coerceAtLeast(0))
}
