package com.vitorpamplona.sot.cli

import com.vitorpamplona.sot.config.Config
import com.vitorpamplona.sot.query.SearchOptions
import com.vitorpamplona.sot.query.VespaSearch

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
    println("query=${query} algo=$algo observer=$obsLabel")

    val results =
        VespaSearch(vespaUrl).search(
            query, observer,
            SearchOptions(hits = hits, rankProfile = algo, includeZeroScore = !onlyRanked),
        )

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
