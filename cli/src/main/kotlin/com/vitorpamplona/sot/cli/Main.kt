package com.vitorpamplona.sot.cli

import com.vitorpamplona.sot.query.SearchOptions
import com.vitorpamplona.sot.query.VespaSearch
import kotlinx.serialization.Serializable
import kotlinx.serialization.json.Json
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.time.Duration

/**
 * sot CLI: install/run the local components, check their health, and
 * search from the terminal. Search reuses :query-engine, so terminal results
 * match http-api and the relay exactly.
 *
 *   sot search "vitor" [--observer <hex>] [--hits N] [--rank P] [--only-ranked]
 *   sot compare "vitor" "alex" [--topn 5] [--variants variants.json]
 *   sot status
 *   sot up | down            # docker compose for local Vespa
 *   sot deploy               # (re)deploy vespa to the config server
 */
/**
 * Default web-of-trust observer (hex pubkey) when `--observer` is omitted, taken
 * from the `DEFAULT_OBSERVER` env var. Unset means no default: text results
 * still come back, but every `user_score` is 0 (no trust perspective applied).
 */
private val DEFAULT_OBSERVER = System.getenv("DEFAULT_OBSERVER").orEmpty()

private fun flag(args: List<String>, name: String, default: String): String {
    val i = args.indexOf(name)
    return if (i >= 0 && i + 1 < args.size) args[i + 1] else default
}

private fun has(args: List<String>, name: String) = args.contains(name)

/** The `--observer` flag, else DEFAULT_OBSERVER; warns (once) if neither is set. */
private fun observerOrWarn(args: List<String>): String {
    val observer = flag(args, "--observer", DEFAULT_OBSERVER)
    if (observer.isBlank()) {
        System.err.println(
            "note: no observer set - pass --observer <hex> or set DEFAULT_OBSERVER; " +
                "results are not trust-ranked (every score is 0).",
        )
    }
    return observer
}

fun main(argv: Array<String>) {
    val args = argv.toList()
    when (args.firstOrNull()) {
        "search" -> search(args.drop(1))
        "compare" -> compare(args.drop(1))
        "status" -> status(args.drop(1))
        "up" -> up(args.drop(1))
        "down" -> down()
        "deploy" -> deploy(args.drop(1))
        else -> usage()
    }
}

private fun usage() {
    println(
        """
        sot — local Nostr profile search

          search "<query>" [--observer <hex>] [--hits N] [--rank <profile>] [--only-ranked] [--vespa <url>]
          compare "<query>" [<query> ...] [--queries <file>] [--variants <json>] [--observer <hex>] [--topn N] [--vespa <url>]
          status  [--vespa <url>] [--api <url>] [--relay <url>]
          up                 start local Vespa (docker compose) and deploy vespa
          down               stop local Vespa
          deploy [--app <dir>] [--config <host:port>]   redeploy the Vespa app package
        """.trimIndent(),
    )
}

private fun search(args: List<String>) {
    val query = args.firstOrNull { !it.startsWith("--") }
    if (query == null) {
        println("usage: search \"<query>\" [--observer <hex>] [--hits N] ...")
        return
    }
    val observer = observerOrWarn(args)
    val hits = flag(args, "--hits", "20").toInt()
    val rank = flag(args, "--rank", "name_and_quality_score_only")
    val vespaUrl = flag(args, "--vespa", System.getenv("VESPA_URL") ?: "http://localhost:8080")
    val onlyRanked = has(args, "--only-ranked")

    val results =
        VespaSearch(vespaUrl).search(
            query, observer,
            SearchOptions(hits = hits, rankProfile = rank, includeZeroScore = !onlyRanked),
        )

    println("query=${query} ranking=$rank observer=${observer.take(12)}.. results=${results.size}")
    println("-".repeat(92))
    println("%2s  %11s  %6s  %-30s %s".format("#", "relevance", "score", "name / display_name", "nip05"))
    println("-".repeat(92))
    results.forEachIndexed { i, h ->
        val label = (h.displayName.ifBlank { h.name }).take(30)
        val rel = h.relevance?.let { "%.2f".format(it) } ?: "-"
        val sc = h.userScore?.let { "%.0f".format(it) } ?: "-"
        println("%2d  %11s  %6s  %-30s %s".format(i + 1, rel, sc, label, h.fields["nip05"] ?: ""))
    }
}

/** A named ranking variant: a rank-profile plus `query(...)` feature overrides. */
@Serializable
private data class Variant(
    val ranking: String = "name_and_quality_score_only",
    val features: Map<String, Double> = emptyMap(),
)

// Starter panel: production default vs. a few weight tweaks. Override with a
// --variants JSON file of the same shape: {"name": {"ranking":..., "features":{...}}}.
private val DEFAULT_VARIANTS: Map<String, Variant> =
    linkedMapOf(
        "prod" to Variant("name_and_quality_score_only", mapOf("w_gram" to 5.0, "w_about" to 0.5)),
        "more_gram" to Variant("name_and_quality_score_only", mapOf("w_gram" to 15.0, "w_about" to 0.5)),
        "less_about" to Variant("name_and_quality_score_only", mapOf("w_gram" to 5.0, "w_about" to 0.1)),
        "search_rank" to Variant("search_rank", emptyMap()),
    )

/**
 * A/B compare ranking variants over one or more queries: run each query through
 * every variant and print the top-N side by side, so an equation or weight
 * change's reordering is visible at a glance. This is the experiment loop —
 * edit doc.sd (and redeploy) or a variant's features, then eyeball the deltas.
 */
private fun compare(args: List<String>) {
    val observer = observerOrWarn(args)
    val topn = flag(args, "--topn", "5").toInt()
    val vespaUrl = flag(args, "--vespa", System.getenv("VESPA_URL") ?: "http://localhost:8080")

    val flagsWithValue = setOf("--observer", "--topn", "--vespa", "--queries", "--variants")
    val queries = ArrayList<String>()
    var i = 0
    while (i < args.size) {
        val a = args[i]
        when {
            a in flagsWithValue -> i++ // skip this flag's value
            a.startsWith("--") -> {} // unknown bare flag, ignore
            else -> queries.add(a)
        }
        i++
    }
    flag(args, "--queries", "").takeIf { it.isNotEmpty() }?.let { path ->
        java.io.File(path).readLines()
            .map { it.trim() }
            .filter { it.isNotEmpty() && !it.startsWith("#") }
            .forEach { queries.add(it) }
    }
    if (queries.isEmpty()) {
        println("usage: compare \"<query>\" [<query> ...] [--queries <file>] [--variants <json>] [--topn N]")
        return
    }

    val variants: Map<String, Variant> =
        flag(args, "--variants", "").takeIf { it.isNotEmpty() }?.let { path ->
            Json { ignoreUnknownKeys = true }.decodeFromString<LinkedHashMap<String, Variant>>(
                java.io.File(path).readText(),
            )
        } ?: DEFAULT_VARIANTS

    val vespa = VespaSearch(vespaUrl)
    val names = variants.keys.toList()
    val width = 34
    for (q in queries) {
        println("=".repeat(100))
        println("QUERY: \"$q\"   (observer ${observer.take(12)}..)")
        println("=".repeat(100))
        val columns =
            names.associateWith { name ->
                val v = variants.getValue(name)
                vespa.search(q, observer, SearchOptions(hits = topn, rankProfile = v.ranking, features = v.features))
                    .take(topn)
                    .map { h ->
                        val label = (h.displayName.ifBlank { h.name }.ifBlank { "?" }).take(22)
                        val sc = h.userScore?.let { "%.0f".format(it) } ?: "-"
                        "$label (q=$sc)"
                    }
            }
        print("%-6s".format("rank"))
        names.forEach { print("%-${width}s".format(it.take(width - 2))) }
        println()
        for (r in 0 until topn) {
            print("%-6d".format(r + 1))
            names.forEach { n -> print("%-${width}s".format((columns[n]?.getOrNull(r) ?: "").take(width - 2))) }
            println()
        }
        println()
    }
}

private val http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(3)).build()

private fun ping(url: String, accept: String? = null): Boolean =
    runCatching {
        val b = HttpRequest.newBuilder(URI.create(url)).timeout(Duration.ofSeconds(3)).GET()
        if (accept != null) b.header("Accept", accept)
        http.send(b.build(), HttpResponse.BodyHandlers.ofString()).statusCode() < 400
    }.getOrDefault(false)

private fun status(args: List<String>) {
    val vespa = flag(args, "--vespa", "http://localhost:8080")
    val api = flag(args, "--api", "http://localhost:8081")
    val relay = flag(args, "--relay", "http://localhost:7777")
    fun line(name: String, ok: Boolean) = println("  ${if (ok) "[ UP ]" else "[DOWN]"}  $name")
    println("component status:")
    line("Vespa        ($vespa)", ping("$vespa/ApplicationStatus"))
    line("http-api     ($api)", ping("$api/search?text=_"))
    line("relay        ($relay)", ping("$relay/", accept = "application/nostr+json"))
}

private fun run(vararg cmd: String): Int {
    println("$ ${cmd.joinToString(" ")}")
    return ProcessBuilder(*cmd).inheritIO().start().waitFor()
}

private fun up(args: List<String>) {
    if (run("docker", "compose", "up", "-d", "vespa") == 0) {
        println("waiting for Vespa config server, then deploying vespa…")
        run("docker", "compose", "up", "vespa-deploy")
    }
}

private fun down() {
    run("docker", "compose", "down")
}

private fun deploy(args: List<String>) {
    val app = flag(args, "--app", "vespa")
    val config = flag(args, "--config", "localhost:19071")
    val tgz = "/tmp/vespa.tgz"
    if (run("bash", "-c", "tar -czf $tgz -C '$app' .") != 0) return
    run(
        "bash", "-c",
        "curl -fSs --data-binary @$tgz -H 'Content-Type: application/x-gzip' " +
            "http://$config/application/v2/tenant/default/prepareandactivate",
    )
}
