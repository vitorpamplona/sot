package com.vitorpamplona.sot.cli

import com.vitorpamplona.sot.query.SearchOptions
import com.vitorpamplona.sot.query.VespaSearch
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.time.Duration

/**
 * sot CLI: install/run the local components, check their health, and
 * search from the terminal. Search reuses :query-engine, so terminal results
 * match the http service and the relay exactly.
 *
 *   sot search "vitor" [--observer <hex>] [--hits N] [--rank P] [--only-ranked]
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

/** Non-flag args, skipping each valued flag's value so it's never mistaken for a positional. */
private fun positionalArgs(args: List<String>, valuedFlags: Set<String>): List<String> {
    val out = ArrayList<String>()
    var i = 0
    while (i < args.size) {
        val a = args[i]
        when {
            a in valuedFlags -> i++ // skip this flag's value
            a.startsWith("--") -> {} // bare/boolean flag
            else -> out.add(a)
        }
        i++
    }
    return out
}

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
        sot - local Nostr profile search

          search "<query>" [--observer <hex>] [--hits N] [--rank <profile>] [--only-ranked] [--vespa <url>]
          status  [--vespa <url>] [--api <url>] [--relay <url>]
          up                 start local Vespa (docker compose) and deploy vespa
          down               stop local Vespa
          deploy [--app <dir>] [--config <host:port>]   redeploy the Vespa app package
        """.trimIndent(),
    )
}

private val SEARCH_VALUED_FLAGS = setOf("--observer", "--hits", "--rank", "--vespa")

private fun search(args: List<String>) {
    val query = positionalArgs(args, SEARCH_VALUED_FLAGS).firstOrNull()
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
    line("http         ($api)", ping("$api/search?text=_"))
    line("relay        ($relay)", ping("$relay/", accept = "application/nostr+json"))
}

private fun run(vararg cmd: String): Int {
    println("$ ${cmd.joinToString(" ")}")
    return ProcessBuilder(*cmd).inheritIO().start().waitFor()
}

/** Poll [url] until it answers < 400 or [tries] attempts elapse, printing dots. */
private fun waitUntil(url: String, tries: Int = 60, everyMs: Long = 2000): Boolean {
    repeat(tries) {
        if (ping(url)) return true
        print("."); System.out.flush()
        Thread.sleep(everyMs)
    }
    return false
}

private fun up(args: List<String>) {
    if (run("docker", "compose", "up", "-d", "vespa") != 0) return
    print("waiting for Vespa config server")
    if (!waitUntil("http://localhost:19071/state/v1/health")) {
        println(" - timed out"); return
    }
    println(" ready; deploying vespa/ ...")
    if (deploy(args) != 0) return
    print("waiting for Vespa to serve the app")
    println(if (waitUntil("http://localhost:8080/ApplicationStatus")) " ready." else " - timed out")
}

private fun down() {
    run("docker", "compose", "down")
}

/** Package `vespa/` and POST it to the config server. Returns the curl exit code. */
private fun deploy(args: List<String>): Int {
    val app = flag(args, "--app", "vespa")
    val config = flag(args, "--config", "localhost:19071")
    val tgz = "/tmp/vespa.tgz"
    if (run("bash", "-c", "tar -czf $tgz -C '$app' .") != 0) return 1
    return run(
        "bash", "-c",
        "curl -fSs --data-binary @$tgz -H 'Content-Type: application/x-gzip' " +
            "http://$config/application/v2/tenant/default/prepareandactivate",
    )
}
