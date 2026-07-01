package com.vitorpamplona.vespasearch.cli

import com.vitorpamplona.vespasearch.query.SearchOptions
import com.vitorpamplona.vespasearch.query.VespaSearch
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.time.Duration

/**
 * vespa-search CLI: install/run the local components, check their health, and
 * search from the terminal. Search reuses :common-query, so terminal results
 * match http-api and the relay exactly.
 *
 *   vespa-search search "vitor" [--observer <hex>] [--hits N] [--rank P] [--only-ranked]
 *   vespa-search status
 *   vespa-search up | down            # docker compose for local Vespa
 *   vespa-search deploy               # (re)deploy vespa-app to the config server
 */
private val DEFAULT_OBSERVER =
    System.getenv("PERIODIC_GRAPERANK_PUBKEY")
        ?: "be7bf5de068c1d842ed34a7c270507ec940f5ea51671cfd062a95e9d09420d0a"

private fun flag(args: List<String>, name: String, default: String): String {
    val i = args.indexOf(name)
    return if (i >= 0 && i + 1 < args.size) args[i + 1] else default
}

private fun has(args: List<String>, name: String) = args.contains(name)

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
        vespa-search — local Nostr profile search

          search "<query>" [--observer <hex>] [--hits N] [--rank <profile>] [--only-ranked] [--vespa <url>]
          status  [--vespa <url>] [--api <url>] [--relay <url>]
          up                 start local Vespa (docker compose) and deploy vespa-app
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
    val observer = flag(args, "--observer", DEFAULT_OBSERVER)
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
    line("http-api     ($api)", ping("$api/search/byText?text=_"))
    line("search-relay ($relay)", ping("$relay/", accept = "application/nostr+json"))
}

private fun run(vararg cmd: String): Int {
    println("$ ${cmd.joinToString(" ")}")
    return ProcessBuilder(*cmd).inheritIO().start().waitFor()
}

private fun up(args: List<String>) {
    if (run("docker", "compose", "up", "-d", "vespa") == 0) {
        println("waiting for Vespa config server, then deploying vespa-app…")
        run("docker", "compose", "up", "vespa-deploy")
    }
}

private fun down() {
    run("docker", "compose", "down")
}

private fun deploy(args: List<String>) {
    val app = flag(args, "--app", "vespa-app")
    val config = flag(args, "--config", "localhost:19071")
    val tgz = "/tmp/vespa-app.tgz"
    if (run("bash", "-c", "tar -czf $tgz -C '$app' .") != 0) return
    run(
        "bash", "-c",
        "curl -fSs --data-binary @$tgz -H 'Content-Type: application/x-gzip' " +
            "http://$config/application/v2/tenant/default/prepareandactivate",
    )
}
