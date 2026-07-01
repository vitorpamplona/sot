package com.vitorpamplona.sot.cli

import com.vitorpamplona.quartz.nip01Core.relay.filters.Filter
import com.vitorpamplona.quartz.nip01Core.store.sqlite.EventStore
import kotlinx.coroutines.runBlocking
import java.io.File
import java.net.URLEncoder

private const val DEFAULT_WEB = "http://localhost:8090"

/** `sot status` — report whether Vespa / http / relay / web are reachable, plus data health. */
internal fun status(args: List<String>) {
    val vespa = flag(args, "--vespa", "http://localhost:8080")
    val httpUrl = flag(args, "--http", "http://localhost:8081")
    val relay = flag(args, "--relay", "http://localhost:7777")
    fun line(name: String, ok: Boolean) = println("  ${if (ok) "[ UP ]" else "[DOWN]"}  $name")

    println("component status:")
    val docs = vespaDocCount(vespa)?.let { "  ${"%,d".format(it)} docs" } ?: ""
    line("Vespa        ($vespa)$docs", ping("$vespa/ApplicationStatus"))
    line("http         ($httpUrl)", ping("$httpUrl/search?text=_"))
    line("relay        ($relay)", ping("$relay/", accept = "application/nostr+json"))
    line("web          ($DEFAULT_WEB)", ping("$DEFAULT_WEB/"))

    val dbPath = System.getenv("EVENTS_DB") ?: "events.db"
    println("  store: $dbPath (${eventStoreSummary(dbPath)})")
}

/** Vespa's total indexed doc count, or null if it can't be read (catches "up but empty"). */
private fun vespaDocCount(vespa: String): Int? {
    val yql = URLEncoder.encode("select * from doc where true", "UTF-8")
    val body = httpGet("$vespa/search/?yql=$yql&hits=0") ?: return null
    return Regex("\"totalCount\"\\s*:\\s*(\\d+)").find(body)?.groupValues?.get(1)?.toIntOrNull()
}

/** Event count in the local store (what the relay serves from), or "missing"/"unreadable". */
private fun eventStoreSummary(dbPath: String): String {
    if (!File(dbPath).exists()) return "missing"
    val n = runCatching {
        runBlocking { EventStore(dbName = dbPath, relay = null).use { it.count(Filter()) } }
    }.getOrNull()
    return n?.let { "%,d events".format(it) } ?: "unreadable"
}
