package com.vitorpamplona.sot.cli

import com.vitorpamplona.quartz.nip01Core.metadata.MetadataEvent
import com.vitorpamplona.quartz.nip01Core.relay.filters.Filter
import com.vitorpamplona.quartz.nip01Core.store.sqlite.EventStore
import com.vitorpamplona.quartz.nip85TrustedAssertions.list.TrustProviderListEvent
import com.vitorpamplona.quartz.nip85TrustedAssertions.users.ContactCardEvent
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
    val docs = vespaDocCount(vespa)
    val docsLabel = docs?.let { "  ${"%,d".format(it)} docs" } ?: ""
    line("Vespa        ($vespa)$docsLabel", ping("$vespa/ApplicationStatus"))
    line("http         ($httpUrl)", ping("$httpUrl/search?text=_"))
    line("relay        ($relay)", ping("$relay/", accept = "application/nostr+json"))
    line("web          ($DEFAULT_WEB)", ping("$DEFAULT_WEB/"))

    storeReport(System.getenv("EVENTS_DB") ?: "events.db", docs)
}

/** Vespa's total indexed doc count, or null if it can't be read (catches "up but empty"). */
private fun vespaDocCount(vespa: String): Int? {
    val yql = URLEncoder.encode("select * from doc where true", "UTF-8")
    val body = httpGet("$vespa/search/?yql=$yql&hits=0") ?: return null
    return Regex("\"totalCount\"\\s*:\\s*(\\d+)").find(body)?.groupValues?.get(1)?.toIntOrNull()
}

/**
 * Report the local event store (what the relay serves from) and reconcile it
 * against Vespa. kind:0 profiles should project ~1:1 into Vespa docs — but Vespa
 * docs are also created for 30382 score subjects, so `vespa docs >= kind:0` is
 * expected; `vespa docs < kind:0` means projection is lagging or failing.
 */
private fun storeReport(dbPath: String, vespaDocs: Int?) {
    if (!File(dbPath).exists()) {
        println("  store: $dbPath (missing)")
        return
    }
    val counts = runCatching {
        runBlocking {
            EventStore(dbName = dbPath, relay = null).use { s ->
                intArrayOf(
                    s.count(Filter(kinds = listOf(MetadataEvent.KIND))),
                    s.count(Filter(kinds = listOf(TrustProviderListEvent.KIND))),
                    s.count(Filter(kinds = listOf(ContactCardEvent.KIND))),
                )
            }
        }
    }.getOrNull()
    if (counts == null) {
        println("  store: $dbPath (unreadable)")
        return
    }
    val (profiles, providers, assertions) = Triple(counts[0], counts[1], counts[2])
    println("  store: $dbPath")
    println("    kind:0      profiles      ${"%,d".format(profiles)}")
    println("    kind:10040  providers     ${"%,d".format(providers)}")
    println("    kind:30382  assertions    ${"%,d".format(assertions)}")
    if (vespaDocs != null) {
        val delta = vespaDocs - profiles
        val note = if (delta >= 0) "score-only subjects add profiles" else "MISSING profiles in Vespa"
        println("    reconcile:  vespa profiles ${"%,d".format(vespaDocs)} vs profile events ${"%,d".format(profiles)} (${"%+,d".format(delta)}; $note)")
    }
}
