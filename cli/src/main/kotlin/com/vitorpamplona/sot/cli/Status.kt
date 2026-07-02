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

import com.vitorpamplona.quartz.nip01Core.metadata.MetadataEvent
import com.vitorpamplona.quartz.nip01Core.relay.filters.Filter
import com.vitorpamplona.quartz.nip85TrustedAssertions.list.TrustProviderListEvent
import com.vitorpamplona.quartz.nip85TrustedAssertions.users.ContactCardEvent
import com.vitorpamplona.sot.config.Config
import com.vitorpamplona.sot.store.openEventStore
import kotlinx.coroutines.runBlocking
import java.io.File
import java.net.URLEncoder

/**
 * `sot status` — report whether Vespa and the sot server are reachable, plus data
 * health. The server hosts the web UI, /search API, and the NIP-50 relay on one port.
 */
internal fun status(args: List<String>) {
    val vespa = flag(args, "--vespa", Config.vespaUrl)
    val server = flag(args, "--server", Config.serverUrl)

    fun line(
        name: String,
        ok: Boolean,
    ) = println("  ${if (ok) "[ UP ]" else "[DOWN]"}  $name")

    println("component status:")
    val docs = vespaDocCount(vespa)
    val docsLabel = docs?.let { "  ${"%,d".format(it)} docs" } ?: ""
    line("Vespa   ($vespa)$docsLabel", ping("$vespa/ApplicationStatus"))
    // Probe the server root (web UI / NIP-11) — 200 regardless of Vespa, unlike /search.
    line("server  ($server)", ping("$server/"))

    storeReport(Config.eventsDb, docs)
}

/** Vespa's total indexed doc count, or null if it can't be read (catches "up but empty"). */
private fun vespaDocCount(vespa: String): Int? {
    val yql = URLEncoder.encode("select * from doc where true", "UTF-8")
    val body = httpGet("$vespa/search/?yql=$yql&hits=0") ?: return null
    return Regex("\"totalCount\"\\s*:\\s*(\\d+)")
        .find(body)
        ?.groupValues
        ?.get(1)
        ?.toIntOrNull()
}

/**
 * Report the local event store (what the relay serves from) and reconcile it
 * against Vespa. kind:0 profiles should project ~1:1 into Vespa docs — but Vespa
 * docs are also created for 30382 score subjects, so `vespa docs >= kind:0` is
 * expected; `vespa docs < kind:0` means projection is lagging or failing.
 */
private fun storeReport(
    dbPath: String,
    vespaDocs: Int?,
) {
    if (!File(dbPath).exists()) {
        println("  store: $dbPath (missing)")
        return
    }
    val counts =
        runCatching {
            runBlocking {
                openEventStore(dbPath).use { s ->
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
