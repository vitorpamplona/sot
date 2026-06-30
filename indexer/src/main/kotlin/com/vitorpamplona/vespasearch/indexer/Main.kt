package com.vitorpamplona.vespasearch.indexer

import com.vitorpamplona.quartz.nip01Core.core.Event
import com.vitorpamplona.quartz.nip01Core.metadata.MetadataEvent
import com.vitorpamplona.quartz.nip01Core.relay.filters.Filter
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.RelayUrlNormalizer
import com.vitorpamplona.quartz.nip85TrustedAssertions.users.ContactCardEvent
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeoutOrNull

/**
 * Kotlin loader: negentropy-sync Nostr data from relays (via Amethyst's Quartz)
 * straight into the local Vespa, replacing the Python REQ-based loader.
 *
 *   gradle run --args="profiles"
 *   gradle run --args="nip85"
 *   gradle run --args="all --limit-secs 180"
 */
private const val DEFAULT_PROFILE_RELAY = "wss://wot.grapevine.network"
private const val DEFAULT_SCORE_RELAY = "wss://nip85-staging.nosfabrica.com"

private fun arg(args: Array<String>, name: String, default: String): String {
    val i = args.indexOf(name)
    return if (i >= 0 && i + 1 < args.size) args[i + 1] else default
}

private fun argList(args: Array<String>, name: String, default: List<String>): List<String> {
    val i = args.indexOf(name)
    if (i < 0) return default
    val out = mutableListOf<String>()
    var j = i + 1
    while (j < args.size && !args[j].startsWith("--")) { out.add(args[j]); j++ }
    return out.ifEmpty { default }
}

fun main(args: Array<String>) {
    val stage = args.firstOrNull()?.takeUnless { it.startsWith("--") } ?: "all"
    val vespaUrl = arg(args, "--vespa", System.getenv("VESPA_URL") ?: "http://localhost:8080")
    val minRank = arg(args, "--min-rank", "1").toInt()
    val batch = arg(args, "--batch", "500").toInt()
    // Cap how many events we ingest per stage (0 = unlimited). Relays can hold
    // millions of kind:0 events; for local equation experiments a slice is plenty.
    val maxEvents = arg(args, "--max-events", "25000").toInt()
    val timeoutSecs = arg(args, "--limit-secs", "240").toLong()
    val profileRelays = argList(args, "--relays", listOf(DEFAULT_PROFILE_RELAY))
    val scoreRelays = argList(args, "--score-relays", listOf(DEFAULT_SCORE_RELAY))

    val vespa = VespaClient(vespaUrl)
    val socketBuilder = okHttpWebsocketBuilder()
    val log: (String) -> Unit = { println("${ts()} $it"); System.out.flush() }

    // Vespa writes run on a small pool so they don't block the relay socket
    // reader thread one-PUT-at-a-time. The pool is drained before exit.
    val writers = java.util.concurrent.Executors.newFixedThreadPool(16)
    val profiles = AtomicCounter()
    val scores = AtomicCounter()

    // kind:0 -> upsert profile
    val onProfile: (Event) -> Unit = { ev ->
        val md = (ev as? MetadataEvent)?.contactMetaData()
        if (md != null) {
            writers.submit {
                runCatching { vespa.upsertProfile(ev.pubKey, md) }
                    .onSuccess { profiles.inc() }
            }
        }
    }

    // kind:30382 -> upsert quality_scores{author}=rank on subject's doc
    val onScore: (Event) -> Unit = { ev ->
        val card = ev as? ContactCardEvent
        val subject = card?.aboutUser()
        val rank = card?.rank()
        if (subject != null && rank != null && rank >= minRank) {
            writers.submit {
                runCatching { vespa.upsertScore(subject, ev.pubKey, rank) }
                    .onSuccess { scores.inc() }
            }
        }
    }

    runBlocking {
        if (stage == "profiles" || stage == "all") {
            runStage("profiles", profileRelays, Filter(kinds = listOf(MetadataEvent.KIND)),
                socketBuilder, batch, maxEvents, onProfile, log, timeoutSecs)
        }
        if (stage == "nip85" || stage == "all") {
            runStage("nip85", scoreRelays, Filter(kinds = listOf(ContactCardEvent.KIND)),
                socketBuilder, batch, maxEvents, onScore, log, timeoutSecs)
        }
    }

    log("draining Vespa writes ...")
    writers.shutdown()
    writers.awaitTermination(120, java.util.concurrent.TimeUnit.SECONDS)
    log("DONE - profiles upserted=${profiles.get()}, scores upserted=${scores.get()}")
    // OkHttp keeps non-daemon dispatcher threads alive; force a clean exit.
    System.out.flush()
    kotlin.system.exitProcess(0)
}

private fun ts(): String {
    val now = java.time.LocalTime.now()
    return "%02d:%02d:%02d".format(now.hour, now.minute, now.second)
}

private suspend fun runStage(
    label: String,
    relays: List<String>,
    filter: Filter,
    socketBuilder: com.vitorpamplona.quartz.nip01Core.relay.sockets.WebsocketBuilder,
    batch: Int,
    maxEvents: Int,
    onEvent: (Event) -> Unit,
    log: (String) -> Unit,
    timeoutSecs: Long,
) {
    val stages =
        relays.map { url ->
            NegentropyStage(
                RelayUrlNormalizer.normalize(url),
                socketBuilder,
                filter,
                batch,
                maxEvents,
                onEvent,
                log,
            )
        }
    stages.forEach { it.start() }
    val results =
        withTimeoutOrNull(timeoutSecs * 1000) {
            stages.map { it.done.await() }
        }
    if (results == null) log("[$label] timed out after ${timeoutSecs}s; partial results kept")
}

private class AtomicCounter {
    private val n = java.util.concurrent.atomic.AtomicInteger(0)
    fun inc() = n.incrementAndGet()
    fun get() = n.get()
}
