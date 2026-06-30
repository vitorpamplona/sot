package com.vitorpamplona.vespasearch.indexer

import com.vitorpamplona.quartz.nip01Core.core.Event
import com.vitorpamplona.quartz.nip01Core.metadata.MetadataEvent
import com.vitorpamplona.quartz.nip01Core.relay.client.NostrClient
import com.vitorpamplona.quartz.nip01Core.relay.client.accessories.fetchAllPages
import com.vitorpamplona.quartz.nip01Core.relay.filters.Filter
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.RelayUrlNormalizer
import com.vitorpamplona.quartz.nip01Core.relay.sockets.WebsocketBuilder
import com.vitorpamplona.quartz.nip85TrustedAssertions.users.ContactCardEvent
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeoutOrNull

/**
 * Kotlin loader: pull Nostr data from relays (via Amethyst's Quartz) straight
 * into the local Vespa, replacing the Python REQ-based loader.
 *
 *   gradle run --args="profiles"
 *   gradle run --args="nip85"
 *   gradle run --args="all --max-events 25000"
 *   gradle run --args="profiles --mode negentropy"   # NIP-77 delta sync
 *
 * Two fetch modes:
 *   pages      (default) Quartz's fetchAllPages — paginated `until` cursors.
 *              Bypasses the relay's ~500/page cap, works on every relay, and
 *              `Filter.limit` is the ingest cap.
 *   negentropy NIP-77 set reconciliation via NegentropyStage. Faster for
 *              re-syncs (only fetches missing ids) but a relay can refuse to
 *              reconcile a set larger than its `negentropy.maxSyncEvents`
 *              (e.g. nip85-staging), in which case `pages` is the way.
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
    val mode = arg(args, "--mode", "pages")
    val vespaUrl = arg(args, "--vespa", System.getenv("VESPA_URL") ?: "http://localhost:8080")
    val minRank = arg(args, "--min-rank", "1").toInt()
    val batch = arg(args, "--batch", "500").toInt()
    // Per-stage ingest cap (0 = unlimited). Relays can hold millions of events;
    // for local equation experiments a slice is plenty.
    val maxEvents = arg(args, "--max-events", "25000").toInt()
    val timeoutSecs = arg(args, "--limit-secs", "240").toLong()
    val profileRelays = argList(args, "--relays", listOf(DEFAULT_PROFILE_RELAY))
    val scoreRelays = argList(args, "--score-relays", listOf(DEFAULT_SCORE_RELAY))

    val vespa = VespaClient(vespaUrl)
    val socketBuilder = okHttpWebsocketBuilder()
    val scope = CoroutineScope(Dispatchers.IO + SupervisorJob())
    val client = NostrClient(socketBuilder, scope)
    val log: (String) -> Unit = { println("${ts()} $it"); System.out.flush() }

    // EventStore-backed pipeline: store is source of truth, Vespa is a projection.
    // Scores are keyed by the OBSERVER (10040 author), resolved from the service key.
    if (stage == "sync") {
        val dbPath = arg(args, "--db", "events.db")
        val seeds = argList(args, "--seeds", listOf(
            "wss://purplepag.es",
            "wss://relay.damus.io",
            "wss://profiles.nostr1.com",
            DEFAULT_SCORE_RELAY,
            DEFAULT_PROFILE_RELAY,
        ))
        val fetchTimeout = arg(args, "--fetch-timeout", "30").toLong() * 1000
        val maxProviders = arg(args, "--max-providers", "0").toInt()
        val syncProfiles = arg(args, "--profiles", "true").toBooleanStrict()
        val writers = java.util.concurrent.Executors.newFixedThreadPool(16)
        val store = openStore(dbPath)
        val projection = VespaProjection(store, vespa, writers, log)
        runBlocking { runSync(client, store, projection, seeds, maxEvents, log, fetchTimeout, maxProviders, syncProfiles) }
        writers.shutdown()
        writers.awaitTermination(120, java.util.concurrent.TimeUnit.SECONDS)
        log("DONE sync - profiles=${projection.profiles.get()} scores=${projection.scores.get()} " +
            "deletions=${projection.deletions.get()} unresolved=${projection.unresolved.get()}")
        store.close()
        client.close()
        System.out.flush()
        kotlin.system.exitProcess(0)
    }

    // Vespa writes run on a small pool so they don't block the relay socket
    // reader thread one-PUT-at-a-time. Drained before exit.
    val writers = java.util.concurrent.Executors.newFixedThreadPool(16)
    val profiles = AtomicCounter()
    val scores = AtomicCounter()
    val writeFailures = java.util.concurrent.atomic.AtomicInteger(0)

    fun onWriteError(what: String, e: Throwable) {
        // Never swallow write failures silently — a down/blocked Vespa would
        // otherwise look like "0 upserted" with no explanation.
        if (writeFailures.incrementAndGet() <= 5) log("  ! $what write failed: ${e.message}")
    }

    val onProfile: (Event) -> Unit = { ev ->
        val md = (ev as? MetadataEvent)?.contactMetaData()
        if (md != null) {
            writers.submit {
                runCatching { vespa.upsertProfile(ev.pubKey, md) }
                    .onSuccess { profiles.inc() }
                    .onFailure { onWriteError("profile", it) }
            }
        }
    }
    val onScore: (Event) -> Unit = { ev ->
        val card = ev as? ContactCardEvent
        val subject = card?.aboutUser()
        val rank = card?.rank()
        if (subject != null && rank != null && rank >= minRank) {
            writers.submit {
                runCatching { vespa.upsertScore(subject, ev.pubKey, rank) }
                    .onSuccess { scores.inc() }
                    .onFailure { onWriteError("score", it) }
            }
        }
    }

    runBlocking {
        if (stage == "profiles" || stage == "all") {
            fetch(mode, "profiles", client, profileRelays, MetadataEvent.KIND,
                maxEvents, batch, socketBuilder, onProfile, log, timeoutSecs)
        }
        if (stage == "nip85" || stage == "all") {
            fetch(mode, "nip85", client, scoreRelays, ContactCardEvent.KIND,
                maxEvents, batch, socketBuilder, onScore, log, timeoutSecs)
        }
    }

    log("draining Vespa writes ...")
    writers.shutdown()
    writers.awaitTermination(120, java.util.concurrent.TimeUnit.SECONDS)
    log("DONE - profiles upserted=${profiles.get()}, scores upserted=${scores.get()}, writeFailures=${writeFailures.get()}")
    client.close()
    System.out.flush()
    kotlin.system.exitProcess(0)
}

private suspend fun fetch(
    mode: String,
    label: String,
    client: NostrClient,
    relays: List<String>,
    kind: Int,
    maxEvents: Int,
    batch: Int,
    socketBuilder: WebsocketBuilder,
    onEvent: (Event) -> Unit,
    log: (String) -> Unit,
    timeoutSecs: Long,
) {
    if (mode == "negentropy") {
        runNegentropy(label, relays, Filter(kinds = listOf(kind)), socketBuilder, batch, maxEvents, onEvent, log, timeoutSecs)
        return
    }
    // pages mode: Filter.limit is the ingest cap; fetchAllPages walks `until`
    // cursors until the limit is hit or a page comes back empty.
    val filter = Filter(kinds = listOf(kind), limit = if (maxEvents > 0) maxEvents else null)
    coroutineScopeFetch(label, client, relays, filter, onEvent, log, timeoutSecs)
}

private suspend fun coroutineScopeFetch(
    label: String,
    client: NostrClient,
    relays: List<String>,
    filter: Filter,
    onEvent: (Event) -> Unit,
    log: (String) -> Unit,
    timeoutSecs: Long,
) = kotlinx.coroutines.coroutineScope {
    val counters = java.util.concurrent.atomic.AtomicInteger(0)
    val jobs =
        relays.map { url ->
            async {
                val relay = RelayUrlNormalizer.normalize(url)
                log("[$label] ${shortUrl(url)} fetching all pages (kind=${filter.kinds}) ...")
                val n =
                    withTimeoutOrNull(timeoutSecs * 1000) {
                        client.fetchAllPages(
                            relay,
                            listOf(filter),
                            onNewPage = { until -> log("[$label] ${shortUrl(url)} next page until=$until (got ${counters.get()})") },
                        ) { ev ->
                            onEvent(ev)
                            val c = counters.incrementAndGet()
                            if (c % 500 == 0) log("[$label] ... $c events fetched")
                        }
                    }
                log("[$label] ${shortUrl(url)} done: ${n ?: "timeout"} events")
            }
        }
    jobs.awaitAll()
}

private suspend fun runNegentropy(
    label: String,
    relays: List<String>,
    filter: Filter,
    socketBuilder: WebsocketBuilder,
    batch: Int,
    maxEvents: Int,
    onEvent: (Event) -> Unit,
    log: (String) -> Unit,
    timeoutSecs: Long,
) {
    val stages =
        relays.map { url ->
            NegentropyStage(RelayUrlNormalizer.normalize(url), socketBuilder, filter, batch, maxEvents, onEvent, log)
        }
    stages.forEach { it.start() }
    val results = withTimeoutOrNull(timeoutSecs * 1000) { stages.map { it.done.await() } }
    if (results == null) log("[$label] timed out after ${timeoutSecs}s; partial results kept")
}

private fun shortUrl(url: String) = url.removePrefix("wss://").removePrefix("ws://").trimEnd('/')

private fun ts(): String {
    val now = java.time.LocalTime.now()
    return "%02d:%02d:%02d".format(now.hour, now.minute, now.second)
}

private class AtomicCounter {
    private val n = java.util.concurrent.atomic.AtomicInteger(0)
    fun inc() = n.incrementAndGet()
    fun get() = n.get()
}
