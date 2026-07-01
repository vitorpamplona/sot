package com.vitorpamplona.sot.indexer

import com.vitorpamplona.quartz.nip01Core.relay.client.NostrClient
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.RelayUrlNormalizer
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.runBlocking

/**
 * Kotlin loader: pull Nostr data from relays (via Amethyst's Quartz) into a local
 * Quartz EventStore — the source of truth — and project it into Vespa
 * (see [VespaProjection]). Scores are keyed by the OBSERVER (kind-10040 author),
 * resolved from the 30382 service key, per NIP-85 Trusted Assertions.
 *
 *   gradle run --args="all"                    # kind:0 profiles + NIP-85 scores (default)
 *   gradle run --args="profiles"               # kind:0 profiles only
 *   gradle run --args="nip85"                  # NIP-85 scores only (10040 + 30382)
 *   gradle run --args="all --discover true"    # + kind-10002 outbox crawl
 *
 * Every stage is one call to [runSync]: the store persists each event so re-runs
 * only fetch the delta (negentropy / `since` cursors), and Vespa is a projection.
 */
private const val DEFAULT_PROFILE_RELAY = "wss://wot.grapevine.network"
private const val DEFAULT_SCORE_RELAY = "wss://nip85-staging.nosfabrica.com"
private val DEFAULT_SEEDS = listOf(
    "wss://purplepag.es",
    "wss://relay.damus.io",
    "wss://profiles.nostr1.com",
    DEFAULT_SCORE_RELAY,
    DEFAULT_PROFILE_RELAY,
)

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

/** What a stage syncs: which relays, and whether to pull profiles / scores. */
private class Plan(val relays: List<String>, val profiles: Boolean, val scores: Boolean)

fun main(args: Array<String>) {
    runIndexer(args)
    kotlin.system.exitProcess(0)
}

/** Run one indexer pass to completion (sync + project + drain). Callable from the CLI's `sot index`. */
fun runIndexer(args: Array<String>) {
    val stage = args.firstOrNull()?.takeUnless { it.startsWith("--") } ?: "all"
    val vespaUrl = arg(args, "--vespa", System.getenv("VESPA_URL") ?: "http://localhost:8080")
    // Per-stage ingest cap (0 = unlimited). Relays can hold millions of events;
    // for local equation experiments a slice is plenty.
    val maxEvents = arg(args, "--max-events", "25000").toInt()
    val dbPath = arg(args, "--db", System.getenv("EVENTS_DB") ?: "events.db")
    val statePath = arg(args, "--state", "$dbPath.state.json")
    val fetchTimeoutMs = arg(args, "--fetch-timeout", "30").toLong() * 1000
    val maxProviders = arg(args, "--max-providers", "0").toInt()
    val maxRounds = arg(args, "--max-rounds", "3").toInt()
    val maxRelays = arg(args, "--max-relays", "200").toInt()

    val profileRelays = argList(args, "--profile-relays", listOf(DEFAULT_PROFILE_RELAY))
    val seeds = argList(args, "--seeds", DEFAULT_SEEDS)
    // Scores need the broad seed set: kind-10040 provider lists resolve the
    // observer, and they live on general relays, not the score relay alone.
    val plan = when (stage) {
        "profiles" -> Plan(profileRelays, profiles = true, scores = false)
        "nip85" -> Plan(seeds, profiles = false, scores = true)
        else -> Plan(seeds, profiles = true, scores = true) // "all"
    }
    val discover = arg(args, "--discover", "false").toBooleanStrict()

    val vespa = VespaClient(vespaUrl)
    val client = NostrClient(okHttpWebsocketBuilder(), CoroutineScope(Dispatchers.IO + SupervisorJob()))
    val log: (String) -> Unit = { println("${ts()} $it"); System.out.flush() }
    val state = SyncState.load(statePath)

    // Vespa writes run on a small pool so they don't stall the relay socket
    // reader thread one PUT at a time. Drained before exit.
    val writers = java.util.concurrent.Executors.newFixedThreadPool(16)
    // This store's own relay identity (from env), so NIP-62 vanish requests
    // scoped to our relay are honored. Set up here, at app start.
    val relayUrl = RelayUrlNormalizer.normalize(System.getenv("RELAY_URL") ?: "ws://localhost:7777")
    val store = openStore(dbPath, relayUrl)
    val projection = VespaProjection(store, vespa, writers, log)

    runBlocking {
        runSync(
            client, store, projection, state, statePath, plan.relays, maxEvents, log,
            fetchTimeoutMs, maxProviders, plan.profiles, discover, maxRounds, maxRelays, plan.scores,
        )
    }

    log("draining Vespa writes ...")
    writers.shutdown()
    writers.awaitTermination(120, java.util.concurrent.TimeUnit.SECONDS)
    log("DONE $stage - profiles=${projection.profiles.get()} scores=${projection.scores.get()} " +
        "deletions=${projection.deletions.get()} unresolved=${projection.unresolved.get()}")
    store.close()
    client.close()
    System.out.flush()
}

private fun ts(): String {
    val now = java.time.LocalTime.now()
    return "%02d:%02d:%02d".format(now.hour, now.minute, now.second)
}
