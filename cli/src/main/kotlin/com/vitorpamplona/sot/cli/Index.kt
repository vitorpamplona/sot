package com.vitorpamplona.sot.cli

import com.vitorpamplona.quartz.nip01Core.relay.client.NostrClient
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.RelayUrlNormalizer
import com.vitorpamplona.sot.config.Config
import com.vitorpamplona.sot.indexer.SyncState
import com.vitorpamplona.sot.indexer.VespaProjection
import com.vitorpamplona.sot.vespa.VespaClient
import com.vitorpamplona.sot.indexer.okHttpWebsocketBuilder
import com.vitorpamplona.sot.indexer.openStore
import com.vitorpamplona.sot.indexer.runSync
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import kotlin.system.exitProcess

/**
 * `sot index [<stage>] [flags]` — the composition root for the :indexer library:
 * pull Nostr data from relays into the local EventStore (source of truth) and
 * project it into Vespa. Stages: `all` (default) | `profiles` | `nip85`. Defaults
 * come from Config/.env; flags override.
 *
 * Runs the sync to completion then exits (it spawns non-daemon relay/write threads).
 */
private const val DEFAULT_PROFILE_RELAY = "wss://wot.grapevine.network"

/** What a stage syncs: which relays, and whether to pull profiles / scores. */
private class Plan(val relays: List<String>, val profiles: Boolean, val scores: Boolean)

/** A multi-value flag: values after `--name` up to the next `--flag` (e.g. `--seeds a b c`). */
private fun argList(args: List<String>, name: String, default: List<String>): List<String> {
    val i = args.indexOf(name)
    if (i < 0) return default
    val out = mutableListOf<String>()
    var j = i + 1
    while (j < args.size && !args[j].startsWith("--")) { out.add(args[j]); j++ }
    return out.ifEmpty { default }
}

private fun ts(): String {
    val now = java.time.LocalTime.now()
    return "%02d:%02d:%02d".format(now.hour, now.minute, now.second)
}

internal fun index(args: List<String>) {
    val stage = args.firstOrNull()?.takeUnless { it.startsWith("--") } ?: "all"
    val vespaUrl = flag(args, "--vespa", Config.vespaUrl)
    // Per-stage ingest cap (0 = unlimited). Relays hold millions of events; for
    // local experiments a slice is plenty.
    val maxEvents = flag(args, "--max-events", "25000").toInt()
    val dbPath = flag(args, "--db", Config.eventsDb)
    val statePath = flag(args, "--state", "$dbPath.state.json")
    val fetchTimeoutMs = flag(args, "--fetch-timeout", "30").toLong() * 1000
    val maxProviders = flag(args, "--max-providers", "0").toInt()
    val maxRounds = flag(args, "--max-rounds", "3").toInt()
    val maxRelays = flag(args, "--max-relays", "200").toInt()
    val profileRelays = argList(args, "--profile-relays", listOf(DEFAULT_PROFILE_RELAY))
    val seeds = argList(args, "--seeds", Config.seedRelays)
    val discover = flag(args, "--discover", "false").toBooleanStrict()

    // Scores need the broad seed set: kind-10040 provider lists resolve the
    // observer, and they live on general relays, not the score relay alone.
    val plan =
        when (stage) {
            "profiles" -> Plan(profileRelays, profiles = true, scores = false)
            "nip85" -> Plan(seeds, profiles = false, scores = true)
            else -> Plan(seeds, profiles = true, scores = true) // "all"
        }

    val vespa = VespaClient(vespaUrl)
    val client = NostrClient(okHttpWebsocketBuilder(), CoroutineScope(Dispatchers.IO + SupervisorJob()))
    val log: (String) -> Unit = { println("${ts()} $it"); System.out.flush() }
    val state = SyncState.load(statePath)
    // Vespa writes run on a small pool so they don't stall the relay socket reader.
    val writers = Executors.newFixedThreadPool(16)
    // Our own relay identity (from env/.env), so relay-scoped NIP-62 vanish requests are honored.
    val relayUrl = RelayUrlNormalizer.normalize(Config.relayUrl)
    val store = openStore(dbPath, relayUrl)
    val projection = VespaProjection(store, vespa, writers, log)

    // Wire the store -> Vespa projection here; the indexer only fills the store.
    // Its own supervised scope so a projection error can't cancel the sync;
    // launched before syncing, then drained after so it catches up before exit.
    val projScope = CoroutineScope(Dispatchers.IO + SupervisorJob())
    projScope.launch { projection.run() }

    runBlocking {
        runSync(
            client, store, state, statePath, plan.relays, maxEvents, log,
            fetchTimeoutMs, maxProviders, plan.profiles, discover, maxRounds, maxRelays, plan.scores,
        )
        log("draining projection ...")
        projection.awaitIdle()
    }
    projScope.cancel()

    log("draining Vespa writes ...")
    writers.shutdown()
    writers.awaitTermination(120, TimeUnit.SECONDS)
    log("DONE $stage - profiles=${projection.profiles.get()} scores=${projection.scores.get()} " +
        "deletions=${projection.deletions.get()} unresolved=${projection.unresolved.get()}")
    store.close()
    client.close()
    System.out.flush()
    exitProcess(0)
}
