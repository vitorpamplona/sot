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

import com.vitorpamplona.quartz.nip01Core.relay.client.NostrClient
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.RelayUrlNormalizer
import com.vitorpamplona.sot.config.Config
import com.vitorpamplona.sot.indexer.SyncOptions
import com.vitorpamplona.sot.indexer.SyncState
import com.vitorpamplona.sot.indexer.VespaProjection
import com.vitorpamplona.sot.indexer.okHttpWebsocketBuilder
import com.vitorpamplona.sot.indexer.runSync
import com.vitorpamplona.sot.store.openObservableStore
import com.vitorpamplona.sot.vespa.VespaClient
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import kotlin.system.exitProcess

/*
 * `sot index [flags]` — the composition root for the :indexer library: pull
 * profiles + NIP-85 trust scores from relays into the local EventStore (source
 * of truth) and project them into Vespa. One indivisible sync — profiles without
 * scores (or vice versa) leave the index unusable. Defaults come from
 * Config/.env; flags override.
 *
 * Runs the sync to completion then exits (it spawns non-daemon relay/write threads).
 */

/** A multi-value flag: values after `--name` up to the next `--flag` (e.g. `--seeds a b c`). */
private fun argList(
    args: List<String>,
    name: String,
    default: List<String>,
): List<String> {
    val i = args.indexOf(name)
    if (i < 0) return default
    val out = mutableListOf<String>()
    var j = i + 1
    while (j < args.size && !args[j].startsWith("--")) {
        out.add(args[j])
        j++
    }
    return out.ifEmpty { default }
}

private fun ts(): String {
    val now = java.time.LocalTime.now()
    return "%02d:%02d:%02d".format(now.hour, now.minute, now.second)
}

internal fun index(args: List<String>) {
    val vespaUrl = flag(args, "--vespa", Config.vespaUrl)
    val dbPath = flag(args, "--db", Config.eventsDb)
    val statePath = flag(args, "--state", "$dbPath.state.json")
    // Normalize relay urls once, here at the edge; everything past this point is typed.
    val relays =
        argList(args, "--seeds", Config.seedRelays).mapNotNull { s ->
            RelayUrlNormalizer.normalizeOrNull(s) ?: run {
                println("skipping invalid relay url: $s")
                null
            }
        }
    if (relays.isEmpty()) {
        println("no valid seed relays (--seeds / SEED_RELAYS)")
        exitProcess(1)
    }

    val opts =
        SyncOptions(
            maxEvents = flag(args, "--max-events", "0").toInt(),
            fetchTimeoutMs = flag(args, "--fetch-timeout", "30").toLong() * 1000,
            maxProviders = flag(args, "--max-providers", "0").toInt(),
            discover = flag(args, "--discover", "false").toBooleanStrict(),
            maxRounds = flag(args, "--max-rounds", "3").toInt(),
            maxRelays = flag(args, "--max-relays", "200").toInt(),
        )

    val vespa = VespaClient(vespaUrl)
    val client = NostrClient(okHttpWebsocketBuilder(), CoroutineScope(Dispatchers.IO + SupervisorJob()))
    val log: (String) -> Unit = {
        println("${ts()} $it")
        System.out.flush()
    }
    val state = SyncState.load(statePath)
    // Vespa writes run on a small pool so they don't stall the relay socket reader.
    val writers = Executors.newFixedThreadPool(16)
    // Shared event store (see :event-store): observable for its change feed -> projection.
    val store = openObservableStore(dbPath)
    val projection = VespaProjection(store, vespa, writers, log)

    // Wire the store -> Vespa projection here; the indexer only fills the store.
    // Its own supervised scope so a projection error can't cancel the sync;
    // launched before syncing, then drained after so it catches up before exit.
    val projScope = CoroutineScope(Dispatchers.IO + SupervisorJob())
    projScope.launch { projection.run() }

    runBlocking {
        runSync(client, store, state, statePath, relays, opts, log)
        log("draining projection ...")
        projection.awaitIdle()
    }
    projScope.cancel()

    log("draining Vespa writes ...")
    writers.shutdown()
    writers.awaitTermination(120, TimeUnit.SECONDS)
    log(
        "DONE - profiles=${projection.profiles.get()} scores=${projection.scores.get()} " +
            "deletions=${projection.deletions.get()} unresolved=${projection.unresolved.get()}",
    )
    store.close()
    client.close()
    System.out.flush()
    exitProcess(0)
}
