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

import com.vitorpamplona.sot.config.Config
import com.vitorpamplona.sot.indexer.SyncOptions
import com.vitorpamplona.sot.indexer.SyncService
import com.vitorpamplona.sot.store.openObservableStore
import kotlinx.coroutines.runBlocking
import kotlin.system.exitProcess
import kotlin.time.Duration.Companion.seconds

/*
 * `sot index [flags]` — one full incremental sync pass, then exit: profiles +
 * NIP-85 trust scores from relays into the local EventStore (source of truth),
 * projected into Vespa. `sot serve` runs the same pass on a loop; use this for
 * the initial backfill or bounded experiments. Defaults come from Config/.env;
 * flags override.
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

internal fun index(args: List<String>) {
    val dbPath = flag(args, "--db", Config.eventsDb)
    val opts =
        SyncOptions(
            maxEvents = flag(args, "--max-events", "0").toInt(),
            fetchTimeoutMs = flag(args, "--fetch-timeout", "30").toLong() * 1000,
            maxProviders = flag(args, "--max-providers", "0").toInt(),
            discover = flag(args, "--discover", "true").toBooleanStrict(),
            maxRounds = flag(args, "--max-rounds", "3").toInt(),
            maxRelays = flag(args, "--max-relays", "200").toInt(),
            concurrency = flag(args, "--concurrency", "8").toInt(),
            reconcileScores = flag(args, "--reconcile", "false").toBooleanStrict(),
        )

    // Shared event store (see :event-store): observable so the projection follows its feed.
    val store = openObservableStore(dbPath)
    val service =
        SyncService(
            store = store,
            vespaUrl = flag(args, "--vespa", Config.vespaUrl),
            seedRelays = argList(args, "--seeds", Config.seedRelays),
            statePath = flag(args, "--state", "$dbPath.state.json"),
            opts = opts,
            log = ::logLine,
        )

    runBlocking {
        service.runOnce()
        logLine("draining projection + Vespa writes ...")
        service.drain(120.seconds)
    }
    logLine("DONE - ${service.summary()}")
    service.close()
    store.close()
    System.out.flush()
    exitProcess(0)
}
