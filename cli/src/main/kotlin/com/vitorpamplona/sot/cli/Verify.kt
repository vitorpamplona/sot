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
import com.vitorpamplona.sot.indexer.IndexVerifier
import com.vitorpamplona.sot.store.openObservableStore
import com.vitorpamplona.sot.vespa.VespaClient
import kotlinx.coroutines.runBlocking
import kotlin.system.exitProcess

/*
 * `sot verify [--repair]` — anti-entropy between the events db (source of
 * truth) and the Vespa index. The projection is fire-and-forget, so a Vespa
 * outage or a crash can lose writes; this walks both sides and reports every
 * difference — with `--repair`, it also fixes them through the same writes the
 * projection uses. Safe to run against a live `sot serve` (it only reads the
 * db), but quietest between sync passes.
 */
internal fun verify(args: List<String>) {
    ensureVespaIsUp(args)
    val repair = has(args, "--repair")
    val dbPath = flag(args, "--db", Config.eventsDb)
    logLine(if (repair) "verifying $dbPath against ${Config.vespaUrl} (repairing) ..." else "verifying $dbPath against ${Config.vespaUrl} (read-only) ...")

    val store = openObservableStore(dbPath)
    val vespa = VespaClient(Config.vespaUrl)
    try {
        val report = runBlocking { IndexVerifier(store, vespa, repair, ::logLine).verify() }
        logLine("DONE - ${report.summary()}")
        if (!repair && report.diffs > 0) logLine("run `sot verify --repair` to fix the ${report.diffs} difference(s)")
    } finally {
        vespa.close()
        store.close()
    }
    exitProcess(0)
}
