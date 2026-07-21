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

import com.vitorpamplona.quartz.nip19Bech32.toNpub
import kotlinx.coroutines.runBlocking
import kotlin.time.Duration.Companion.minutes

/*
 * `sot serve` — the long-running CRAWL service: it fills the Vespa store from the
 * network on a loop (every SYNC_INTERVAL minutes), self-publishing the identity's
 * kind 0 / 10002 / 10086 on first run.
 *
 * It does NOT serve clients — that is the relay, a SEPARATE app (vespa-relay).
 * The two share the ONE Vespa-backed store: this side writes trust data and
 * content IN; the relay reads and ranks OUT. The trust projection sits under the
 * store, so every event the crawl inserts updates ranking with no extra wiring.
 *
 * With SYNC_INTERVAL=0 there is no loop to run, so this makes a single pass and
 * exits (the same as `sot index`).
 */
internal fun serve(args: List<String>) {
    val house = requireHouse() // the trust root; refuse to crawl without it
    ensureVespaIsUp(args)
    val identity = serverIdentity()
    logLine("crawl identity (self): ${identity.signer.keyPair.pubKey.toNpub()}")

    val stack = openStack()
    val sync = syncService(stack, identity, house)
    val syncMinutes = Config.syncIntervalMinutes

    if (syncMinutes > 0) {
        logLine("crawl service: syncing every ${syncMinutes}m (SYNC_INTERVAL)")
        Runtime.getRuntime().addShutdownHook(
            Thread {
                sync.close()
                stack.close()
            },
        )
        runBlocking { sync.runForever(syncMinutes.minutes) }
    } else {
        logLine("SYNC_INTERVAL=0: one crawl pass, then exit")
        try {
            runBlocking { sync.runOnce() }
            ok("pass complete.")
        } finally {
            sync.close()
            stack.close()
        }
    }
}

/** Timestamped, styled log line for the long-running commands. */
internal fun logLine(msg: String) = println(styleLogLine(msg))
