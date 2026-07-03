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
package com.vitorpamplona.sot.v2.cli

import com.vitorpamplona.quartz.nip01Core.relay.filters.Filter
import kotlinx.coroutines.runBlocking

/** `sot status` — is Vespa up, and what does the store hold? */
internal fun status(args: List<String>) {
    val vespaUrl = flag(args, "--vespa", Config.vespaUrl)
    val vespaUp = ping("$vespaUrl/ApplicationStatus")
    if (vespaUp) ok("vespa: up at $vespaUrl") else err("vespa: NOT reachable at $vespaUrl")

    val serverUp = ping(Config.serverUrl, accept = "application/nostr+json")
    if (serverUp) ok("server: up at ${Config.serverUrl}") else warn("server: not reachable at ${Config.serverUrl} (is `sot serve` running?)")

    if (!vespaUp) return
    val store = openStore()
    try {
        runBlocking {
            fun countOf(kind: Int) = runBlocking { store.count(Filter(kinds = listOf(kind))) }
            println("  events:    ${store.count(Filter())}")
            println("  profiles:  ${countOf(0)} (kind 0)")
            println("  relaylists:${countOf(10002)} (kind 10002)")
            println("  observers: ${countOf(10040)} (kind 10040)")
            println("  scores:    ${countOf(30382)} (kind 30382)")
        }
    } finally {
        store.close()
    }
}
