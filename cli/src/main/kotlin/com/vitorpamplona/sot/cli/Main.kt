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

import com.vitorpamplona.quartz.utils.Log
import com.vitorpamplona.quartz.utils.LogLevel

/**
 * sot CLI entry point — the composition root. Each command lives in its own
 * file: the interactive [init] (Init.kt), [serve]/[index] (the composition,
 * Serve.kt/Index.kt), [status], and the local-Vespa lifecycle
 * [up]/[down]/[deploy]/[destroy] (Vespa.kt).
 */
fun main(argv: Array<String>) {
    // Quartz narrates every oddity it meets (garbage relay urls in 10002s, and
    // so on) to stderr. Errors-only by default keeps sync output readable.
    Log.minLevel = runCatching { LogLevel.valueOf(Config.quartzLogLevel.uppercase()) }.getOrDefault(LogLevel.ERROR)

    val args = argv.toList()
    when (args.firstOrNull()) {
        "init" -> init(args.drop(1))
        "serve" -> serve(args.drop(1))
        "index" -> index(args.drop(1))
        "status" -> status(args.drop(1))
        "up" -> up(args.drop(1))
        "down" -> down()
        "destroy" -> destroy(args.drop(1))
        "deploy" -> deploy(args.drop(1))
        else -> usage()
    }
}

private fun usage() {
    fun cmd(
        name: String,
        blurb: String,
    ) = "  ${Ansi.cyan(name.padEnd(14))} ${Ansi.dim(blurb)}"

    println(
        listOf(
            Ansi.bold("sot") + Ansi.dim(" - web-of-trust trust-sync indexer for a Nostr search relay"),
            "",
            cmd("init", "interactive setup: writes .env (--yes = all defaults, --force = overwrite)"),
            cmd("serve [--up]", "run the trust-sync crawl on a loop (SYNC_INTERVAL); --up starts Vespa first"),
            cmd("index [--up]", "one trust-sync pass (observers -> outboxes -> providers) into Vespa"),
            cmd("status", "[--vespa <url>] [--observer <npub>] reachability, event counts + coverage"),
            "",
            cmd("up", "start local Vespa (docker compose) and deploy the bundled schema"),
            cmd("down", "stop local Vespa"),
            cmd("destroy", "[--yes] wipe sync state + Vespa's data volume (THE event store)"),
            cmd("deploy", "[--config <url>] redeploy the bundled Vespa app package"),
        ).joinToString("\n"),
    )
}
