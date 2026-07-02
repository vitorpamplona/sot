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

/**
 * sot CLI entry point. Each command lives in its own file — [init] (Init.kt),
 * [search] (Search.kt), [status] (Status.kt), and the local-Vespa lifecycle
 * [up]/[down]/[deploy] (Vespa.kt) — with shared helpers in Args.kt, Http.kt,
 * and Observer.kt. Config/`.env` handling lives in :query-engine's Config.
 */
fun main(argv: Array<String>) {
    val args = argv.toList()
    when (args.firstOrNull()) {
        "init" -> init(args.drop(1))
        "serve" -> serve(args.drop(1))
        "index" -> index(args.drop(1))
        "search" -> search(args.drop(1))
        "status" -> status(args.drop(1))
        "up" -> up(args.drop(1))
        "down" -> down()
        "destroy" -> destroy(args.drop(1))
        "deploy" -> deploy(args.drop(1))
        else -> usage()
    }
}

private fun usage() {
    println(
        """
        sot - local Nostr profile search

          init                          write a .env config template
          serve [--up]                  web UI + /search + NIP-50 relay + background sync (SYNC_INTERVAL); --up starts Vespa first
          index [flags]                 one sync pass of profiles + NIP-85 trust scores into Vespa
          search "<query>" [--observer <hex|npub|nprofile|nip05>] [--hits N] [--algo <profile>] [--only-ranked] [--vespa <url>]
          status  [--vespa <url>] [--server <url>]
          up                            start local Vespa (docker compose) and deploy vespa
          down                          stop local Vespa
          destroy [--yes] [--db <path>] wipe events db + sync state + Vespa's data volume
          deploy [--app <dir>] [--config <url>]   redeploy the Vespa app package
        """.trimIndent(),
    )
}
