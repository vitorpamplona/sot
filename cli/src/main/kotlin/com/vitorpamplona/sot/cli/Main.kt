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
        "index" -> index(args.drop(1))
        "search" -> search(args.drop(1))
        "status" -> status(args.drop(1))
        "up" -> up(args.drop(1))
        "down" -> down()
        "deploy" -> deploy(args.drop(1))
        else -> usage()
    }
}

private fun usage() {
    println(
        """
        sot - local Nostr profile search

          init                          write a .env config template
          index [<stage>] [flags]       sync Nostr data into Vespa (stage: all | profiles | nip85)
          search "<query>" [--observer <hex|npub|nprofile|nip05>] [--hits N] [--algo <profile>] [--only-ranked] [--vespa <url>]
          status  [--vespa <url>] [--server <url>]
          up                            start local Vespa (docker compose) and deploy vespa
          down                          stop local Vespa
          deploy [--app <dir>] [--config <url>]   redeploy the Vespa app package
        """.trimIndent(),
    )
}
