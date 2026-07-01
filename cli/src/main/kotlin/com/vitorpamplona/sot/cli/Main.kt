package com.vitorpamplona.sot.cli

/**
 * sot CLI entry point. Each command lives in its own file — [search] (Search.kt),
 * [status] (Status.kt), and the local-Vespa lifecycle [up]/[down]/[deploy]
 * (Vespa.kt) — with shared helpers in Args.kt, Http.kt, and Observer.kt.
 */
fun main(argv: Array<String>) {
    val args = argv.toList()
    when (args.firstOrNull()) {
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

          search "<query>" [--observer <hex|npub|nprofile|nip05>] [--hits N] [--algo <profile>] [--only-ranked] [--vespa <url>]
          status  [--vespa <url>] [--http <url>] [--relay <url>]
          up                 start local Vespa (docker compose) and deploy vespa
          down               stop local Vespa
          deploy [--app <dir>] [--config <host:port>]   redeploy the Vespa app package
        """.trimIndent(),
    )
}
