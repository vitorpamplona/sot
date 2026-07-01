package com.vitorpamplona.sot.cli

import com.vitorpamplona.sot.indexer.runIndexer
import kotlin.system.exitProcess

/**
 * `sot index [<stage>] [flags]` — run one indexer pass (Nostr -> EventStore ->
 * Vespa). Stages: `all` (default) | `profiles` | `nip85`; flags like
 * `--max-events`, `--db`, `--discover` are passed straight through. Runs the
 * indexer in-process, then exits (the sync spawns non-daemon relay/write threads).
 */
internal fun index(args: List<String>) {
    runIndexer(args.toTypedArray())
    exitProcess(0)
}
