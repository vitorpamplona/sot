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
import java.io.File

/*
 * `sot destroy` — wipe every piece of local sot state so the next `sot up` +
 * `sot index` starts from scratch: the event store (with its SQLite sidecars),
 * the sync-state cursors, and Vespa's data volume (docker compose down -v).
 * Asks before deleting unless `--yes`.
 */

internal fun destroy(args: List<String>) {
    val dbPath = flag(args, "--db", Config.eventsDb)
    val files =
        listOf(
            File(dbPath),
            File("$dbPath-wal"),
            File("$dbPath-shm"),
            File("$dbPath.state.json"),
        ).filter { it.exists() }

    println("This wipes all local sot state:")
    if (files.isEmpty()) println("  (no local db/state files found for $dbPath)")
    files.forEach { println("  rm ${it.path}") }
    println("  docker compose down -v      (stops Vespa and deletes its data volume)")

    if (!has(args, "--yes")) {
        print("Continue? [y/N] ")
        val answer = readlnOrNull()?.trim()?.lowercase()
        if (answer != "y" && answer != "yes") {
            println("aborted")
            return
        }
    }

    // Docker may not be running/installed; the local files should go regardless.
    runCatching { run("docker", "compose", "down", "-v") }
        .onFailure { println("! docker compose failed (${it.message}) - remove the vespa_var volume manually") }

    for (f in files) {
        println(if (f.delete()) "deleted ${f.path}" else "! could not delete ${f.path}")
    }
    println("Clean slate. Next: `sot up` then `sot index`.")
}
