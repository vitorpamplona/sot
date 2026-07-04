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

import java.io.File
import kotlin.system.exitProcess

/*
 * Local Vespa lifecycle: `up` / `down` via the repo's docker compose, and
 * `deploy` of the application package (vespa/app — the event + profile
 * schemas and the rank profiles, next to the Kotlin that depends on them).
 * Endpoints come from Config (VESPA_URL / VESPA_CONFIG_URL).
 */

/** Run a subprocess, echoing the command; returns its exit code. */
internal fun run(vararg cmd: String): Int {
    shellEcho(cmd.joinToString(" "))
    return ProcessBuilder(*cmd).inheritIO().start().waitFor()
}

/**
 * Refuse to start a command that needs Vespa when it isn't up. `--up` runs the
 * `sot up` sequence first when the engine is the local docker one. A remote
 * VESPA_URL never gets docker side effects, since its lifecycle isn't ours.
 */
internal fun ensureVespaIsUp(args: List<String>) {
    val statusUrl = "${Config.vespaUrl}/ApplicationStatus"
    if (ping(statusUrl)) return

    val local = Config.vespaUrl.contains("://localhost") || Config.vespaUrl.contains("://127.0.0.1")
    if (has(args, "--up") && local) {
        up(emptyList())
        if (ping(statusUrl)) return
        err("Vespa is still not reachable at ${Config.vespaUrl} - see the `sot up` output above.")
        exitProcess(1)
    }
    err("Vespa is not reachable at ${Config.vespaUrl}.")
    hint(
        if (local) {
            "Start it first with `sot up` - or pass `--up` to do both in one go."
        } else {
            "Check VESPA_URL and the remote engine, then retry."
        },
    )
    exitProcess(1)
}

/** `sot up` — start Vespa (docker compose) and deploy the app package. */
internal fun up(args: List<String>) {
    if (run("docker", "compose", "up", "-d", "vespa") != 0) return
    print("waiting for Vespa config server")
    if (!waitUntil("${Config.vespaConfigUrl}/state/v1/health")) {
        println(Ansi.red(" - timed out"))
        return
    }
    println(Ansi.green(" ready") + "; deploying ${appDir(args)} ...")
    if (deploy(args) != 0) return
    print("waiting for Vespa to serve the app")
    println(if (waitUntil("${Config.vespaUrl}/ApplicationStatus")) Ansi.green(" ready.") else Ansi.red(" - timed out"))
}

/** `sot down` — stop local Vespa. */
internal fun down() {
    run("docker", "compose", "down")
}

/** The Vespa application package to deploy: `--app`, else the repo's vespa/app. */
private fun appDir(args: List<String>): String = flag(args, "--app", "vespa/app")

/** `sot deploy` — package the Vespa app and POST it to the config server. Returns the curl exit code. */
internal fun deploy(args: List<String>): Int {
    val app = appDir(args)
    val configUrl = flag(args, "--config", Config.vespaConfigUrl)
    if (!ping("$configUrl/state/v1/health")) {
        err("Vespa config server is not reachable at $configUrl.")
        hint("Start it first: `sot up` (starts Vespa via docker compose AND deploys), or `docker compose up -d vespa` then retry.")
        return 1
    }
    val tgz = "/tmp/vespa.tgz"
    // COPYFILE_DISABLE=1 stops macOS tar from embedding `._*` AppleDouble sidecars
    // (files here carry a com.apple.quarantine xattr); Vespa would try to parse
    // `._services.xml` as XML and reject the package. No-op on Linux.
    if (run("bash", "-c", "COPYFILE_DISABLE=1 tar -czf $tgz -C '$app' .") != 0) return 1
    return run(
        "bash",
        "-c",
        "curl -fSs --data-binary @$tgz -H 'Content-Type: application/x-gzip' " +
            "$configUrl/application/v2/tenant/default/prepareandactivate",
    )
}

/**
 * `sot destroy` — wipe local sot state for a from-scratch run: the sync-state
 * cursors and Vespa's data volume. The events live in that volume; there is no
 * local db. Asks before deleting unless `--yes`.
 */
internal fun destroy(args: List<String>) {
    val state = File(Config.syncStatePath)

    println(Ansi.bold("This wipes all local sot state:"))
    if (state.exists()) hint("  rm ${state.path}") else hint("  (no sync state file at ${state.path})")
    hint("  docker compose down -v      (stops Vespa and deletes its data volume - THE event store)")

    if (!has(args, "--yes")) {
        print("Continue? [y/N] ")
        val answer = readlnOrNull()?.trim()?.lowercase()
        if (answer != "y" && answer != "yes") {
            warn("aborted")
            return
        }
    }

    runCatching { run("docker", "compose", "down", "-v") }
        .onFailure { warn("docker compose failed (${it.message}) - remove the vespa_var volume manually") }
    if (state.exists()) {
        if (state.delete()) ok("deleted ${state.path}") else err("could not delete ${state.path}")
    }
    ok("Clean slate. Next: `sot up` then `sot serve` (or `sot index`).")
}
