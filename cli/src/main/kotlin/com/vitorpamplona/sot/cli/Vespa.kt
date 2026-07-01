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

/*
 * Local Vespa lifecycle: `up` / `down` via docker compose, and `deploy` of the
 * application package (vespa-engine/app — schema + rank profiles, next to the
 * Kotlin that depends on it) to the config server. Endpoints come from Config
 * (VESPA_URL / VESPA_CONFIG_URL), so moving Vespa's ports doesn't break these.
 */

/** Run a subprocess, echoing the command; returns its exit code. */
private fun run(vararg cmd: String): Int {
    println("$ ${cmd.joinToString(" ")}")
    return ProcessBuilder(*cmd).inheritIO().start().waitFor()
}

/** `sot up` — start Vespa (docker compose) and deploy the app package. */
internal fun up(args: List<String>) {
    if (run("docker", "compose", "up", "-d", "vespa") != 0) return
    print("waiting for Vespa config server")
    if (!waitUntil("${Config.vespaConfigUrl}/state/v1/health")) {
        println(" - timed out")
        return
    }
    println(" ready; deploying vespa-engine/app ...")
    if (deploy(args) != 0) return
    print("waiting for Vespa to serve the app")
    println(if (waitUntil("${Config.vespaUrl}/ApplicationStatus")) " ready." else " - timed out")
}

/** `sot down` — stop local Vespa. */
internal fun down() {
    run("docker", "compose", "down")
}

/** `sot deploy` — package the Vespa app and POST it to the config server. Returns the curl exit code. */
internal fun deploy(args: List<String>): Int {
    val app = flag(args, "--app", "vespa-engine/app")
    val configUrl = flag(args, "--config", Config.vespaConfigUrl)
    val tgz = "/tmp/vespa.tgz"
    if (run("bash", "-c", "tar -czf $tgz -C '$app' .") != 0) return 1
    return run(
        "bash",
        "-c",
        "curl -fSs --data-binary @$tgz -H 'Content-Type: application/x-gzip' " +
            "$configUrl/application/v2/tenant/default/prepareandactivate",
    )
}
