package com.vitorpamplona.sot.cli

import com.vitorpamplona.sot.config.Config

/**
 * Local Vespa lifecycle: `up` / `down` via docker compose, and `deploy` of the
 * vespa/ application package to the config server. Endpoints come from Config
 * (VESPA_URL / VESPA_CONFIG_URL), so moving Vespa's ports doesn't break these.
 */

/** Run a subprocess, echoing the command; returns its exit code. */
private fun run(vararg cmd: String): Int {
    println("$ ${cmd.joinToString(" ")}")
    return ProcessBuilder(*cmd).inheritIO().start().waitFor()
}

/** `sot up` — start Vespa (docker compose) and deploy the vespa/ app package. */
internal fun up(args: List<String>) {
    if (run("docker", "compose", "up", "-d", "vespa") != 0) return
    print("waiting for Vespa config server")
    if (!waitUntil("${Config.vespaConfigUrl}/state/v1/health")) {
        println(" - timed out"); return
    }
    println(" ready; deploying vespa/ ...")
    if (deploy(args) != 0) return
    print("waiting for Vespa to serve the app")
    println(if (waitUntil("${Config.vespaUrl}/ApplicationStatus")) " ready." else " - timed out")
}

/** `sot down` — stop local Vespa. */
internal fun down() {
    run("docker", "compose", "down")
}

/** `sot deploy` — package vespa/ and POST it to the config server. Returns the curl exit code. */
internal fun deploy(args: List<String>): Int {
    val app = flag(args, "--app", "vespa")
    val configUrl = flag(args, "--config", Config.vespaConfigUrl)
    val tgz = "/tmp/vespa.tgz"
    if (run("bash", "-c", "tar -czf $tgz -C '$app' .") != 0) return 1
    return run(
        "bash", "-c",
        "curl -fSs --data-binary @$tgz -H 'Content-Type: application/x-gzip' " +
            "$configUrl/application/v2/tenant/default/prepareandactivate",
    )
}
