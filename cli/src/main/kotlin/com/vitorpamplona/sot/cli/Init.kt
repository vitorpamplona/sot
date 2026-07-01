package com.vitorpamplona.sot.cli

import com.vitorpamplona.sot.config.Config
import java.io.File

/**
 * `sot init` — write a commented `.env` so a first-time user can configure ports,
 * VESPA_URL, DEFAULT_OBSERVER, etc. in one place. The CLI and the sot server both
 * read this file (a real environment variable still overrides any value in it).
 */
internal fun init(args: List<String>) {
    val path = flag(args, "--path", System.getenv("SOT_ENV") ?: ".env")
    val f = File(path)
    if (f.exists() && !has(args, "--force")) {
        println("$path already exists - edit it, or re-run with --force to overwrite.")
        return
    }
    f.writeText(Config.sampleDotenv())
    println("wrote $path - edit it to configure sot (VESPA_URL, SERVER_PORT, DEFAULT_OBSERVER, ...).")
}
