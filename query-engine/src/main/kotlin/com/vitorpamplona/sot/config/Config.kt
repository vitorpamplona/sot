package com.vitorpamplona.sot.config

import java.io.File

/**
 * Central configuration for every sot component. A value resolves in order:
 *   1. a real environment variable (an explicit override always wins)
 *   2. a matching key in a `.env` file (path from `SOT_ENV`, else `./.env`)
 *   3. the built-in default
 *
 * One place for every host/port default — so `localhost` strings don't drift
 * across modules — and a single `.env` (see `sot init`) configures the CLI and
 * the http/relay services alike.
 */
object Config {
    /** key, default, one-line doc — drives `sot init` / `.env.example`. */
    val KEYS: List<Triple<String, String, String>> =
        listOf(
            Triple("VESPA_URL", "http://localhost:8080", "Vespa query/document endpoint"),
            Triple("VESPA_CONFIG_URL", "http://localhost:19071", "Vespa config server (deploy + readiness)"),
            Triple("SERVER_PORT", "7777", "port for the sot server (web UI + /search API + NIP-50 relay)"),
            Triple("SERVER_URL", "http://localhost:7777", "public http url of the server (web UI base + status)"),
            Triple("RELAY_URL", "ws://localhost:7777", "public ws url the relay advertises (NIP-42)"),
            Triple("EVENTS_DB", "events.db", "SQLite event store shared by indexer + server"),
            Triple("DEFAULT_OBSERVER", "", "fallback web-of-trust observer (hex pubkey) when none is given"),
        )
    private val defaults = KEYS.associate { it.first to it.second }

    private val dotenv: Map<String, String> = loadDotenv(System.getenv("SOT_ENV") ?: ".env")

    private fun loadDotenv(path: String): Map<String, String> {
        val f = File(path)
        if (!f.exists()) return emptyMap()
        return f.readLines().mapNotNull { raw ->
            val line = raw.trim().removePrefix("export ").trim()
            if (line.isEmpty() || line.startsWith("#") || "=" !in line) return@mapNotNull null
            val k = line.substringBefore("=").trim()
            val v = line.substringAfter("=").trim().trim('"', '\'')
            if (k.isEmpty()) null else k to v
        }.toMap()
    }

    /** Resolve [key]: real env, else `.env`, else its registered default. */
    fun env(key: String): String = System.getenv(key) ?: dotenv[key] ?: defaults[key] ?: ""

    val vespaUrl get() = env("VESPA_URL")
    val vespaConfigUrl get() = env("VESPA_CONFIG_URL")
    val serverPort get() = env("SERVER_PORT").toInt()
    val serverUrl get() = env("SERVER_URL")
    val relayUrl get() = env("RELAY_URL")
    val eventsDb get() = env("EVENTS_DB")
    val defaultObserver get() = env("DEFAULT_OBSERVER")

    /** A commented `.env` seed for a fresh setup (used by `sot init`). */
    fun sampleDotenv(): String =
        buildString {
            appendLine("# sot configuration. Picked up by the CLI and the http/relay services.")
            appendLine("# A real environment variable of the same name overrides any value here.")
            appendLine()
            for ((k, v, doc) in KEYS) {
                appendLine("# $doc")
                appendLine("$k=$v")
                appendLine()
            }
        }
}
