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

/**
 * sot configuration. A value resolves in order:
 *   1. a real environment variable (an explicit override always wins)
 *   2. a matching key in a `.env` file (path from `SOT_ENV`, else `./.env`)
 *   3. the built-in default
 *
 * The key set is deliberately small. There is no EVENTS_DB (Vespa is the
 * store) and no DEFAULT_OBSERVER (the house account plays that role, with a
 * home relay so its trust chain can bootstrap). There is also no INDEX_RELAYS
 * key: the identity's stored kind-10086 IS the indexer configuration. `sot
 * init` seeds it, and the operator supersedes it from any Nostr client.
 */
object Config {
    /** key, default, one-line doc — drives the interactive `sot init` and the `.env` it writes. */
    val KEYS: List<Triple<String, String, String>> =
        listOf(
            Triple("VESPA_URL", "http://localhost:8080", "Vespa query/document endpoint (the event store)"),
            Triple("VESPA_CONFIG_URL", "http://localhost:19071", "Vespa config server (deploy + readiness)"),
            Triple("VESPA_PORT", "8080", "host port docker publishes Vespa's query/document API on (keep VESPA_URL in sync)"),
            Triple("VESPA_CONFIG_PORT", "19071", "host port docker publishes Vespa's config server on (keep VESPA_CONFIG_URL in sync)"),
            Triple("SERVER_PORT", "7777", "port for the sot server (NIP-50 relay + NIP-11)"),
            Triple("SERVER_URL", "http://localhost:7777", "public http url of the server"),
            Triple("RELAY_URL", "ws://localhost:7777", "public ws url the relay advertises (NIP-42 + the identity's 10002)"),
            Triple("SYNC_INTERVAL", "15", "minutes between background sync passes in `sot serve`; 0 = serve-only"),
            Triple("SYNC_STATE", "sync-state.json", "where per-relay sync cursors persist (losing it costs a re-download, never correctness)"),
            Triple("SYNC_RECORDS", "false", "also sync searchable CONTENT (every indexable kind) for scored authors, not just trust scores - the full firehose, off by default"),
            Triple("SYNC_RECORD_BAND", "2000", "records plane: scored authors per trust-score band (highest-score bands sync first)"),
            Triple(
                "SEED_RELAYS",
                "wss://purplepag.es,wss://relay.damus.io,wss://nos.lol",
                "comma-separated relays swept for kind-10040 hints (who the observers are; their outbox stays the authority)",
            ),
            Triple("HOUSE_NPUB", "", "the house account (npub or hex): the observer behind every unauthenticated search"),
            Triple("HOUSE_RELAY", "", "the house account's home relay — where its first kind-10002 is synced from"),
            Triple("QUARTZ_LOG_LEVEL", "ERROR", "min level for the Nostr library's stderr diagnostics: DEBUG, INFO, WARN, or ERROR"),
            Triple("SERVER_NAME", "sot", "service name (NIP-11 + the identity's kind 0)"),
            Triple("SERVER_DESCRIPTION", "Search over Trust - a web-of-trust Nostr search relay", "service description (NIP-11 + kind 0)"),
            Triple("SERVER_ICON", "", "service icon url (NIP-11 + kind 0)"),
            Triple("SERVER_PUBKEY", "", "admin contact pubkey, hex (NIP-11 pubkey)"),
            Triple("SERVER_NSEC", "", "this relay's own identity (nsec or hex secret key): NIP-11 self, NIP-42 auth upstream, signs the kind 0/10002/10086 self-publish; `sot init` generates one"),
        )
    private val defaults = KEYS.associate { it.first to it.second }

    private val dotenv: Map<String, String> = loadDotenv(System.getenv("SOT_ENV") ?: ".env")

    /** Parse a `.env` file: KEY=value lines, `export` prefixes and quotes stripped, comments skipped. */
    internal fun loadDotenv(path: String): Map<String, String> {
        val f = File(path)
        if (!f.exists()) return emptyMap()
        return f
            .readLines()
            .mapNotNull { raw ->
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
    val syncIntervalMinutes get() = env("SYNC_INTERVAL").toInt()
    val syncStatePath get() = env("SYNC_STATE")
    val syncRecords get() = env("SYNC_RECORDS").trim().lowercase() in setOf("true", "1", "yes", "on")
    val syncRecordBand get() = env("SYNC_RECORD_BAND").toIntOrNull()?.coerceAtLeast(1) ?: 2_000
    val seedRelays get() = env("SEED_RELAYS").split(",").map { it.trim() }.filter { it.isNotEmpty() }
    val houseNpub get() = env("HOUSE_NPUB")
    val houseRelay get() = env("HOUSE_RELAY")
    val quartzLogLevel get() = env("QUARTZ_LOG_LEVEL")
    val serverName get() = env("SERVER_NAME")
    val serverDescription get() = env("SERVER_DESCRIPTION")
    val serverIcon get() = env("SERVER_ICON")
    val serverPubkey get() = env("SERVER_PUBKEY")
    val serverNsec get() = env("SERVER_NSEC")

    /**
     * The commented `.env` text for a fresh setup, with [answers] (from the
     * interactive `sot init`) overriding the registered defaults.
     */
    fun renderDotenv(answers: Map<String, String> = emptyMap()): String =
        buildString {
            appendLine("# sot configuration. Picked up by every `sot` command.")
            appendLine("# A real environment variable of the same name overrides any value here.")
            appendLine()
            for ((k, default, doc) in KEYS) {
                appendLine("# $doc")
                appendLine("$k=${answers[k] ?: default}")
                appendLine()
            }
        }
}
