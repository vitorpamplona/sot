package com.vitorpamplona.vespasearch.indexer

import java.io.File
import kotlinx.serialization.Serializable
import kotlinx.serialization.json.Json

/**
 * Small persisted state so periodic re-runs are cheap and don't rediscover
 * everything:
 *  - [relays]: per-relay negentropy capability + per-kind last-synced cursor
 *  - [relayPool]: every relay URL discovered via kind-10002 (stage C)
 *
 * Stored as JSON next to the event DB. The event store holds the events; this
 * holds the sync bookkeeping.
 */
@Serializable
data class RelayState(
    var negentropyCapable: Boolean? = null,
    // scope key ("kind" or "kind:author") -> last time we finished that sync (epoch seconds).
    // The author is part of the key because each provider's 30382 set on a relay is an
    // independent sync scope — a shared per-kind cursor would let one provider's sync
    // since-filter every other provider on that relay.
    val lastSyncedAt: MutableMap<String, Long> = mutableMapOf(),
)

@Serializable
data class SyncState(
    val relays: MutableMap<String, RelayState> = mutableMapOf(),
    val relayPool: MutableSet<String> = mutableSetOf(),
) {
    fun state(url: String) = relays.getOrPut(url) { RelayState() }

    fun cursor(url: String, scope: String): Long? = relays[url]?.lastSyncedAt?.get(scope)

    fun mark(url: String, scope: String, atSecs: Long) {
        state(url).lastSyncedAt[scope] = atSecs
    }

    companion object {
        private val json = Json { prettyPrint = true; ignoreUnknownKeys = true }

        fun load(path: String): SyncState =
            runCatching { json.decodeFromString(serializer(), File(path).readText()) }
                .getOrElse { SyncState() }

        fun save(path: String, state: SyncState) {
            runCatching { File(path).writeText(json.encodeToString(serializer(), state)) }
        }
    }
}
