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
    // kind -> last time we finished a sync of that kind (epoch seconds)
    val lastSyncedAt: MutableMap<Int, Long> = mutableMapOf(),
)

@Serializable
data class SyncState(
    val relays: MutableMap<String, RelayState> = mutableMapOf(),
    val relayPool: MutableSet<String> = mutableSetOf(),
) {
    fun state(url: String) = relays.getOrPut(url) { RelayState() }

    fun cursor(url: String, kind: Int): Long? = relays[url]?.lastSyncedAt?.get(kind)

    fun mark(url: String, kind: Int, atSecs: Long) {
        state(url).lastSyncedAt[kind] = atSecs
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
