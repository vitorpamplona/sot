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
package com.vitorpamplona.sot.indexer

import com.vitorpamplona.quartz.nip01Core.relay.normalizer.NormalizedRelayUrl
import kotlinx.serialization.Serializable
import kotlinx.serialization.json.Json
import java.io.File

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
    // Keyed/persisted by the normalized url STRING (this is a JSON file);
    // the API accepts only NormalizedRelayUrl so callers can't key by raw text.
    val relays: MutableMap<String, RelayState> = mutableMapOf(),
    val relayPool: MutableSet<String> = mutableSetOf(),
) {
    fun relay(relay: NormalizedRelayUrl) = relays.getOrPut(relay.url) { RelayState() }

    fun cursor(
        relay: NormalizedRelayUrl,
        scope: String,
    ): Long? = relays[relay.url]?.lastSyncedAt?.get(scope)

    fun markSynced(
        relay: NormalizedRelayUrl,
        scope: String,
        atSecs: Long,
    ) {
        relay(relay).lastSyncedAt[scope] = atSecs
    }

    companion object {
        private val json =
            Json {
                prettyPrint = true
                ignoreUnknownKeys = true
            }

        fun load(path: String): SyncState =
            runCatching { json.decodeFromString(serializer(), File(path).readText()) }
                .getOrElse { SyncState() }

        fun save(
            path: String,
            state: SyncState,
        ) {
            runCatching { File(path).writeText(json.encodeToString(serializer(), state)) }
        }
    }
}
