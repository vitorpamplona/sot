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
@file:UseSerializers(RelayUrlSerializer::class)

package com.vitorpamplona.sot.v2.sync

import com.vitorpamplona.quartz.nip01Core.relay.normalizer.NormalizedRelayUrl
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.RelayUrlNormalizer
import kotlinx.serialization.KSerializer
import kotlinx.serialization.Serializable
import kotlinx.serialization.UseSerializers
import kotlinx.serialization.descriptors.PrimitiveKind
import kotlinx.serialization.descriptors.PrimitiveSerialDescriptor
import kotlinx.serialization.encoding.Decoder
import kotlinx.serialization.encoding.Encoder
import kotlinx.serialization.json.Json
import java.io.File

/**
 * Persist a [NormalizedRelayUrl] as its plain url string, so the state file
 * stays a readable JSON of urls while the in-memory maps stay typed. Loading
 * re-normalizes; a corrupt url makes [SyncState.load] start fresh (same as any
 * other corrupt state file).
 */
internal object RelayUrlSerializer : KSerializer<NormalizedRelayUrl> {
    override val descriptor = PrimitiveSerialDescriptor("NormalizedRelayUrl", PrimitiveKind.STRING)

    override fun serialize(
        encoder: Encoder,
        value: NormalizedRelayUrl,
    ) = encoder.encodeString(value.url)

    override fun deserialize(decoder: Decoder): NormalizedRelayUrl = RelayUrlNormalizer.normalize(decoder.decodeString())
}

/**
 * Per-relay sync bookkeeping: negentropy capability + per-scope last-synced
 * cursor. (Ported from v1's `:indexer`; the relayPool is gone — v2 has no
 * blind discovery crawl, the outbox directory is the store itself.)
 */
@Serializable
data class RelayState(
    var negentropyCapable: Boolean? = null,
    // scope key ("kinds" or "kinds:authors") -> last time we finished that sync
    // (epoch seconds). The authors are part of the key because each provider's
    // 30382 set on a relay is an independent sync scope — a shared per-kind
    // cursor would let one provider's sync since-filter every other provider
    // on that relay.
    val lastSyncedAt: MutableMap<String, Long> = mutableMapOf(),
)

/**
 * Small persisted state so periodic re-runs are cheap: which relays speak
 * negentropy and how far each scope has synced. Stored as JSON next to the
 * config. The store holds the events; this holds only the sync bookkeeping —
 * losing it costs a re-download, never correctness.
 */
@Serializable
data class SyncState(
    val relays: MutableMap<NormalizedRelayUrl, RelayState> = mutableMapOf(),
) {
    // Relay syncs run in parallel; every access to the shared maps synchronizes here.
    fun relay(relay: NormalizedRelayUrl): RelayState = synchronized(relays) { relays.getOrPut(relay) { RelayState() } }

    fun cursor(
        relay: NormalizedRelayUrl,
        scope: String,
    ): Long? = synchronized(relays) { relays[relay]?.lastSyncedAt?.get(scope) }

    fun markSynced(
        relay: NormalizedRelayUrl,
        scope: String,
        atSecs: Long,
    ) {
        synchronized(relays) { relays.getOrPut(relay) { RelayState() }.lastSyncedAt[scope] = atSecs }
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
