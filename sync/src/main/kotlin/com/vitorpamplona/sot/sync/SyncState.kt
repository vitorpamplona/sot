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

package com.vitorpamplona.sot.sync

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
 * re-normalizes. A corrupt url makes [SyncState.load] start fresh, the same as
 * any other corrupt state file.
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
 * Per-relay sync bookkeeping: negentropy capability plus a per-scope
 * last-synced cursor. There is no blind discovery crawl and no relay pool; the
 * outbox directory is the store itself.
 */
@Serializable
data class RelayState(
    var negentropyCapable: Boolean? = null,
    // scope key ("kinds" or "kinds:authors") -> last time we finished that
    // sync (epoch seconds). The authors are part of the key because each
    // provider's 30382 set on a relay is an independent sync scope. A shared
    // per-kind cursor would let one provider's sync since-filter every other
    // provider on that relay.
    val lastSyncedAt: MutableMap<String, Long> = mutableMapOf(),
)

/**
 * A one-line summary of the most recent completed pass, persisted so a separate
 * `sot status` process can report sync freshness and throughput (the live
 * per-pass counters otherwise exist only in the running `serve` process).
 */
@Serializable
data class PassSummary(
    // epoch seconds when the pass finished
    val endedAtSecs: Long,
    val durationSecs: Long,
    val received: Long,
    val inserted: Long,
)

/**
 * Small persisted state that keeps periodic re-runs cheap: which relays speak
 * negentropy and how far each scope has synced. Stored as JSON next to the
 * config. The store holds the events; this holds only the sync bookkeeping.
 * Losing it costs a re-download, never correctness.
 */
@Serializable
data class SyncState(
    val relays: MutableMap<NormalizedRelayUrl, RelayState> = mutableMapOf(),
    // Relays that failed to become reachable -> the epoch-ms UNTIL which we skip
    // them (see [DeadRelayCache]). Persisted so the skip survives across passes
    // (the discovery crawl turns up thousands of dead relays; re-paying a connect
    // timeout on each one every pass is the cost this avoids). TTL-based ON
    // PURPOSE: a relay's temporary downtime expires and gets re-checked, so it is
    // never a permanent ban.
    val deadUntil: MutableMap<NormalizedRelayUrl, Long> = mutableMapOf(),
    // The last completed pass, for `sot status` freshness/throughput. Null until the first pass ends.
    var lastPass: PassSummary? = null,
) {
    /** Record the just-finished pass (called from the save path at pass end). */
    fun recordPass(
        endedAtSecs: Long,
        durationSecs: Long,
        received: Long,
        inserted: Long,
    ) {
        lastPass = PassSummary(endedAtSecs, durationSecs, received, inserted)
    }

    // Relay syncs run in parallel; every access to the shared maps synchronizes here.
    fun relay(relay: NormalizedRelayUrl): RelayState = synchronized(relays) { relays.getOrPut(relay) { RelayState() } }

    /** Dead (skip it) if a live mark exists; an expired mark is dropped and the relay retried. */
    fun isRelayDead(
        relay: NormalizedRelayUrl,
        nowMs: Long,
    ): Boolean =
        synchronized(deadUntil) {
            val until = deadUntil[relay] ?: return false
            if (nowMs < until) return true
            deadUntil.remove(relay, until) // expired — retry, but don't clobber a fresher mark
            false
        }

    fun markRelayDead(
        relay: NormalizedRelayUrl,
        untilMs: Long,
    ) {
        synchronized(deadUntil) { deadUntil[relay] = untilMs }
    }

    /** How many relays are dead right now (expired marks don't count). */
    fun activeDeadCount(nowMs: Long): Int = synchronized(deadUntil) { deadUntil.count { it.value > nowMs } }

    /** Drop marks that expired before [nowMs] so the state file doesn't accumulate stale dead relays. */
    private fun pruneDeadRelays(nowMs: Long) = synchronized(deadUntil) { deadUntil.values.removeAll { it <= nowMs } }

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
                .also { it.pruneDeadRelays(System.currentTimeMillis()) }

        fun save(
            path: String,
            state: SyncState,
        ) {
            runCatching { File(path).writeText(json.encodeToString(serializer(), state)) }
        }
    }
}
