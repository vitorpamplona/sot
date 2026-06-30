package com.vitorpamplona.vespasearch.indexer

import com.vitorpamplona.quartz.nip01Core.core.Event
import com.vitorpamplona.quartz.nip01Core.relay.client.NostrClient
import com.vitorpamplona.quartz.nip01Core.relay.client.accessories.fetchAllPages
import com.vitorpamplona.quartz.nip01Core.relay.filters.Filter
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.NormalizedRelayUrl
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.RelayUrlNormalizer
import com.vitorpamplona.quartz.nip01Core.relay.sockets.WebsocketBuilder
import com.vitorpamplona.quartz.nip01Core.store.IEventStore
import com.vitorpamplona.quartz.nip01Core.store.ObservableEventStore
import java.util.Collections
import kotlinx.coroutines.withTimeoutOrNull

/**
 * Syncs one (relay, filter) into the store, preferring NIP-77 negentropy and
 * falling back to paginated fetch:
 *
 *  - **negentropy** (seeded with what we already hold for that filter, so only
 *    the delta transfers) when the relay accepts it and the local set is small
 *    enough to seed in memory;
 *  - **fetchAllPages(since = lastSync − slack)** otherwise — for relays/filters
 *    that exceed the relay's `negentropy.maxSyncEvents`, or that don't speak
 *    NIP-77, or whose local set is too large to seed.
 *
 * Either way it advances the per-(relay, kind) cursor in [SyncState] so the next
 * run only pulls newer events. The slack overlap absorbs back-dated events a
 * bare `since` would miss.
 */
class RelaySyncer(
    private val client: NostrClient,
    private val socketBuilder: WebsocketBuilder,
    private val store: ObservableEventStore,
    private val state: SyncState,
    private val log: (String) -> Unit,
    private val fetchBatch: Int = 500,
    private val fetchTimeoutMs: Long = 30_000,
    private val slackSecs: Long = 3600,
    private val maxNegLocal: Int = 100_000,
) {
    data class Outcome(val inserted: Int, val usedNegentropy: Boolean, val downloaded: Int)

    private fun nowSecs() = System.currentTimeMillis() / 1000

    /** Sync [filter] (single-kind) from [relayUrl] into the store. Returns inserted count. */
    suspend fun sync(relayUrl: String, filter: Filter): Outcome {
        val relay = RelayUrlNormalizer.normalizeOrNull(relayUrl) ?: return Outcome(0, false, 0)
        val url = relay.url
        val kind = filter.kinds?.firstOrNull() ?: -1
        val cursor = state.cursor(url, kind)
        val since = cursor?.let { it - slackSecs }

        val buf = Collections.synchronizedList(ArrayList<Event>())
        var usedNeg = false

        val capable = state.state(url).negentropyCapable
        val localCount = store.count(filter)
        val tryNeg = capable != false && localCount <= maxNegLocal

        if (tryNeg) {
            val local = store.query<Event>(filter)
            val stage =
                NegentropyStage(
                    relay, socketBuilder, filter, fetchBatch, maxEvents = 0,
                    onEvent = { buf.add(it) }, log = log,
                    localEvents = local, fallbackToReq = false,
                )
            stage.start()
            val downloaded = withTimeoutOrNull(fetchTimeoutMs) { stage.done.await() } ?: 0
            // Negentropy "worked" only if it delivered everything it reconciled.
            // Some relays reconcile fine but stall on the follow-up id fetch — treat
            // a partial/empty delivery as a miss and let the paginated path recover.
            val complete = !stage.fellBack && downloaded >= stage.neededCount
            if (complete) {
                usedNeg = true
                if (capable == null) state.state(url).negentropyCapable = true
            } else {
                // Don't keep paying the negentropy round-trip on a relay that won't
                // deliver through it; the cursor-based fetch keeps us incremental.
                state.state(url).negentropyCapable = false
                buf.clear()
            }
        }

        if (!usedNeg) {
            val paged = if (since != null) filter.copy(since = since) else filter
            withTimeoutOrNull(fetchTimeoutMs) {
                client.fetchAllPages(relay, listOf(paged)) { buf.add(it) }
            }
        }

        val inserted =
            store.batchInsert(ArrayList(buf)).count { it is IEventStore.InsertOutcome.Accepted }
        state.mark(url, kind, nowSecs())
        return Outcome(inserted, usedNeg, buf.size)
    }

    /** Convenience for a relay already normalized (e.g. a 10040 provider relay hint). */
    suspend fun sync(relay: NormalizedRelayUrl, filter: Filter): Outcome = sync(relay.url, filter)
}
