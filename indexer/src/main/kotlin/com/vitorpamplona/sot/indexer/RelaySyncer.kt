package com.vitorpamplona.sot.indexer

import com.vitorpamplona.quartz.nip01Core.core.Event
import com.vitorpamplona.quartz.nip01Core.relay.client.NostrClient
import com.vitorpamplona.quartz.nip01Core.relay.client.accessories.fetchAllPages
import com.vitorpamplona.quartz.nip01Core.relay.client.accessories.negentropySyncOrFetch
import com.vitorpamplona.quartz.nip01Core.relay.filters.Filter
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.NormalizedRelayUrl
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.RelayUrlNormalizer
import com.vitorpamplona.quartz.nip01Core.store.IEventStore
import com.vitorpamplona.quartz.nip01Core.store.ObservableEventStore
import java.util.Collections
import java.util.concurrent.atomic.AtomicInteger
import kotlinx.coroutines.withTimeoutOrNull

/**
 * Syncs one (relay, filter) into the store using Quartz's generalized
 * `negentropySyncOrFetch`: it prefers NIP-77 negentropy (windowing around the
 * relay's `max_sync_events`, downloading reconciled ids through a bounded pool
 * of REQs) and transparently falls back to paginated fetch — with id-dedup — on
 * relays that can't reconcile. No hand-rolled state machine here anymore.
 *
 * Incrementality is the persisted `since` cursor: each run scopes the filter to
 * `lastSync − slack`, so only new events transfer (the slack overlap absorbs
 * back-dated events). The cursor advances after every successful sync.
 */
class RelaySyncer(
    private val client: NostrClient,
    private val store: ObservableEventStore,
    private val state: SyncState,
    private val log: (String) -> Unit,
    private val fetchBatch: Int = 500,
    private val idleTimeoutMs: Long = 30_000,
    private val slackSecs: Long = 3600,
) {
    data class Outcome(val inserted: Int, val usedNegentropy: Boolean, val downloaded: Int)

    private fun nowSecs() = System.currentTimeMillis() / 1000

    suspend fun sync(relayUrl: String, filter: Filter, maxEvents: Int = 0): Outcome {
        val relay = RelayUrlNormalizer.normalizeOrNull(relayUrl) ?: return Outcome(0, false, 0)
        val url = relay.url
        val kind = filter.kinds?.firstOrNull() ?: -1
        // Cursor scope includes authors so per-provider 30382 syncs don't share a cursor.
        val scope = kind.toString() + (filter.authors?.let { ":" + it.joinToString(",") } ?: "")
        val firstSync = state.cursor(url, scope) == null
        val since = state.cursor(url, scope)?.let { it - slackSecs }
        val scoped = if (since != null) filter.copy(since = since) else filter

        val buf = Collections.synchronizedList(ArrayList<Event>())
        var usedNeg = false
        val capable = state.state(url).negentropyCapable

        suspend fun pageInto() {
            // Honor the ingest cap on the fallback path too (Filter.limit stops fetchAllPages).
            val paged = if (maxEvents > 0) scoped.copy(limit = maxEvents) else scoped
            withTimeoutOrNull(idleTimeoutMs) { client.fetchAllPages(relay, listOf(paged)) { buf.add(it) } }
        }

        if (capable == false) {
            // Known not to reconcile usefully here — go straight to paginated `since`.
            pageInto()
        } else {
            val need = AtomicInteger(0)
            val res =
                runCatching {
                    client.negentropySyncOrFetch(
                        relay, scoped,
                        maxEvents = maxEvents,
                        fetchBatch = fetchBatch,
                        idleTimeoutMs = idleTimeoutMs,
                        onProgress = { needSoFar, _ -> need.updateAndGet { maxOf(it, needSoFar) } },
                    ) { buf.add(it) }
                }.getOrElse { log("  ! $url sync failed: ${it.message}"); null }
            usedNeg = res?.pagedFallback == false

            // The library pages only on a NEG-ERR. A relay can also reconcile to
            // nothing / deliver < reconciled without erroring (purplepag.es on
            // 10040). On the FIRST sync of a (relay, kind), verify a suspicious
            // empty/partial result with pages; if pages find data, mark the relay
            // pages-only for this kind so future runs skip the wasted round-trip.
            val suspicious = res != null && !res.pagedFallback &&
                (need.get() > res.downloaded || (res.downloaded == 0 && firstSync))
            if (suspicious) {
                val before = buf.size
                pageInto()
                if (buf.size > before) {
                    state.state(url).negentropyCapable = false
                    usedNeg = false
                    log("  [$url] negentropy unreliable for kind $kind - using pages")
                }
            } else if (usedNeg && (res?.downloaded ?: 0) > 0 && capable == null) {
                state.state(url).negentropyCapable = true
            }
        }

        val inserted =
            store.batchInsert(ArrayList(buf)).count { it is IEventStore.InsertOutcome.Accepted }
        state.mark(url, scope, nowSecs())
        return Outcome(inserted, usedNegentropy = usedNeg, buf.size)
    }

    suspend fun sync(relay: NormalizedRelayUrl, filter: Filter, maxEvents: Int = 0): Outcome =
        sync(relay.url, filter, maxEvents)
}
