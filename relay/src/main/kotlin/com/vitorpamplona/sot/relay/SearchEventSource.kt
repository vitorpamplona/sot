package com.vitorpamplona.sot.relay

import com.vitorpamplona.quartz.nip01Core.core.Event
import com.vitorpamplona.quartz.nip01Core.metadata.MetadataEvent
import com.vitorpamplona.quartz.nip01Core.relay.filters.Filter
import com.vitorpamplona.quartz.nip01Core.relay.server.backend.EventSource
import com.vitorpamplona.quartz.nip01Core.relay.server.backend.RequestContext
import com.vitorpamplona.quartz.nip01Core.store.IEventStore
import com.vitorpamplona.sot.query.SearchOptions
import com.vitorpamplona.sot.query.VespaSearch
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.currentCoroutineContext
import kotlinx.coroutines.ensureActive
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.withContext

/**
 * NIP-50 search over Nostr: a `REQ` whose filter carries a `search` term is
 * answered by ranking profiles in Vespa (via :query-engine) and streaming back
 * the **original signed** kind-0 events from the indexer's local EventStore, in
 * ranked order. The ranking observer is the NIP-42-authenticated pubkey if the
 * client authenticated, else [defaultObserver].
 *
 * Filters without a `search` term are ignored here (this is a search relay); the
 * engine still sends EOSE when the flow completes.
 */
class SearchEventSource(
    private val vespa: VespaSearch,
    private val store: IEventStore,
    private val defaultObserver: String,
) : EventSource {
    override fun events(ctx: RequestContext, filters: List<Filter>): Flow<Event> =
        flow {
            val observer = ctx.authenticatedUsers.firstOrNull() ?: defaultObserver
            for (f in filters) {
                val term = f.search?.trim()
                if (term.isNullOrEmpty()) continue
                val limit = (f.limit ?: 50).coerceIn(1, 400)

                val hits =
                    withContext(Dispatchers.IO) {
                        vespa.search(term, observer, SearchOptions(hits = limit))
                    }
                if (hits.isEmpty()) continue

                // One store query for every ranked author (not one per hit). kind:0 is
                // replaceable, so the store already holds just the latest per author.
                val byAuthor =
                    withContext(Dispatchers.IO) {
                        store
                            .query<Event>(Filter(kinds = listOf(MetadataEvent.KIND), authors = hits.map { it.pubkey }))
                            .associateBy { it.pubKey }
                    }
                for (hit in hits) {
                    currentCoroutineContext().ensureActive() // honor CLOSE / disconnect
                    byAuthor[hit.pubkey]?.let { emit(it) }
                }
            }
        }
}
