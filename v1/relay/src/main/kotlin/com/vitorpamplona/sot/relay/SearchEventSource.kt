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
package com.vitorpamplona.sot.relay

import com.vitorpamplona.quartz.nip01Core.core.Event
import com.vitorpamplona.quartz.nip01Core.metadata.MetadataEvent
import com.vitorpamplona.quartz.nip01Core.relay.filters.Filter
import com.vitorpamplona.quartz.nip01Core.relay.server.backend.EventSource
import com.vitorpamplona.quartz.nip01Core.relay.server.backend.RequestContext
import com.vitorpamplona.quartz.nip01Core.store.IEventStore
import com.vitorpamplona.sot.vespa.SearchOptions
import com.vitorpamplona.sot.vespa.VespaSearch
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
    override fun events(
        ctx: RequestContext,
        filters: List<Filter>,
    ): Flow<Event> =
        flow {
            val observer = ctx.authenticatedUsers.firstOrNull() ?: defaultObserver
            for (f in filters) {
                val term = f.search?.trim()
                if (term.isNullOrEmpty()) continue
                val limit = (f.limit ?: 50).coerceIn(1, 400)

                // Non-blocking: suspends on Vespa I/O without holding a thread, so many
                // concurrent REQs don't contend for the IO pool here.
                val hits = vespa.search(term, observer, SearchOptions(hits = limit))
                if (hits.isEmpty()) continue

                // One store query for all ranked authors, indexed by pubkey. The map isn't
                // dedup (kind:0 is replaceable, so the store already returns one per author)
                // — it lets us re-emit in Vespa's *rank* order below. A streaming
                // query(filter, onEach) can't help: its callback is non-suspend (can't call
                // emit) and would deliver rows in store order, dropping the ranking.
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
