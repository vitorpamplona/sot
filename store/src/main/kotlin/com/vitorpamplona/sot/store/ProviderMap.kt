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
package com.vitorpamplona.sot.store

import com.vitorpamplona.quartz.nip01Core.core.Event
import com.vitorpamplona.quartz.nip85TrustedAssertions.list.TrustProviderListEvent
import com.vitorpamplona.quartz.nip85TrustedAssertions.list.tags.ProviderTypes
import com.vitorpamplona.sot.vespa.client.EventIndex
import com.vitorpamplona.sot.vespa.doc.EventDoc
import com.vitorpamplona.sot.vespa.query.EventQuery

/**
 * The NIP-85 observer-attribution map: `service key -> observer`, derived from
 * every stored kind-10040's `30382:rank` entries. A 30382 is SIGNED by a service
 * key, but its score is credited to the OBSERVER: the 10040 author who named that
 * service. This is the one place that link is resolved.
 *
 * [get] is CACHED across a pass. The map only changes when a 10040 is written or
 * removed, so a run of single-30382 publishes (each re-deriving its subject) pays
 * the full 10040 scan ONCE, not per event. Every mutation path that touches a
 * 10040 [invalidate]s it. It is safe as a plain @Volatile field because every
 * caller runs under [TrustProjection]'s store single-writer lock.
 */
internal class ProviderMap(
    private val inner: EventIndex,
) {
    @Volatile private var cached: Map<String, String>? = null

    suspend fun get(): Map<String, String> =
        cached ?: rankProviders(inner.search(EventQuery(kinds = listOf(TrustProviderListEvent.KIND))))
            .toMap()
            .also { cached = it }

    /** Drop the cache; the next [get] rebuilds. Call after any 10040 write/remove. */
    fun invalidate() {
        cached = null
    }

    companion object {
        /** Every 10040 doc's `30382:rank` entries as `service key -> observer (the 10040 author)` pairs. */
        private fun rankProviders(listDocs: List<EventDoc>): List<Pair<String, String>> =
            listDocs
                .mapNotNull { Event.fromJsonOrNull(it.toEventJson()) as? TrustProviderListEvent }
                .flatMap { list -> list.serviceProviders().filter { it.service == ProviderTypes.rank }.map { it.pubkey to list.pubKey } }

        /** The distinct rank-service keys named across a batch of 10040 lists. */
        fun rankServicesOf(listDocs: List<EventDoc>): List<String> = rankProviders(listDocs).map { it.first }.distinct()
    }
}
