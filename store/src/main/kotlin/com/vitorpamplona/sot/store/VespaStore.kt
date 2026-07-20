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

import com.vitorpamplona.quartz.nip01Core.store.IEventStore
import com.vitorpamplona.sot.vespa.client.VespaEventIndex

/**
 * The library handle: a ready [IEventStore] backed by Vespa, obtained from
 * [VespaEventStores.open]. It delegates the entire [IEventStore] surface to the
 * wired store, so a consumer programs against the Quartz interface and never sees
 * the VespaEventStore(TrustProjection(VespaEventIndex, VespaProfileIndex)) stack.
 *
 * Closeable (via [IEventStore]'s AutoCloseable), so `open(...).use { ... }` works.
 */
class VespaStore internal constructor(
    /**
     * The concrete store — the [IEventStore] this handle delegates, exposed for
     * the Vespa-specific capabilities that live beyond the Quartz interface (e.g.
     * `distinctDTags`, used by trust-graph walks). Most consumers can ignore it
     * and use this handle directly as an [IEventStore].
     */
    val store: VespaEventStore,
    /**
     * The raw engine index, NOT trust-projected. Reads through it skip the
     * projection decorator — status/health metrics query it directly, since they
     * only count and never mutate trust data.
     */
    val events: VespaEventIndex,
) : IEventStore by store {
    /** The engine's feed-health status line (bulk-ingest backpressure), for progress/status output. */
    fun feedGauge(): String = events.feedGauge()
}
