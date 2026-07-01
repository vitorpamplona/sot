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
import com.vitorpamplona.quartz.nip01Core.store.ObservableEventStore
import com.vitorpamplona.quartz.nip01Core.store.sqlite.DefaultIndexingStrategy
import com.vitorpamplona.quartz.nip01Core.store.sqlite.EventStore

/**
 * The local event store is the single source of truth. Vespa is a projection of
 * it: [VespaProjection] subscribes to the `changes` feed below, maps each event
 * to a plain object, and writes it via :vespa-engine's schema-aware VespaClient.
 * SQLite persists every synced event so re-runs only
 * need the delta (negentropy `snapshotIdsForNegentropy` / `since` cursors), and
 * `ObservableEventStore` exposes a `changes` feed that drives the projection.
 *
 * [relay] is this store's own relay identity — needed so NIP-62 Request-to-Vanish
 * events scoped to a specific relay are honored (vs. `ALL_RELAYS`). Search lives
 * in Vespa, so the SQLite full-text index is dead weight — turn it off ([NO_FTS])
 * to skip building/maintaining the FTS table on every insert. Every opener of the
 * shared DB must use the same strategy.
 */
val NO_FTS = DefaultIndexingStrategy(indexFullTextSearch = false)

fun openStore(dbPath: String, relay: NormalizedRelayUrl): ObservableEventStore = ObservableEventStore(EventStore(dbPath, relay, NO_FTS))
