package com.vitorpamplona.sot.indexer

import com.vitorpamplona.quartz.nip01Core.relay.normalizer.NormalizedRelayUrl
import com.vitorpamplona.quartz.nip01Core.store.ObservableEventStore
import com.vitorpamplona.quartz.nip01Core.store.sqlite.DefaultIndexingStrategy
import com.vitorpamplona.quartz.nip01Core.store.sqlite.EventStore

/**
 * The local event store is the single source of truth. Vespa is a projection of
 * it (see [VespaProjection]). SQLite persists every synced event so re-runs only
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

fun openStore(dbPath: String, relay: NormalizedRelayUrl): ObservableEventStore =
    ObservableEventStore(EventStore(dbPath, relay, NO_FTS))
