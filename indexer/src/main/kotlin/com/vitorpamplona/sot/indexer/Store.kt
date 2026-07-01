package com.vitorpamplona.sot.indexer

import com.vitorpamplona.quartz.nip01Core.store.ObservableEventStore
import com.vitorpamplona.quartz.nip01Core.store.sqlite.EventStore

/**
 * The local event store is the single source of truth. Vespa is a projection of
 * it (see [VespaProjection]). SQLite persists every synced event so re-runs only
 * need the delta (negentropy `snapshotIdsForNegentropy` / `since` cursors), and
 * `ObservableEventStore` exposes a `changes` feed that drives the projection.
 */
fun openStore(dbPath: String): ObservableEventStore =
    ObservableEventStore(EventStore(dbName = dbPath, relay = null))
