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

import com.vitorpamplona.quartz.nip01Core.metadata.MetadataEvent
import com.vitorpamplona.quartz.nip01Core.relay.filters.Filter
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.RelayUrlNormalizer
import com.vitorpamplona.quartz.nip01Core.signers.NostrSignerSync
import com.vitorpamplona.quartz.nip01Core.store.ObservableEventStore
import com.vitorpamplona.quartz.nip01Core.store.sqlite.DefaultIndexingStrategy
import com.vitorpamplona.quartz.nip01Core.store.sqlite.EventStore
import kotlinx.coroutines.runBlocking
import java.io.File
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

/**
 * [RelaySyncer] against a REAL in-process relay ([InProcessRelay]): NIP-77
 * negentropy and paginated fetches run the actual protocol on both sides. The
 * scenarios cover the promises the syncer makes — complete streamed ingest,
 * honest capping, forged-event rejection, incremental cursors, the pages
 * fallback, and reconcile's silent-deletion detection.
 */
class RelaySyncerTest {
    private val kind0 = Filter(kinds = listOf(MetadataEvent.KIND))

    /** Distinct 64-hex ids/authors: one hex prefix char + 63 hex digits of [i]. */
    private fun hex(
        prefix: Char,
        i: Int,
    ) = "$prefix${"%063x".format(i)}"

    private fun kind0(
        i: Int,
        createdAt: Long = 1_700_000_000L + i,
    ) = MetadataEvent(hex('e', i), hex('a', i), createdAt, emptyArray(), """{"name":"user$i"}""", "")

    private fun localStore() =
        ObservableEventStore(
            EventStore(
                File
                    .createTempFile("relay-syncer-local", ".db")
                    .also {
                        it.delete()
                        it.deleteOnExit()
                    }.path,
                // The local store's own identity - distinct from the in-process relay's url.
                RelayUrlNormalizer.normalize("ws://localhost:7777"),
                DefaultIndexingStrategy(indexFullTextSearch = false),
            ),
        )

    private fun syncer(
        relay: InProcessRelay,
        store: ObservableEventStore,
        state: SyncState,
        verifyEvents: Boolean = false,
        fetchBatch: Int = 500,
    ) = RelaySyncer(relay.client, store, state, log = { }, fetchBatch = fetchBatch, idleTimeoutMs = 15_000, verifyEvents = verifyEvents)

    @Test
    fun `negentropy streams a multi-chunk event set completely into the store`() =
        runBlocking {
            InProcessRelay().use { relay ->
                // More than two insert chunks (CHUNK_SIZE=5000), plus a remainder.
                val total = 10_047
                relay.store.batchInsert((1..total).map { kind0(it) })

                val store = localStore()
                val state = SyncState()
                try {
                    val o = syncer(relay, store, state).sync(relay.url, kind0)

                    assertEquals(total, o.downloaded, "every reconciled event is downloaded")
                    assertEquals(total, o.inserted, "every downloaded event lands in the store")
                    assertTrue(o.usedNegentropy)
                    assertEquals(total, store.count(kind0))
                    assertEquals(true, state.relay(relay.url).negentropyCapable, "a clean full sync marks the relay capable")
                } finally {
                    store.close()
                }
            }
        }

    @Test
    fun `a maxEvents-capped run stays negentropy and is not mistaken for a lossy relay`() =
        runBlocking {
            InProcessRelay().use { relay ->
                relay.store.batchInsert((1..500).map { kind0(it) })

                val store = localStore()
                val state = SyncState()
                try {
                    val o = syncer(relay, store, state, fetchBatch = 50).sync(relay.url, kind0, maxEvents = 100)

                    assertTrue(o.inserted in 100..250, "the cap bounds the ingest (got ${o.inserted})")
                    assertTrue(o.inserted < 500, "the cap actually capped")
                    assertTrue(o.usedNegentropy, "a capped shortfall must not trigger the pages fallback")
                    assertEquals(true, state.relay(relay.url).negentropyCapable, "...nor demote the relay")
                } finally {
                    store.close()
                }
            }
        }

    @Test
    fun `verification drops forged events and keeps genuinely signed ones`() =
        runBlocking {
            InProcessRelay().use { relay ->
                val signer = NostrSignerSync()
                val real = signer.sign<MetadataEvent>(1_700_000_500L, MetadataEvent.KIND, emptyArray(), """{"name":"real"}""")
                relay.store.batchInsert((1..5).map { kind0(it) } + real)

                val store = localStore()
                try {
                    val o = syncer(relay, store, SyncState(), verifyEvents = true).sync(relay.url, kind0)

                    assertEquals(6, o.downloaded, "relays are untrusted: everything arrives...")
                    assertEquals(1, o.inserted, "...but only the signed event is stored")
                    assertEquals(real.id, store.query<MetadataEvent>(kind0).single().id)
                } finally {
                    store.close()
                }
            }
        }

    @Test
    fun `the second sync pulls only the delta since the cursor`() =
        runBlocking {
            InProcessRelay().use { relay ->
                relay.store.batchInsert((1..200).map { kind0(it) })

                val store = localStore()
                val state = SyncState()
                val syncer = syncer(relay, store, state)
                try {
                    assertEquals(200, syncer.sync(relay.url, kind0).inserted)

                    // One fresh event appears on the relay (inside the cursor+slack window).
                    relay.store.insert(kind0(9999, createdAt = System.currentTimeMillis() / 1000))
                    val second = syncer.sync(relay.url, kind0)

                    assertEquals(1, second.downloaded, "the 200 old events stay outside the since window")
                    assertEquals(1, second.inserted)
                    assertEquals(201, store.count(kind0))
                } finally {
                    store.close()
                }
            }
        }

    @Test
    fun `a relay marked negentropy-incapable syncs completely over pages`() =
        runBlocking {
            InProcessRelay().use { relay ->
                relay.store.batchInsert((1..300).map { kind0(it) })

                val store = localStore()
                val state = SyncState()
                state.relay(relay.url).negentropyCapable = false
                try {
                    val o = syncer(relay, store, state).sync(relay.url, kind0)

                    assertEquals(300, o.inserted)
                    assertTrue(!o.usedNegentropy)
                    assertEquals(300, store.count(kind0))
                } finally {
                    store.close()
                }
            }
        }

    @Test
    fun `reconcile returns the relay's complete id set so silent deletions are detectable`() =
        runBlocking {
            InProcessRelay().use { relay ->
                val events = (1..100).map { kind0(it) }
                relay.store.batchInsert(events)

                val store = localStore()
                val syncer = syncer(relay, store, SyncState())
                try {
                    assertEquals(100, syncer.sync(relay.url, kind0).inserted)

                    // Matching sets: the full id set still comes back (the diff is just empty).
                    val clean = syncer.reconcile(relay.url, kind0)
                    assertTrue(clean.usedNegentropy)
                    assertEquals(events.map { it.id }.toSet(), clean.relayIds)

                    // The relay operator silently wipes 3 events - no kind:5, no trace.
                    val wiped = events.take(3).map { it.id }
                    relay.store.delete(Filter(ids = wiped))

                    val r = syncer.reconcile(relay.url, kind0)
                    assertTrue(r.usedNegentropy)
                    val relayIds = r.relayIds ?: error("a completed reconcile must return the relay's id set")
                    assertEquals(97, relayIds.size)
                    assertTrue(wiped.none { it in relayIds })

                    // The SyncPipeline diff: what we hold but the relay no longer serves gets deleted.
                    val stale = store.query<MetadataEvent>(kind0).map { it.id }.filterNot { it in relayIds }
                    assertEquals(wiped.toSet(), stale.toSet())
                    syncer.deleteFromStore(Filter(ids = stale))
                    assertEquals(97, store.count(kind0))
                } finally {
                    store.close()
                }
            }
        }
}
