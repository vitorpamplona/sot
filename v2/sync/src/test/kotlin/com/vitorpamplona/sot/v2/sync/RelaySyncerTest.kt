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
package com.vitorpamplona.sot.v2.sync

import com.vitorpamplona.quartz.nip01Core.metadata.MetadataEvent
import com.vitorpamplona.quartz.nip01Core.relay.filters.Filter
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.RelayUrlNormalizer
import com.vitorpamplona.quartz.nip01Core.signers.NostrSignerSync
import com.vitorpamplona.sot.v2.store.VespaEventStore
import com.vitorpamplona.sot.v2.vespa.InMemoryEventIndex
import kotlinx.coroutines.runBlocking
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

/**
 * Smoke tests for the ported [RelaySyncer] over the v2 stack — the full
 * behavioral suite lives with v1's `:indexer` (the port is verbatim except
 * the store type); what needs re-proving here is that the real protocol runs
 * against a [VespaEventStore] on BOTH sides, and that the verify gate holds.
 */
class RelaySyncerTest {
    private val kind0 = Filter(kinds = listOf(MetadataEvent.KIND))

    private fun localStore() = VespaEventStore(InMemoryEventIndex(), relay = RelayUrlNormalizer.normalize("ws://localhost:7777"))

    private fun signed(
        i: Int,
        at: Long = 1_700_000_000L + i,
    ): MetadataEvent = NostrSignerSync().sign(at, MetadataEvent.KIND, emptyArray(), """{"name":"user$i"}""")

    @Test
    fun `negentropy syncs a relay's set into the Vespa store completely`() =
        runBlocking {
            InProcessNet().use { net ->
                val events = (1..40).map { signed(it) }
                net.store("wss://a.test").let { s -> events.forEach { s.insert(it) } }

                localStore().use { store ->
                    val state = SyncState()
                    val o = RelaySyncer(net.client, store, state, log = { }, idleTimeoutMs = 15_000).sync(net.url("wss://a.test"), kind0)

                    assertEquals(40, o.inserted)
                    assertTrue(o.usedNegentropy)
                    assertEquals(40, store.count(kind0))
                    assertEquals(true, state.relay(net.url("wss://a.test")).negentropyCapable)
                }
            }
        }

    @Test
    fun `forged events are dropped before the store`() =
        runBlocking {
            InProcessNet().use { net ->
                val good = signed(1)
                // A different author's shape with a garbage signature — the relay serves it anyway.
                val forged = MetadataEvent(good.id.replaceFirstChar { 'f' }, NostrSignerSync().pubKey, good.createdAt + 1, emptyArray(), """{"name":"evil"}""", "f".repeat(128))
                net.store("wss://b.test").insert(good)
                net.store("wss://b.test").insert(forged)

                localStore().use { store ->
                    val o = RelaySyncer(net.client, store, SyncState(), log = { }, idleTimeoutMs = 15_000).sync(net.url("wss://b.test"), kind0)

                    assertEquals(1, o.inserted, "only the verifiable event lands")
                    assertEquals(listOf(good.id), store.query<MetadataEvent>(kind0).map { it.id })
                }
            }
        }
}
