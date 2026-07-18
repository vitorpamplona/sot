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
package com.vitorpamplona.sot.sync

import com.vitorpamplona.quartz.nip01Core.core.HexKey
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.RelayUrlNormalizer
import com.vitorpamplona.quartz.nip01Core.signers.NostrSignerSync
import com.vitorpamplona.quartz.nip65RelayList.AdvertisedRelayListEvent
import com.vitorpamplona.quartz.nip65RelayList.tags.AdvertisedRelayInfo
import com.vitorpamplona.quartz.nip65RelayList.tags.AdvertisedRelayType
import com.vitorpamplona.quartz.nip85TrustedAssertions.list.TrustProviderListEvent
import com.vitorpamplona.quartz.nip85TrustedAssertions.users.ContactCardEvent
import com.vitorpamplona.sot.store.VespaEventStore
import com.vitorpamplona.sot.vespa.InMemoryCrawlIndex
import com.vitorpamplona.sot.vespa.InMemoryEventIndex
import kotlinx.coroutines.runBlocking
import kotlin.test.Test
import kotlin.test.assertEquals

/**
 * The per-observer coverage snapshot: roster (scored subjects) split into
 * outbox-known vs no-outbox, with the synced/pending/posts numerators derived by
 * intersecting the roster with the crawl ledger and the stored 10002s/content.
 */
class CoverageTest {
    private val observer = NostrSignerSync()
    private val service = NostrSignerSync()
    private val alice = NostrSignerSync() // outbox known + synced
    private val bob = NostrSignerSync() // outbox known, not synced (pending)
    private val carol = NostrSignerSync() // no outbox, but we hold a post

    private fun relayList(
        signer: NostrSignerSync,
        vararg writeRelays: String,
    ) = AdvertisedRelayListEvent.create(writeRelays.map { AdvertisedRelayInfo(RelayUrlNormalizer.normalize(it), AdvertisedRelayType.BOTH) }, signer, 1_000)

    private fun providerList() =
        observer.sign<TrustProviderListEvent>(
            1_000,
            TrustProviderListEvent.KIND,
            arrayOf(arrayOf("30382:rank", service.pubKey, RelayUrlNormalizer.normalize("wss://p.test").url)),
            "",
        )

    private fun score(
        subject: HexKey,
        rank: Int,
    ) = service.sign<ContactCardEvent>(1_100, ContactCardEvent.KIND, arrayOf(arrayOf("d", subject), arrayOf("rank", "$rank")), "")

    @Test
    fun `completion is uniform - proxy-synced no-outbox authors count as fully synced`() =
        runBlocking {
            val store = VespaEventStore(InMemoryEventIndex(), relay = RelayUrlNormalizer.normalize("ws://localhost:7777"))
            val crawl = InMemoryCrawlIndex()
            store.use {
                store.insert(providerList())
                store.insert(score(alice.pubKey, 90))
                store.insert(score(bob.pubKey, 50))
                store.insert(score(carol.pubKey, 20))
                // alice and bob advertise their own outbox; carol does not (rides the popular relays).
                store.insert(relayList(alice, "wss://alice.test"))
                store.insert(relayList(bob, "wss://bob.test"))
                // alice synced from her outbox; carol synced from the popular-relay PROXY.
                // Both count the same. bob is routed but not yet complete.
                crawl.markSynced(listOf(alice.pubKey, carol.pubKey), atSecs = 2_000)
                crawl.markOutboxChecked(listOf(alice.pubKey, bob.pubKey, carol.pubKey), atSecs = 2_000)

                val c = observerCoverage(observer.pubKey, store, crawl)
                assertEquals(3, c.rosterSize, "three scored subjects")
                assertEquals(2, c.withOwnOutbox, "alice + bob advertise a 10002")
                assertEquals(2, c.synced, "alice (own outbox) + carol (proxy) both fully synced")
                assertEquals(1, c.pending, "only bob is left")
                assertEquals(0, c.unreached, "everyone was routed at least once")
            }
        }

    @Test
    fun `a scored author never routed counts as unreached`() =
        runBlocking {
            val store = VespaEventStore(InMemoryEventIndex(), relay = RelayUrlNormalizer.normalize("ws://localhost:7777"))
            val crawl = InMemoryCrawlIndex()
            store.use {
                store.insert(providerList())
                store.insert(score(carol.pubKey, 20)) // scored, but never routed for content

                val c = observerCoverage(observer.pubKey, store, crawl)
                assertEquals(1, c.rosterSize)
                assertEquals(0, c.synced)
                assertEquals(1, c.pending)
                assertEquals(1, c.unreached, "carol has not been routed yet")
            }
        }

    @Test
    fun `an observer with no 10040 has an empty roster`() =
        runBlocking {
            val store = VespaEventStore(InMemoryEventIndex(), relay = RelayUrlNormalizer.normalize("ws://localhost:7777"))
            val crawl = InMemoryCrawlIndex()
            store.use {
                val c = observerCoverage(observer.pubKey, store, crawl)
                assertEquals(0, c.providers)
                assertEquals(0, c.rosterSize)
                assertEquals(0, c.pending)
            }
        }
}
