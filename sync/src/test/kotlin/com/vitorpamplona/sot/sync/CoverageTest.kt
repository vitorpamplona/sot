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
import com.vitorpamplona.quartz.nip10Notes.TextNoteEvent
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
    fun `coverage splits roster into outbox-known, no-outbox, synced and pending`() =
        runBlocking {
            val store = VespaEventStore(InMemoryEventIndex(), relay = RelayUrlNormalizer.normalize("ws://localhost:7777"))
            val crawl = InMemoryCrawlIndex()
            store.use {
                store.insert(providerList())
                store.insert(score(alice.pubKey, 90))
                store.insert(score(bob.pubKey, 50))
                store.insert(score(carol.pubKey, 20))
                // alice and bob advertise an outbox; carol does not.
                store.insert(relayList(alice, "wss://alice.test"))
                store.insert(relayList(bob, "wss://bob.test"))
                // We hold a post for carol (scraped from a fallback), none for the others.
                store.insert(carol.sign<TextNoteEvent>(1_300, TextNoteEvent.KIND, arrayOf(), "carol's note"))
                // alice was reconciled cleanly; bob and carol were not.
                crawl.markSynced(listOf(alice.pubKey), atSecs = 2_000)
                // All three were resolved this load (carol resolved to "no 10002").
                crawl.markOutboxChecked(listOf(alice.pubKey, bob.pubKey, carol.pubKey), atSecs = 2_000)

                val c = observerCoverage(observer.pubKey, store, crawl)
                assertEquals(1, c.providers, "one rank provider named")
                assertEquals(3, c.rosterSize, "three scored subjects")
                assertEquals(2, c.outboxKnown, "alice + bob have a 10002")
                assertEquals(1, c.noOutbox, "carol resolved to no 10002")
                assertEquals(0, c.unresolved, "everyone was resolved this load")
                assertEquals(1, c.syncedWithOutbox, "only alice reconciled cleanly")
                assertEquals(1, c.pending, "bob is outbox-known but not synced")
                assertEquals(1, c.postsForNoOutbox, "we hold a post for no-outbox carol")
            }
        }

    @Test
    fun `a scored author not yet resolved counts as unresolved, not no-outbox`() =
        runBlocking {
            val store = VespaEventStore(InMemoryEventIndex(), relay = RelayUrlNormalizer.normalize("ws://localhost:7777"))
            val crawl = InMemoryCrawlIndex()
            store.use {
                store.insert(providerList())
                store.insert(score(carol.pubKey, 20)) // no 10002, and never resolved

                val c = observerCoverage(observer.pubKey, store, crawl)
                assertEquals(1, c.rosterSize)
                assertEquals(0, c.noOutbox, "not resolved yet, so not confirmed no-outbox")
                assertEquals(1, c.unresolved, "carol is still awaiting outbox resolution")
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
