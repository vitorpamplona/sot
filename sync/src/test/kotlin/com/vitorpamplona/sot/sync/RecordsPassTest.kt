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
import com.vitorpamplona.quartz.nip01Core.relay.filters.Filter
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.RelayUrlNormalizer
import com.vitorpamplona.quartz.nip01Core.signers.NostrSignerSync
import com.vitorpamplona.quartz.nip09Deletions.DeletionEvent
import com.vitorpamplona.quartz.nip10Notes.TextNoteEvent
import com.vitorpamplona.quartz.nip65RelayList.AdvertisedRelayListEvent
import com.vitorpamplona.quartz.nip65RelayList.tags.AdvertisedRelayInfo
import com.vitorpamplona.quartz.nip65RelayList.tags.AdvertisedRelayType
import com.vitorpamplona.quartz.nip85TrustedAssertions.list.TrustProviderListEvent
import com.vitorpamplona.quartz.nip85TrustedAssertions.users.ContactCardEvent
import com.vitorpamplona.sot.store.VespaEventStore
import com.vitorpamplona.sot.vespa.InMemoryEventIndex
import kotlinx.coroutines.runBlocking
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

/**
 * The records plane end to end, over the same in-process relay network the
 * scores plane is tested on. It proves the promise this pass exists for: once
 * trust has scored some authors, their actual searchable content (kind-1 notes
 * here) is pulled from their outboxes into the local store, ready to index —
 * and their outbox deletions (kind 5) are honoured.
 */
class RecordsPassTest {
    private val observer = NostrSignerSync()
    private val service = NostrSignerSync()
    private val bob = NostrSignerSync()
    private val carol = NostrSignerSync()

    private val index = "wss://index.test"
    private val outbox = "wss://outbox.test"
    private val provider = "wss://provider.test"
    private val bobOutbox = "wss://bob.test"
    private val carolOutbox = "wss://carol.test"

    private fun relayList(
        signer: NostrSignerSync,
        at: Long,
        vararg writeRelays: String,
    ) = AdvertisedRelayListEvent.create(writeRelays.map { AdvertisedRelayInfo(RelayUrlNormalizer.normalize(it), AdvertisedRelayType.BOTH) }, signer, at)

    private fun providerList(
        signer: NostrSignerSync,
        service: HexKey,
        relay: String,
        at: Long,
    ) = signer.sign<TrustProviderListEvent>(at, TrustProviderListEvent.KIND, arrayOf(arrayOf("30382:rank", service, RelayUrlNormalizer.normalize(relay).url)), "")

    private fun score(
        signer: NostrSignerSync,
        subject: HexKey,
        rank: Int,
        at: Long,
    ) = signer.sign<ContactCardEvent>(at, ContactCardEvent.KIND, arrayOf(arrayOf("d", subject), arrayOf("rank", "$rank")), "")

    private fun note(
        signer: NostrSignerSync,
        content: String,
        at: Long,
    ) = signer.sign<TextNoteEvent>(at, TextNoteEvent.KIND, arrayOf(), content)

    private fun localStore() = VespaEventStore(InMemoryEventIndex(), relay = RelayUrlNormalizer.normalize("ws://localhost:7777"))

    private fun trustSync(
        net: InProcessNet,
        store: VespaEventStore,
    ): TrustSync {
        val opts = SyncOptions(concurrency = 4, fetchTimeoutMs = 15_000)
        val syncer = RelaySyncer(net.client, store, SyncState(), log = { }, idleTimeoutMs = opts.fetchTimeoutMs)
        return TrustSync(syncer, store, opts, log = { })
    }

    private suspend fun VespaEventStore.notesBy(author: HexKey): List<String> = query<TextNoteEvent>(Filter(kinds = listOf(TextNoteEvent.KIND), authors = listOf(author))).map { it.content }

    /** Scores plane scores bob and carol; the records plane then pulls their notes from their own outboxes. */
    @Test
    fun `records plane indexes the scored authors' content`() =
        runBlocking {
            InProcessNet().use { net ->
                // Trust chain: observer -> outbox 10040 -> provider scores bob (60) and carol (40).
                net.store(index).insert(relayList(observer, at = 1_000, outbox))
                net.store(outbox).insert(providerList(observer, service.pubKey, provider, at = 1_100))
                net.store(provider).insert(score(service, bob.pubKey, 60, at = 1_200))
                net.store(provider).insert(score(service, carol.pubKey, 40, at = 1_201))

                // Records: bob and carol each advertise an outbox (on the index) holding a note.
                net.store(index).insert(relayList(bob, at = 1_000, bobOutbox))
                net.store(index).insert(relayList(carol, at = 1_000, carolOutbox))
                net.store(bobOutbox).insert(note(bob, "bob writes about coffee", at = 1_300))
                net.store(carolOutbox).insert(note(carol, "carol writes about tea", at = 1_301))

                val store = localStore()
                store.use {
                    trustSync(net, store).run(
                        observers = setOf(observer.pubKey),
                        indexRelays = listOf(net.url(index)),
                    )

                    assertEquals(2, store.count(Filter(kinds = listOf(ContactCardEvent.KIND))), "both scores synced")
                    assertEquals(listOf("bob writes about coffee"), store.notesBy(bob.pubKey), "bob's note was pulled from his outbox")
                    assertEquals(listOf("carol writes about tea"), store.notesBy(carol.pubKey), "carol's note was pulled from her outbox")
                }
            }
        }

    /** A record deletion published to the author's own outbox erases the note locally (explicit kind 5). */
    @Test
    fun `records plane honours an author's outbox deletion`() =
        runBlocking {
            InProcessNet().use { net ->
                net.store(index).insert(relayList(observer, at = 1_000, outbox))
                net.store(outbox).insert(providerList(observer, service.pubKey, provider, at = 1_100))
                net.store(provider).insert(score(service, bob.pubKey, 60, at = 1_200))

                net.store(index).insert(relayList(bob, at = 1_000, bobOutbox))
                val doomed = note(bob, "a note bob will delete", at = 1_300)
                net.store(bobOutbox).insert(doomed)
                net.store(bobOutbox).insert(bob.sign<DeletionEvent>(1_400, DeletionEvent.KIND, arrayOf(arrayOf("e", doomed.id)), ""))

                val store = localStore()
                store.use {
                    trustSync(net, store).run(
                        observers = setOf(observer.pubKey),
                        indexRelays = listOf(net.url(index)),
                    )
                    assertTrue(store.notesBy(bob.pubKey).isEmpty(), "the deleted note did not survive in the local store")
                }
            }
        }
}
