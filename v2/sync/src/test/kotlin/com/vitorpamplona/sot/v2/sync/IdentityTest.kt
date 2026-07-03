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

import com.vitorpamplona.quartz.nip01Core.core.Event
import com.vitorpamplona.quartz.nip01Core.crypto.verify
import com.vitorpamplona.quartz.nip01Core.metadata.MetadataEvent
import com.vitorpamplona.quartz.nip01Core.relay.filters.Filter
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.RelayUrlNormalizer
import com.vitorpamplona.quartz.nip01Core.signers.NostrSignerSync
import com.vitorpamplona.quartz.nip51Lists.relayLists.IndexerRelayListEvent
import com.vitorpamplona.quartz.nip51Lists.relayLists.tags.RelayTag
import com.vitorpamplona.quartz.nip65RelayList.AdvertisedRelayListEvent
import com.vitorpamplona.sot.v2.store.VespaEventStore
import com.vitorpamplona.sot.v2.vespa.InMemoryEventIndex
import kotlinx.coroutines.runBlocking
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertNull
import kotlin.test.assertTrue

class IdentityTest {
    private val self = RelayUrlNormalizer.normalize("wss://sot.example.com")

    private fun store() = VespaEventStore(InMemoryEventIndex(), relay = self)

    private fun identity(signer: NostrSignerSync = NostrSignerSync()) = Identity(signer, selfRelayUrl = self, name = "sot-test")

    @Test
    fun `first run publishes a verifiable kind 0, 10002, and 10086 into the store`() =
        runBlocking {
            val id = identity()
            store().use { store ->
                id.ensurePublished(store)

                val events = store.query<Event>(Filter(authors = listOf(id.pubkey)))
                assertEquals(setOf(0, 10002, 10086), events.map { it.kind }.toSet())
                assertTrue(events.all { it.verify() }, "self-published events are real signed events")

                val relayList = store.query<AdvertisedRelayListEvent>(Filter(kinds = listOf(10002))).single()
                assertEquals(listOf(self), relayList.writeRelaysNorm(), "the 10002 points at this relay")

                assertEquals(Identity.DEFAULT_INDEX_RELAYS, id.indexRelays(store), "the stored 10086 seeds the defaults")
            }
        }

    @Test
    fun `a second run leaves the published events alone`() =
        runBlocking {
            val id = identity()
            store().use { store ->
                id.ensurePublished(store)
                val first = store.query<MetadataEvent>(Filter(kinds = listOf(0))).single().id
                id.ensurePublished(store)
                assertEquals(first, store.query<MetadataEvent>(Filter(kinds = listOf(0))).single().id)
            }
        }

    @Test
    fun `an operator supersede of the 10086 changes the index relays without a config redeploy`() =
        runBlocking {
            val signer = NostrSignerSync()
            val id = identity(signer)
            store().use { store ->
                id.ensurePublished(store)

                // The operator, from any Nostr client, publishes a fresher 10086.
                val custom = RelayUrlNormalizer.normalize("wss://my-indexer.example.com")
                val fresher =
                    signer.sign<IndexerRelayListEvent>(
                        System.currentTimeMillis() / 1000 + 10,
                        IndexerRelayListEvent.KIND,
                        arrayOf(RelayTag.assemble(custom)),
                        "",
                    )
                store.insert(fresher)

                id.ensurePublished(store) // must not resurrect the seed list
                assertEquals(listOf(custom), id.indexRelays(store))
            }
        }

    @Test
    fun `signerFromSecret accepts hex and nsec, rejects garbage`() {
        val hex = NostrSignerSync().keyPair.privKey!!.joinToString("") { "%02x".format(it) }
        assertNotNull(Identity.signerFromSecret(hex))
        assertNull(Identity.signerFromSecret("not-a-key"))
        assertNull(Identity.signerFromSecret(""))
    }
}
