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
package com.vitorpamplona.sot.profile

import com.vitorpamplona.quartz.nip01Core.relay.normalizer.RelayUrlNormalizer
import com.vitorpamplona.quartz.nip09Deletions.DeletionEvent
import com.vitorpamplona.quartz.nip62RequestToVanish.RequestToVanishEvent
import com.vitorpamplona.quartz.nip85TrustedAssertions.list.TrustProviderListEvent
import com.vitorpamplona.quartz.nip85TrustedAssertions.users.ContactCardEvent
import com.vitorpamplona.sot.store.VespaEventStore
import com.vitorpamplona.sot.vespa.InMemoryEventIndex
import com.vitorpamplona.sot.vespa.InMemoryProfileIndex
import com.vitorpamplona.sot.vespa.ProfileDoc
import kotlinx.coroutines.runBlocking
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNull

/**
 * The projection is driven through the REAL store, so every path that
 * mutates trust data — inserts, supersession, kind-5, vanish — reaches the
 * tensors through the index decorator, with no deletion-specific code.
 */
class TrustProjectionTest {
    private val observer = "0b".repeat(32)
    private val observer2 = "2b".repeat(32)
    private val service = "5e".repeat(32)
    private val service2 = "6e".repeat(32)
    private val subject = "ab".repeat(32)

    private val profiles = InMemoryProfileIndex()
    private val projection = TrustProjection(InMemoryEventIndex(), profiles)
    private val store = VespaEventStore(projection, relay = RelayUrlNormalizer.normalize("ws://localhost:7777"))

    private var t = 1_000_000L

    private fun next() = t++

    private var seq = 0

    private fun id() = (++seq).toString(16).padStart(64, '0')

    private fun list10040(
        author: String = observer,
        serviceKey: String = service,
        at: Long = next(),
    ) = TrustProviderListEvent(id(), author, at, arrayOf(arrayOf("30382:rank", serviceKey, "wss://scores.example.com/")), "", "")

    private fun card(
        signer: String = service,
        about: String = subject,
        rank: Int? = 87,
        followers: Int? = 120,
        at: Long = next(),
        eventId: String = id(),
    ): ContactCardEvent {
        val tags =
            buildList {
                add(arrayOf("d", about))
                rank?.let { add(arrayOf("rank", it.toString())) }
                followers?.let { add(arrayOf("followers", it.toString())) }
            }.toTypedArray()
        return ContactCardEvent(eventId, signer, at, tags, "", "")
    }

    @Test
    fun `scores land keyed by the observer, not the service key`() =
        runBlocking {
            store.insert(list10040())
            store.insert(card())
            assertEquals(
                ProfileDoc(subject, mapOf(observer to 87), mapOf(observer to 120.0)),
                profiles.get(subject),
            )
        }

    @Test
    fun `a 30382 arriving before its 10040 is attributed when the list shows up`() =
        runBlocking {
            store.insert(card())
            assertNull(profiles.get(subject), "no provider mapping yet")
            store.insert(list10040())
            assertEquals(mapOf(observer to 87), profiles.get(subject)?.qualityScores)
        }

    @Test
    fun `supersession without a rank tag retracts the score`() =
        runBlocking {
            store.insert(list10040())
            store.insert(card(rank = 87, at = 100))
            store.insert(card(rank = null, followers = null, at = 200))
            assertNull(profiles.get(subject), "the newest version is the whole truth")
        }

    @Test
    fun `kind 5 deletion of the score erases the cell`() =
        runBlocking {
            store.insert(list10040())
            val scored = card()
            store.insert(scored)
            store.insert(DeletionEvent(id(), service, next(), arrayOf(arrayOf("e", scored.id)), "", ""))
            assertNull(profiles.get(subject))
        }

    @Test
    fun `a vanishing service key sweeps its cells`() =
        runBlocking {
            store.insert(list10040())
            store.insert(card(at = 100))
            store.insert(RequestToVanishEvent(id(), service, 200, arrayOf(arrayOf("relay", "ALL_RELAYS")), "", ""))
            assertNull(profiles.get(subject))
        }

    @Test
    fun `two observers on one subject hold independent cells`() =
        runBlocking {
            store.insert(list10040(author = observer, serviceKey = service))
            store.insert(list10040(author = observer2, serviceKey = service2))
            store.insert(card(signer = service, rank = 87))
            store.insert(card(signer = service2, rank = 15, followers = 3))
            assertEquals(mapOf(observer to 87, observer2 to 15), profiles.get(subject)?.qualityScores)
            assertEquals(mapOf(observer to 120.0, observer2 to 3.0), profiles.get(subject)?.followerCounts)
        }

    @Test
    fun `switching providers re-attributes stored scores`() =
        runBlocking {
            store.insert(list10040(serviceKey = service, at = 100))
            store.insert(card(signer = service, rank = 87))
            store.insert(card(signer = service2, rank = 42))
            assertEquals(mapOf(observer to 87), profiles.get(subject)?.qualityScores)

            // The observer's NEW 10040 picks service2: the superseding insert
            // re-attributes — service's score detaches, service2's attaches.
            store.insert(list10040(serviceKey = service2, at = 200))
            assertEquals(mapOf(observer to 42), profiles.get(subject)?.qualityScores)
        }

    @Test
    fun `rebuildAll re-derives everything from the event corpus`() =
        runBlocking {
            store.insert(list10040())
            store.insert(card())
            profiles.docs.clear()

            projection.rebuildAll()
            assertEquals(mapOf(observer to 87), profiles.get(subject)?.qualityScores)
        }
}
