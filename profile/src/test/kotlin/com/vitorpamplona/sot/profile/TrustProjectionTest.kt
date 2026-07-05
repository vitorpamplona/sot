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
import com.vitorpamplona.quartz.nip01Core.store.IEventStore
import com.vitorpamplona.quartz.nip09Deletions.DeletionEvent
import com.vitorpamplona.quartz.nip62RequestToVanish.RequestToVanishEvent
import com.vitorpamplona.quartz.nip85TrustedAssertions.list.TrustProviderListEvent
import com.vitorpamplona.quartz.nip85TrustedAssertions.users.ContactCardEvent
import com.vitorpamplona.sot.store.VespaEventStore
import com.vitorpamplona.sot.vespa.InMemoryEventIndex
import com.vitorpamplona.sot.vespa.InMemoryProfileIndex
import com.vitorpamplona.sot.vespa.doc.ProfileDoc
import kotlinx.coroutines.runBlocking
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNull
import kotlin.test.assertTrue

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
    fun `an unchanged 30382 supersession skips the recompute but keeps the cell`() =
        runBlocking {
            store.insert(list10040())
            store.insert(card(rank = 87, followers = 120, at = 100))
            val before = profiles.writes.get()
            // A newer version with IDENTICAL trust tags — ranking can't move.
            store.insert(card(rank = 87, followers = 120, at = 200))
            assertEquals(before, profiles.writes.get(), "no profile write for an unchanged supersession")
            assertEquals(
                ProfileDoc(subject, mapOf(observer to 87), mapOf(observer to 120.0)),
                profiles.get(subject),
                "the cell still holds the unchanged score",
            )
        }

    @Test
    fun `a changed 30382 supersession still recomputes`() =
        runBlocking {
            store.insert(list10040())
            store.insert(card(rank = 87, followers = 120, at = 100))
            val before = profiles.writes.get()
            store.insert(card(rank = 50, followers = 120, at = 200))
            assertTrue(profiles.writes.get() > before, "a changed score must re-derive")
            assertEquals(mapOf(observer to 50), profiles.get(subject)?.qualityScores)
        }

    @Test
    fun `an unchanged 30382 supersession through the bulk path skips the recompute`() =
        runBlocking {
            store.insert(list10040())
            store.insert(card(rank = 87, followers = 120, at = 100))
            val before = profiles.writes.get()
            store.batchInsert(listOf(card(rank = 87, followers = 120, at = 200)))
            assertEquals(before, profiles.writes.get(), "bulk unchanged supersession issues no profile write")
            assertEquals(
                ProfileDoc(subject, mapOf(observer to 87), mapOf(observer to 120.0)),
                profiles.get(subject),
            )
        }

    /**
     * The write split must not cross wires: a batch where one subject is a
     * trust-neutral supersession (skipped) and another genuinely changed
     * (re-derived) lands both final cells correctly.
     */
    @Test
    fun `a bulk batch mixing neutral and changed supersessions stays correct`() =
        runBlocking {
            val subjectB = "cd".repeat(32)
            // Distinct observers so both services map (one 10040 per author).
            store.insert(list10040(author = observer, serviceKey = service))
            store.insert(list10040(author = observer2, serviceKey = service2))
            store.insert(card(signer = service, about = subject, rank = 87, followers = 120, at = 100))
            store.insert(card(signer = service2, about = subjectB, rank = 40, followers = 9, at = 100))
            store.batchInsert(
                listOf(
                    card(signer = service, about = subject, rank = 87, followers = 120, at = 200), // neutral
                    card(signer = service2, about = subjectB, rank = 55, followers = 9, at = 200), // changed
                ),
            )
            assertEquals(mapOf(observer to 87), profiles.get(subject)?.qualityScores, "neutral subject unchanged")
            assertEquals(mapOf(observer2 to 55), profiles.get(subjectB)?.qualityScores, "changed subject re-derived")
        }

    /**
     * The ordering net: ONE subject scored by a neutral service and a changed
     * service in the same batch. The neutral card's event doc must be written
     * BEFORE the changed service's re-derive runs, or the re-derive would drop
     * the neutral cell (its old card gone, new card not yet stored).
     */
    @Test
    fun `a neutral and a changed service on the same subject both survive a bulk batch`() =
        runBlocking {
            store.insert(list10040(author = observer, serviceKey = service))
            store.insert(list10040(author = observer2, serviceKey = service2))
            store.insert(card(signer = service, about = subject, rank = 87, followers = 120, at = 100))
            store.insert(card(signer = service2, about = subject, rank = 40, followers = 9, at = 100))
            assertEquals(mapOf(observer to 87, observer2 to 40), profiles.get(subject)?.qualityScores)

            store.batchInsert(
                listOf(
                    card(signer = service, about = subject, rank = 87, followers = 120, at = 200), // neutral
                    card(signer = service2, about = subject, rank = 55, followers = 9, at = 200), // changed
                ),
            )
            assertEquals(
                mapOf(observer to 87, observer2 to 55),
                profiles.get(subject)?.qualityScores,
                "the neutral cell survives the changed service's re-derive",
            )
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

    /** The BULK path: one store batch of scores builds every subject's parent doc. */
    @Test
    fun `a bulk batch of scores projects one parent doc per subject`() =
        runBlocking {
            store.insert(list10040())
            val subjects = (1..40).map { it.toString(16).padStart(64, 'f') }
            val batch = subjects.map { s -> card(about = s, rank = 10, followers = null) }
            val outcomes = store.batchInsert(batch)
            assertEquals(40, outcomes.count { it is IEventStore.InsertOutcome.Accepted })
            subjects.forEach { s ->
                assertEquals(mapOf(observer to 10), profiles.docs.getValue(s).qualityScores, "subject ${'$'}s")
            }
        }

    /**
     * Two services (both mapped to the observer) scoring one subject in the
     * SAME bulk batch: the observer cell holds the last-applied card's value —
     * the zero-read [ProfileIndex.updateCells] path's documented
     * "latest-arriving mapped card wins", matching what [derive] does with a
     * LinkedHashMap (last write wins) on the sequential path.
     */
    @Test
    fun `two services scoring one subject in a bulk batch attribute to the observer`() =
        runBlocking {
            store.insert(list10040(serviceKey = service))
            store.insert(list10040(author = observer, serviceKey = service2))
            val outcomes = store.batchInsert(listOf(card(signer = service, rank = 30), card(signer = service2, rank = 71)))
            assertEquals(2, outcomes.count { it is IEventStore.InsertOutcome.Accepted })
            // Both cards attribute to the ONE observer cell; last applied wins.
            assertEquals(mapOf(observer to 71), profiles.get(subject)?.qualityScores)
        }

    /** A retraction (rank tag gone) inside a bulk batch supersedes and empties the cell. */
    @Test
    fun `a retraction in a bulk batch empties the subject cell`() =
        runBlocking {
            store.insert(list10040())
            store.insert(card(rank = 87, at = 100))
            // Newer version with no rank/followers, delivered through the bulk path.
            val outcomes = store.batchInsert(listOf(card(rank = null, followers = null, at = 200)))
            assertEquals(1, outcomes.count { it is IEventStore.InsertOutcome.Accepted })
            assertNull(profiles.get(subject), "the retraction is the newest version — no cell left")
        }

    /**
     * A PARTIAL retraction through the bulk path — rank dropped, followers kept.
     * The zero-read cell update can't express "clear the quality cell", so this
     * must fall back to the read-based recompute; otherwise the stale quality
     * cell survives (the bulk-vs-single divergence the audit caught).
     */
    @Test
    fun `a partial retraction in a bulk batch drops the stale dimension`() =
        runBlocking {
            store.insert(list10040())
            store.insert(card(rank = 87, followers = 120, at = 100))
            assertEquals(mapOf(observer to 87), profiles.get(subject)?.qualityScores)
            // Newer card keeps followers but drops rank — via the bulk path.
            store.batchInsert(listOf(card(rank = null, followers = 200, at = 200)))
            val doc = profiles.get(subject)
            assertEquals(emptyMap(), doc?.qualityScores, "the dropped rank must not linger")
            assertEquals(mapOf(observer to 200.0), doc?.followerCounts, "followers updated")
        }

    /**
     * The bulk fast path (zero-read cell updates) must land the SAME tensors as
     * one-by-one inserts (full re-derivation), across supersession, multi-service
     * attribution, and retraction in one batch. The parity net for the
     * insert-path optimization.
     */
    @Test
    fun `bulk projection equals sequential across supersession, multi-service and retraction`() =
        runBlocking {
            val subjectB = "cd".repeat(32)
            val events =
                listOf(
                    list10040(serviceKey = service, at = 10),
                    list10040(author = observer2, serviceKey = service2, at = 11),
                    card(signer = service, about = subject, rank = 20, at = 20),
                    card(signer = service, about = subject, rank = 55, at = 30), // supersedes -> 55
                    card(signer = service2, about = subject, rank = 9, followers = 4, at = 40), // observer2 cell
                    card(signer = service, about = subjectB, rank = 88, at = 50),
                    card(signer = service, about = subjectB, rank = null, followers = null, at = 60), // retracts subjectB
                )

            val sequentialProfiles = InMemoryProfileIndex()
            val sequential = VespaEventStore(TrustProjection(InMemoryEventIndex(), sequentialProfiles), relay = RelayUrlNormalizer.normalize("ws://localhost:7777"))
            events.forEach { sequential.insert(it) }

            val bulkProfiles = InMemoryProfileIndex()
            val bulk = VespaEventStore(TrustProjection(InMemoryEventIndex(), bulkProfiles), relay = RelayUrlNormalizer.normalize("ws://localhost:7777"))
            bulk.batchInsert(events)

            assertEquals(sequentialProfiles.docs, bulkProfiles.docs, "bulk cell-updates must match sequential re-derivation")
            // And the values are what we expect, not coincidentally-equal empties.
            assertEquals(mapOf(observer to 55, observer2 to 9), bulkProfiles.docs.getValue(subject).qualityScores)
            assertNull(bulkProfiles.docs[subjectB], "subjectB was retracted")
        }
}
