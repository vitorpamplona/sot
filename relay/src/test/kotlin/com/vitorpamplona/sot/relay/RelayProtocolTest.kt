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
package com.vitorpamplona.sot.relay

import com.vitorpamplona.quartz.eventstore.store.DEFAULT_MIN_RANK
import com.vitorpamplona.quartz.eventstore.store.NostrEventStore
import com.vitorpamplona.quartz.eventstore.vespa.InMemoryEventIndex
import com.vitorpamplona.quartz.eventstore.vespa.client.EventIndex
import com.vitorpamplona.quartz.eventstore.vespa.doc.EventDoc
import com.vitorpamplona.quartz.eventstore.vespa.query.EventQuery
import com.vitorpamplona.quartz.eventstore.vespa.query.EventYql
import com.vitorpamplona.quartz.nip01Core.core.Event
import com.vitorpamplona.quartz.nip01Core.metadata.MetadataEvent
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.RelayUrlNormalizer
import com.vitorpamplona.quartz.nip01Core.signers.NostrSignerSync
import com.vitorpamplona.quartz.nip42RelayAuth.RelayAuthEvent
import kotlinx.coroutines.runBlocking
import java.util.Collections
import kotlin.test.AfterTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue
import kotlin.test.fail

/**
 * The whole relay stack, driven over the wire protocol: Quartz's engine ->
 * ObserverRoutingBackend -> NostrEventStore -> a recording in-memory index.
 * Sessions speak raw NIP-01 JSON through [SotRelayServer.connect], exactly
 * what the websocket route feeds them.
 */
class RelayProtocolTest {
    private val defaultObserver = "d".repeat(64)
    private val relayUrl = RelayUrlNormalizer.normalize("ws://localhost:7777")

    /** Records each SEARCH query's ranking context (observer, profile, trust floor). */
    private class RecordingIndex : EventIndex {
        val inner = InMemoryEventIndex()
        val searchObservers = Collections.synchronizedList(mutableListOf<String?>())
        val searchQueries = Collections.synchronizedList(mutableListOf<EventQuery>())

        override suspend fun get(id: String) = inner.get(id)

        override suspend fun put(doc: EventDoc) = inner.put(doc)

        override suspend fun remove(id: String) = inner.remove(id)

        override suspend fun search(query: EventQuery): List<EventDoc> {
            if (query.search != null || query.ranking != null) {
                searchObservers += query.observer
                searchQueries += query
            }
            return inner.search(query)
        }

        override suspend fun count(query: EventQuery) = inner.count(query)

        override suspend fun distinctAuthors(query: EventQuery) = inner.distinctAuthors(query)

        override fun close() {}
    }

    private val index = RecordingIndex()
    private val store = NostrEventStore(index, relay = relayUrl)
    private val server = SotRelayServer(store, defaultObserver, relayUrl)
    private val signer = NostrSignerSync()

    @AfterTest
    fun tearDown() {
        server.close()
    }

    @Test
    fun `publishes, answers full filters, and streams live events`() =
        runBlocking {
            val bob = "b2".repeat(32)
            val out = Collections.synchronizedList(mutableListOf<String>())
            val session = server.connect { out.add(it) }
            try {
                // A live subscription with a full NIP-01 filter — not just a search term.
                session.receive("""["REQ","sub",{"kinds":[1],"#p":["$bob"]}]""")
                awaitMessage(out) { it.startsWith("""["EOSE","sub"]""") }

                // Publish a signed note (VerifyPolicy checks id + signature).
                val note = signer.sign<Event>(1_700_000_000L, 1, arrayOf(arrayOf("p", bob)), "hi bob")
                session.receive("""["EVENT",${note.toJson()}]""")
                awaitMessage(out) { it.startsWith("""["OK","${note.id}",true""") }
                // ...and the open subscription sees it live.
                awaitMessage(out) { it.startsWith("""["EVENT","sub",""") && note.id in it }

                // A fresh REQ answers the same event from storage, through author + tag + time filters.
                session.receive("""["REQ","q2",{"kinds":[1],"authors":["${signer.pubKey}"],"#p":["$bob"],"since":1699999999}]""")
                awaitMessage(out) { it.startsWith("""["EVENT","q2",""") && note.id in it }
                awaitMessage(out) { it.startsWith("""["EOSE","q2"]""") }

                // NIP-45 COUNT over the stored set.
                session.receive("""["COUNT","c1",{"kinds":[1]}]""")
                val count = awaitMessage(out) { it.startsWith("""["COUNT","c1"""") }
                assertTrue("\"count\":1" in count, "exact count from the store: $count")
            } finally {
                session.close()
            }
        }

    @Test
    fun `forged publishes are rejected before the store`() =
        runBlocking {
            val out = Collections.synchronizedList(mutableListOf<String>())
            val session = server.connect { out.add(it) }
            try {
                val real = signer.sign<Event>(1_700_000_000L, 1, emptyArray(), "genuine")
                val forged = Event(real.id, real.pubKey, real.createdAt, real.kind, real.tags, "tampered", real.sig)
                session.receive("""["EVENT",${forged.toJson()}]""")
                val ok = awaitMessage(out) { it.startsWith("""["OK","${forged.id}"""") }
                assertTrue(""","false,""" in ok || """,false,""" in ok, "VerifyPolicy must reject: $ok")
                assertEquals(
                    0,
                    store.count(
                        com.vitorpamplona.quartz.nip01Core.relay.filters
                            .Filter(kinds = listOf(1)),
                    ),
                )
            } finally {
                session.close()
            }
        }

    @Test
    fun `NIP-42 auth switches the ranking observer`() =
        runBlocking {
            // A searchable profile in the store (search_text derives from the typed event).
            store.insert(MetadataEvent("4".repeat(64), "a1".repeat(32), 1_700_000_000L, emptyArray(), """{"name":"alice"}""", ""))

            val out = Collections.synchronizedList(mutableListOf<String>())
            val session = server.connect { out.add(it) }
            try {
                // The relay advertises NIP-42 on connect.
                val challenge = awaitMessage(out) { it.startsWith("""["AUTH",""") }.substringAfter("""["AUTH","""").substringBefore('"')

                // Unauthenticated search: ranked by the operator's DEFAULT observer.
                session.receive("""["REQ","s1",{"kinds":[0],"search":"ali","limit":10}]""")
                awaitMessage(out) { it.startsWith("""["EOSE","s1"]""") }
                assertTrue(out.any { it.startsWith("""["EVENT","s1",""") && "alice" in it }, "the stored kind-0 streams back: $out")
                assertEquals(listOf(defaultObserver), index.searchObservers.toList())

                // Authenticate with a real signed kind-22242, then search again.
                val auth = signer.sign(RelayAuthEvent.build(relayUrl, challenge))
                session.receive("""["AUTH",${auth.toJson()}]""")
                awaitMessage(out) { it.startsWith("""["OK","${auth.id}",true""") }

                session.receive("""["REQ","s2",{"kinds":[0],"search":"ali","limit":10}]""")
                awaitMessage(out) { it.startsWith("""["EOSE","s2"]""") }
                assertEquals(signer.pubKey, index.searchObservers.last(), "an authenticated user searches through their OWN web of trust")
            } finally {
                session.close()
            }
        }

    /**
     * Quartz's engine STRIPS NIP-50 extensions from REQ filters before the
     * store ([OriginalFilters] carries the originals past it) — this store
     * honors them, so they must survive the whole websocket path. This is the
     * session-level net for future Quartz bumps: it fails if the extensions
     * stop reaching the Vespa query.
     */
    @Test
    fun `NIP-50 extensions survive the session to the engine query`() =
        runBlocking {
            store.insert(MetadataEvent("5".repeat(64), "a2".repeat(32), 1_700_000_000L, emptyArray(), """{"name":"alice"}""", ""))
            val out = Collections.synchronizedList(mutableListOf<String>())
            val session = server.connect { out.add(it) }
            try {
                session.receive("""["REQ","x1",{"search":"ali","limit":5}]""")
                awaitMessage(out) { it.startsWith("""["EOSE","x1"]""") }
                assertEquals(DEFAULT_MIN_RANK, index.searchQueries.last().minRank, "a plain search is trust-gated by default")

                session.receive("""["REQ","x2",{"search":"ali include:spam","limit":5}]""")
                awaitMessage(out) { it.startsWith("""["EOSE","x2"]""") }
                assertEquals(null, index.searchQueries.last().minRank, "include:spam lifts the default floor")
                assertEquals("ali", index.searchQueries.last().search, "the extension itself never becomes a term")

                session.receive("""["REQ","x3",{"search":"ali sort:rank filter:rank:gte:7","limit":5}]""")
                awaitMessage(out) { it.startsWith("""["EOSE","x3"]""") }
                assertEquals(EventYql.RANK_DESC, index.searchQueries.last().ranking, "sort:rank picks the profile")
                assertEquals(7.0, index.searchQueries.last().minRank, "filter:rank:gte sets the floor")
            } finally {
                session.close()
            }
        }

    @Test
    fun `an authenticated search enrolls the observer through the hook`() =
        runBlocking {
            val enrolled = Collections.synchronizedList(mutableListOf<String>())
            val hooked = SotRelayServer(store, defaultObserver, relayUrl, onObserver = { enrolled.add(it) })
            try {
                val out = Collections.synchronizedList(mutableListOf<String>())
                val session = hooked.connect { out.add(it) }
                try {
                    val challenge = awaitMessage(out) { it.startsWith("""["AUTH",""") }.substringAfter("""["AUTH","""").substringBefore('"')

                    // Anonymous searches never enroll anyone.
                    session.receive("""["REQ","s1",{"kinds":[0],"search":"ali","limit":10}]""")
                    awaitMessage(out) { it.startsWith("""["EOSE","s1"]""") }
                    assertEquals(emptyList(), enrolled.toList())

                    val auth = signer.sign(RelayAuthEvent.build(relayUrl, challenge))
                    session.receive("""["AUTH",${auth.toJson()}]""")
                    awaitMessage(out) { it.startsWith("""["OK","${auth.id}",true""") }

                    session.receive("""["REQ","s2",{"kinds":[0],"search":"ali","limit":10}]""")
                    awaitMessage(out) { it.startsWith("""["EOSE","s2"]""") }
                    assertEquals(listOf(signer.pubKey), enrolled.distinct(), "the login becomes a sync observer")
                } finally {
                    session.close()
                }
            } finally {
                hooked.close()
            }
        }

    private fun awaitMessage(
        out: List<String>,
        match: (String) -> Boolean,
    ): String {
        val deadline = System.currentTimeMillis() + 10_000
        while (System.currentTimeMillis() < deadline) {
            synchronized(out) { out.firstOrNull(match) }?.let { return it }
            Thread.sleep(20)
        }
        fail("timed out waiting for a matching relay message; got: $out")
    }
}
