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
import com.vitorpamplona.quartz.nip65RelayList.AdvertisedRelayListEvent
import com.vitorpamplona.quartz.nip65RelayList.tags.AdvertisedRelayInfo
import com.vitorpamplona.quartz.nip65RelayList.tags.AdvertisedRelayType
import com.vitorpamplona.quartz.nip85TrustedAssertions.list.TrustProviderListEvent
import com.vitorpamplona.sot.store.VespaEventStore
import com.vitorpamplona.sot.vespa.InMemoryEventIndex
import kotlinx.coroutines.runBlocking
import kotlin.test.Test
import kotlin.test.assertEquals

/**
 * Broad relay discovery (always on — a sync is all-or-nothing). A kind-10040
 * trust list lives on its author's OWN outbox, not on the profile-directory
 * relays that only aggregate kind 0 / 10002. So seeding a directory alone would
 * never find it — discovery has to pull the directory's 10002s and sweep the
 * write relays they name. These tests prove exactly that reach.
 */
class RelayDiscoveryTest {
    private val alice = NostrSignerSync() // an observer nobody told us about
    private val bob = NostrSignerSync() // an observer reachable only via alice's outbox
    private val service = NostrSignerSync()

    private val directory = "wss://directory.test" // holds kind 0 / 10002 only
    private val aliceOutbox = "wss://alice.test" // where alice's own 10040 lives
    private val bobOutbox = "wss://bob.test" // only named in a 10002 on alice's outbox
    private val provider = "wss://provider.test"

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

    private fun localStore() = VespaEventStore(InMemoryEventIndex(), relay = RelayUrlNormalizer.normalize("ws://localhost:7777"))

    private fun trustSync(
        net: InProcessNet,
        store: VespaEventStore,
    ): TrustSync {
        val opts = SyncOptions(concurrency = 4, fetchTimeoutMs = 15_000)
        val syncer = RelaySyncer(net.client, store, SyncState(), log = { }, idleTimeoutMs = opts.fetchTimeoutMs)
        return TrustSync(syncer, store, opts, log = { })
    }

    private suspend fun VespaEventStore.observerCount() = count(Filter(kinds = listOf(TrustProviderListEvent.KIND)))

    @Test
    fun `discovery sweeps outbox relays named in the directory 10002s for 10040s`() =
        runBlocking {
            InProcessNet().use { net ->
                // The directory has alice's 10002 (only). Her authoritative 10040
                // sits on HER outbox, which the seed set never names directly.
                net.store(directory).insert(relayList(alice, at = 1_000, aliceOutbox))
                net.store(aliceOutbox).insert(providerList(alice, service.pubKey, provider, at = 1_100))

                // Discovery harvests the directory's 10002, sweeps alice's outbox
                // (which the seed set never names directly), and finds her 10040
                // there — she becomes a known observer. The directory itself carries
                // no 10040, so only the outbox sweep can find her.
                localStore().use { store ->
                    trustSync(net, store).run(
                        indexRelays = listOf(net.url(directory)),
                        seedRelays = listOf(net.url(directory)),
                    )
                    assertEquals(1, store.observerCount(), "discovery found alice's 10040 on her own outbox")
                }
            }
        }

    @Test
    fun `discovery snowballs across rounds via the harvested 10002 r-tags`() =
        runBlocking {
            InProcessNet().use { net ->
                // Round 1 reaches only alice's outbox (from the directory's 10002).
                net.store(directory).insert(relayList(alice, at = 1_000, aliceOutbox))
                // Alice's outbox holds her own 10040 AND bob's 10002 — bob's outbox is
                // named nowhere else, so it is reachable only by snowballing off this.
                net.store(aliceOutbox).insert(providerList(alice, service.pubKey, provider, at = 1_100))
                net.store(aliceOutbox).insert(relayList(bob, at = 1_000, bobOutbox))
                // Bob's own 10040 lives on his outbox — found only in round 2.
                net.store(bobOutbox).insert(providerList(bob, service.pubKey, provider, at = 1_200))

                localStore().use { store ->
                    trustSync(net, store).run(
                        indexRelays = listOf(net.url(directory)),
                        seedRelays = listOf(net.url(directory)),
                    )
                    assertEquals(2, store.observerCount(), "found alice (round 1) and bob (round 2, via alice's outbox r-tags)")
                }
            }
        }
}
