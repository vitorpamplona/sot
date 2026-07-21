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

import com.vitorpamplona.quartz.eventstore.store.VespaEventStore
import com.vitorpamplona.quartz.eventstore.vespa.InMemoryEventIndex
import com.vitorpamplona.quartz.nip01Core.core.HexKey
import com.vitorpamplona.quartz.nip01Core.relay.filters.Filter
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.RelayUrlNormalizer
import com.vitorpamplona.quartz.nip01Core.signers.NostrSignerSync
import com.vitorpamplona.quartz.nip65RelayList.AdvertisedRelayListEvent
import com.vitorpamplona.quartz.nip65RelayList.tags.AdvertisedRelayInfo
import com.vitorpamplona.quartz.nip65RelayList.tags.AdvertisedRelayType
import com.vitorpamplona.quartz.nip85TrustedAssertions.list.TrustProviderListEvent
import com.vitorpamplona.quartz.nip85TrustedAssertions.users.ContactCardEvent
import kotlinx.coroutines.runBlocking
import kotlin.test.Test
import kotlin.test.assertEquals

/**
 * The scores plane end to end, over an in-process NETWORK of real relays
 * ([InProcessNet]) — every event signed, every download verified, the real
 * NIP-77/REQ protocol on both sides. The scenarios are the proposal's chain
 * promises: house-account bootstrap, outbox-authoritative 10040s,
 * provider-switch cleanup, the no-10002 fallback, and silent-removal
 * reconciliation.
 */
class TrustSyncTest {
    // The cast: two observers, two score providers (service keys), two subjects.
    private val observer = NostrSignerSync()
    private val observer2 = NostrSignerSync()
    private val service1 = NostrSignerSync()
    private val service2 = NostrSignerSync()
    private val bob = NostrSignerSync()
    private val carol = NostrSignerSync()

    private val index = "wss://index.test"
    private val home = "wss://home.test"
    private val outbox = "wss://outbox.test"
    private val outbox2 = "wss://outbox2.test"
    private val providerA = "wss://provider-a.test"
    private val providerB = "wss://provider-b.test"

    private fun relayList(
        signer: NostrSignerSync,
        at: Long,
        vararg writeRelays: String,
    ): AdvertisedRelayListEvent =
        AdvertisedRelayListEvent.create(
            writeRelays.map { AdvertisedRelayInfo(RelayUrlNormalizer.normalize(it), AdvertisedRelayType.BOTH) },
            signer,
            at,
        )

    private fun providerList(
        signer: NostrSignerSync,
        service: HexKey,
        relay: String,
        at: Long,
    ): TrustProviderListEvent = signer.sign(at, TrustProviderListEvent.KIND, arrayOf(arrayOf("30382:rank", service, RelayUrlNormalizer.normalize(relay).url)), "")

    private fun score(
        signer: NostrSignerSync,
        subject: HexKey,
        rank: Int,
        at: Long,
    ): ContactCardEvent = signer.sign(at, ContactCardEvent.KIND, arrayOf(arrayOf("d", subject), arrayOf("rank", "$rank")), "")

    private fun localStore() = VespaEventStore(InMemoryEventIndex(), relay = RelayUrlNormalizer.normalize("ws://localhost:7777"))

    private fun trustSync(
        net: InProcessNet,
        store: VespaEventStore,
    ): TrustSync {
        val opts = SyncOptions(concurrency = 4, fetchTimeoutMs = 15_000)
        val syncer = RelaySyncer(net.client, store, SyncState(), log = { }, idleTimeoutMs = opts.fetchTimeoutMs)
        return TrustSync(syncer, store, opts, log = { }, crawl = InMemoryCrawlIndex())
    }

    private suspend fun VespaEventStore.scoreAuthors(): Set<String> = query<ContactCardEvent>(Filter(kinds = listOf(ContactCardEvent.KIND))).map { it.pubKey }.toSet()

    /** The proposal's bootstrap: house 10002 from the HOME relay -> outbox 10040 -> provider 30382s. */
    @Test
    fun `house account bootstraps the full chain from its home relay`() =
        runBlocking {
            InProcessNet().use { net ->
                net.store(home).insert(relayList(observer, at = 1_000, outbox))
                net.store(outbox).insert(providerList(observer, service1.pubKey, providerA, at = 1_100))
                net.store(providerA).insert(score(service1, bob.pubKey, 60, at = 1_200))
                net.store(providerA).insert(score(service1, carol.pubKey, 40, at = 1_201))

                val store = localStore()
                store.use {
                    trustSync(net, store).run(
                        indexRelays = listOf(net.url(index)), // empty relay: home is what bootstraps
                        house = HouseAccount(observer.pubKey, net.url(home)),
                    )

                    assertEquals(1, store.count(Filter(kinds = listOf(AdvertisedRelayListEvent.KIND))), "the 10002 came from the home relay")
                    assertEquals(1, store.count(Filter(kinds = listOf(TrustProviderListEvent.KIND))), "the 10040 came from the outbox")
                    assertEquals(2, store.count(Filter(kinds = listOf(ContactCardEvent.KIND))), "the scores came from the provider")
                }
            }
        }

    /**
     * The freshest 10040 lives on the observer's outbox: a stale hint from the
     * index relay names provider 1, the outbox names provider 2 — provider 2's
     * scores sync, and provider 1's LEFTOVER scores (from the pass that
     * trusted the stale list) are orphan-swept.
     */
    @Test
    fun `outbox 10040 is authoritative and a provider switch sweeps the old scores`() =
        runBlocking {
            InProcessNet().use { net ->
                net.store(index).insert(relayList(observer, at = 1_000, outbox))
                net.store(index).insert(providerList(observer, service1.pubKey, providerA, at = 500)) // stale hint
                net.store(outbox).insert(providerList(observer, service2.pubKey, providerB, at = 2_000)) // authoritative
                net.store(providerA).insert(score(service1, bob.pubKey, 10, at = 800))
                net.store(providerB).insert(score(service2, bob.pubKey, 70, at = 900))

                val store = localStore()
                store.use {
                    // A previous pass, run while the stale list ruled, left provider 1's score behind.
                    store.insert(score(service1, bob.pubKey, 10, at = 800))

                    trustSync(net, store).run(
                        observers = setOf(observer.pubKey), // the observer is known; the sync resolves their outbox
                        indexRelays = listOf(net.url(index)),
                    )

                    val lists = store.query<TrustProviderListEvent>(Filter(kinds = listOf(TrustProviderListEvent.KIND)))
                    assertEquals(listOf(2_000L), lists.map { it.createdAt }, "the outbox version superseded the hint")
                    assertEquals(setOf(service2.pubKey), store.scoreAuthors(), "provider 2 synced; provider 1's leftovers swept")
                }
            }
        }

    /** An observer with no 10002 anywhere degrades to the index relays — never dropped. */
    @Test
    fun `an observer without a relay list falls back to the index relays`() =
        runBlocking {
            InProcessNet().use { net ->
                net.store(index).insert(providerList(observer, service1.pubKey, providerA, at = 1_000))
                net.store(providerA).insert(score(service1, bob.pubKey, 55, at = 1_100))

                val store = localStore()
                store.use {
                    trustSync(net, store).run(
                        observers = setOf(observer.pubKey),
                        indexRelays = listOf(net.url(index)),
                    )

                    assertEquals(1, store.count(Filter(kinds = listOf(TrustProviderListEvent.KIND))), "the 10040 came from the index fallback")
                    assertEquals(setOf(service1.pubKey), store.scoreAuthors())
                }
            }
        }

    /**
     * The blended pass fans out across relays: two observers with outboxes on
     * DIFFERENT relays, each trusting a different provider on a different
     * relay. Both chains resolve independently — neither observer's scores
     * wait on the other's relays — and both providers' scores land.
     */
    @Test
    fun `two observers on different outboxes each sync their own provider`() =
        runBlocking {
            InProcessNet().use { net ->
                net.store(index).insert(relayList(observer, at = 1_000, outbox))
                net.store(index).insert(relayList(observer2, at = 1_000, outbox2))
                net.store(outbox).insert(providerList(observer, service1.pubKey, providerA, at = 1_100))
                net.store(outbox2).insert(providerList(observer2, service2.pubKey, providerB, at = 1_100))
                net.store(providerA).insert(score(service1, bob.pubKey, 60, at = 1_200))
                net.store(providerB).insert(score(service2, carol.pubKey, 40, at = 1_200))

                val store = localStore()
                store.use {
                    trustSync(net, store).run(
                        observers = setOf(observer.pubKey, observer2.pubKey),
                        indexRelays = listOf(net.url(index)),
                    )
                    assertEquals(setOf(service1.pubKey, service2.pubKey), store.scoreAuthors(), "both providers synced")
                    assertEquals(2, store.count(Filter(kinds = listOf(ContactCardEvent.KIND))), "bob and carol scored")
                }
            }
        }

    /**
     * The end-of-pass CATCH-UP backstop: a 10040 already in the local store
     * (from a prior pass) whose observer has no 10002 ANYWHERE and no seed/index
     * source this pass — no outbox unit can run — still gets its providers
     * synced, because the catch-up derives them from the final store state.
     */
    @Test
    fun `a stored 10040 with no discoverable 10002 still syncs its providers via catch-up`() =
        runBlocking {
            InProcessNet().use { net ->
                net.store(providerA).insert(score(service1, bob.pubKey, 55, at = 1_100))

                val store = localStore()
                store.use {
                    // Left behind by an earlier pass; nothing this pass re-discovers it.
                    store.insert(providerList(observer, service1.pubKey, providerA, at = 1_000))

                    trustSync(net, store).run(indexRelays = emptyList())

                    assertEquals(setOf(service1.pubKey), store.scoreAuthors(), "catch-up synced the orphaned 10040's provider")
                }
            }
        }

    /** A score the provider silently removed (no kind 5, no supersession) is detected and deleted. */
    @Test
    fun `reconcile deletes scores the provider no longer serves`() =
        runBlocking {
            InProcessNet().use { net ->
                net.store(providerA).insert(score(service1, bob.pubKey, 60, at = 900))

                val store = localStore()
                store.use {
                    // Previous pass: the provider also served carol's score back then.
                    store.insert(providerList(observer, service1.pubKey, providerA, at = 1_000))
                    store.insert(score(service1, bob.pubKey, 60, at = 900))
                    store.insert(score(service1, carol.pubKey, 40, at = 901))

                    trustSync(net, store).run(indexRelays = listOf(net.url(index)))

                    val subjects =
                        store
                            .query<ContactCardEvent>(Filter(kinds = listOf(ContactCardEvent.KIND)))
                            .mapNotNull { it.tags.firstOrNull { t -> t.size > 1 && t[0] == "d" }?.get(1) }
                    assertEquals(listOf(bob.pubKey), subjects, "carol's silently-removed score is gone")
                }
            }
        }
}
