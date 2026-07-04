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

import com.vitorpamplona.quartz.nip01Core.core.Event
import com.vitorpamplona.quartz.nip01Core.core.HexKey
import com.vitorpamplona.quartz.nip01Core.core.hexToByteArray
import com.vitorpamplona.quartz.nip01Core.crypto.KeyPair
import com.vitorpamplona.quartz.nip01Core.metadata.MetadataEvent
import com.vitorpamplona.quartz.nip01Core.relay.filters.Filter
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.NormalizedRelayUrl
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.RelayUrlNormalizer
import com.vitorpamplona.quartz.nip01Core.signers.NostrSignerSync
import com.vitorpamplona.quartz.nip01Core.store.IEventStore
import com.vitorpamplona.quartz.nip19Bech32.Nip19Parser
import com.vitorpamplona.quartz.nip19Bech32.entities.NSec
import com.vitorpamplona.quartz.nip51Lists.relayLists.IndexerRelayListEvent
import com.vitorpamplona.quartz.nip51Lists.relayLists.tags.RelayTag
import com.vitorpamplona.quartz.nip65RelayList.AdvertisedRelayListEvent
import com.vitorpamplona.quartz.nip65RelayList.tags.AdvertisedRelayInfo
import com.vitorpamplona.quartz.nip65RelayList.tags.AdvertisedRelayType
import com.vitorpamplona.quartz.utils.Hex
import com.vitorpamplona.quartz.utils.TimeUtils

/**
 * The relay's OWN Nostr identity (the key from `SERVER_NSEC`), used two ways:
 *
 *  - **as a NIP-42 client**: the composition root wraps [signer] in Quartz's
 *    `RelayAuthenticator` so auth-required upstream relays serve our sync
 *    connections. It is also the NIP-11 `self`.
 *  - **as an author**: [ensurePublished] signs and inserts the identity's own
 *    events into its OWN store on first run. There the operator (or anyone) can
 *    read them, and the operator can supersede them any time from a normal
 *    Nostr client. Replaceable supersession keeps the newest, with no config
 *    redeploy. The events are:
 *      - **kind 0** — service name/description/icon, seeded from config;
 *      - **kind 10002** — pointing at this relay's own URL;
 *      - **kind 10086** ([IndexerRelayListEvent]) — the indexer relay list,
 *        seeded with [DEFAULT_INDEX_RELAYS].
 *
 * **The stored 10086 IS the indexer configuration.** [indexRelays] reads the
 * newest one back wherever the sync needs "the index relays": the relays used
 * to find 10002s for pubkeys we don't hold yet, and the fallback for authors
 * with no relay list. Changing indexers means publishing a new 10086.
 */
class Identity(
    val signer: NostrSignerSync,
    /** This relay's own public URL — what the identity's 10002 advertises. Null skips the 10002. */
    private val selfRelayUrl: NormalizedRelayUrl? = null,
    /** First-run kind-0 seeds; once published, the STORED kind 0 rules. */
    private val name: String = "SoT",
    private val about: String = "Search over Trust — a web-of-trust Nostr search relay.",
    private val icon: String? = null,
    /** First-run 10086 seed; once published, the STORED 10086 rules. */
    private val indexRelaySeeds: List<NormalizedRelayUrl> = DEFAULT_INDEX_RELAYS,
) {
    val pubkey: HexKey get() = signer.pubKey

    /**
     * First-run self-publish: for each of the identity's kinds the store
     * doesn't hold yet, sign the seed version and insert it. Kinds the store
     * already has (any prior run, or an operator supersede) are left alone.
     */
    suspend fun ensurePublished(store: IEventStore) {
        if (missing(store, MetadataEvent.KIND)) {
            insert(store, signer.sign(MetadataEvent.createNew(name = name, about = about, picture = icon, createdAt = TimeUtils.now())))
        }
        if (selfRelayUrl != null && missing(store, AdvertisedRelayListEvent.KIND)) {
            insert(store, AdvertisedRelayListEvent.create(listOf(AdvertisedRelayInfo(selfRelayUrl, AdvertisedRelayType.BOTH)), signer))
        }
        if (missing(store, IndexerRelayListEvent.KIND)) {
            // PUBLIC relay tags (not NIP-51 private content): the list is the
            // service's advertised configuration, so anyone can audit it.
            val tags = indexRelaySeeds.map { RelayTag.assemble(it) }.toTypedArray()
            insert(store, signer.sign<IndexerRelayListEvent>(TimeUtils.now(), IndexerRelayListEvent.KIND, tags, ""))
        }
    }

    /**
     * The current index relays: the newest stored 10086's public relay tags,
     * falling back to the seeds while the store has none (first run, before
     * [ensurePublished]).
     */
    suspend fun indexRelays(store: IEventStore): List<NormalizedRelayUrl> =
        store
            .query<IndexerRelayListEvent>(Filter(kinds = listOf(IndexerRelayListEvent.KIND), authors = listOf(pubkey)))
            .maxByOrNull { it.createdAt }
            ?.publicRelays()
            ?.takeIf { it.isNotEmpty() }
            ?: indexRelaySeeds

    private suspend fun missing(
        store: IEventStore,
        kind: Int,
    ): Boolean = store.count(Filter(kinds = listOf(kind), authors = listOf(pubkey))) == 0

    private suspend fun insert(
        store: IEventStore,
        event: Event,
    ) {
        // A concurrent writer beating us to it is fine — the stored one rules.
        runCatching { store.insert(event) }
    }

    companion object {
        /** The default 10086 seed: the well-known NIP-65 aggregators. */
        val DEFAULT_INDEX_RELAYS: List<NormalizedRelayUrl> =
            listOf(
                "wss://purplepag.es",
                "wss://indexer.coracle.social",
                "wss://user.kindpag.es",
                "wss://directory.yabu.me",
                "wss://profiles.nostr1.com",
            ).map { RelayUrlNormalizer.normalize(it) }

        /** Parse an `nsec1...` or 64-hex secret key into a signer; null if it's neither. */
        fun signerFromSecret(raw: String): NostrSignerSync? {
            val hex =
                when {
                    raw.startsWith("nsec1") -> (runCatching { Nip19Parser.uriToRoute(raw)?.entity }.getOrNull() as? NSec)?.hex ?: return null
                    Hex.isHex64(raw) -> raw
                    else -> return null
                }
            return NostrSignerSync(KeyPair(privKey = hex.hexToByteArray()))
        }
    }
}
