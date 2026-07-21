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
package com.vitorpamplona.sot.cli

import com.vitorpamplona.quartz.eventstore.store.NostrEventStore
import com.vitorpamplona.quartz.eventstore.store.VespaEventStore
import com.vitorpamplona.quartz.eventstore.vespa.IngestStats
import com.vitorpamplona.quartz.eventstore.vespa.client.VespaEventIndex
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.NormalizedRelayUrl
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.RelayUrlNormalizer
import com.vitorpamplona.quartz.nip01Core.signers.NostrSignerSync
import com.vitorpamplona.quartz.nip19Bech32.toNpub
import com.vitorpamplona.sot.sync.CrawlIndex
import com.vitorpamplona.sot.sync.FileCrawlIndex
import com.vitorpamplona.sot.sync.HouseAccount
import com.vitorpamplona.sot.sync.Identity
import com.vitorpamplona.sot.sync.SyncOptions
import com.vitorpamplona.sot.sync.SyncService
import java.nio.file.Path
import kotlin.system.exitProcess

/*
 * The composition root: how the pieces plug together, in one place.
 *
 *   VespaEventIndex  (events over Vespa HTTP)
 *        └─ TrustProjection            (:store — watches 30382/10040 puts
 *           └─ VespaReputationIndex     and removes, rewrites the ranking parents)
 *   NostrEventStore(TrustProjection)   (:store — Nostr semantics, ONE store)
 *        ├─ NostrRelayServer             (:relay — serves it)
 *        └─ SyncService                (:sync — fills it)
 *
 * Because the projection sits UNDER the store, every insert path (a sync
 * download, a relay publish, a kind-5) updates ranking with no extra wiring.
 */

/** The wired storage stack: the library store handle, plus the sync-side crawl index. */
internal class Stack(
    private val handle: VespaEventStore,
    val crawl: CrawlIndex,
) : AutoCloseable {
    /** The concrete store — Vespa-specific methods (e.g. distinctDTags) and the full IEventStore surface. */
    val store: NostrEventStore get() = handle.store

    /** The raw (non-projected) engine index — status/health metrics query it directly. */
    val vespa: VespaEventIndex get() = handle.events

    /** The engine's feed-health status gauge — wire it into every sync's progress line. */
    fun feedGauge(): () -> String = handle::feedGauge

    override fun close() {
        handle.close()
        crawl.close()
    }
}

/**
 * The one store, wired through the library front door: a NostrEventStore over a
 * trust-projection-decorated Vespa index. autoDeploy is off here — the CLI owns
 * schema deployment through `sot up` / `sot deploy` (the docker flow). Crawl
 * bookkeeping is sync state, so it lives in a file next to the sync cursors, not
 * in the event store.
 */
internal fun openStack(): Stack =
    Stack(
        VespaEventStore.open(Config.vespaUrl, relay = publicRelayUrl(), autoDeploy = false),
        FileCrawlIndex(crawlStatePath()),
    )

/** The crawl bookkeeping file — a sibling of the sync-state cursor file. */
internal fun crawlStatePath(): Path = Path.of(Config.syncStatePath).resolveSibling("crawl-state.json")

/** The relay's public url (`RELAY_URL`) — NIP-42's identity and the vanish scope. */
internal fun publicRelayUrl(): NormalizedRelayUrl =
    RelayUrlNormalizer.normalizeOrNull(Config.relayUrl) ?: run {
        err("RELAY_URL '${Config.relayUrl}' is not a valid relay url.")
        exitProcess(1)
    }

/** The configured identity, or a one-run one (announced) when `SERVER_NSEC` is unset. */
internal fun serverSigner(): NostrSignerSync {
    val raw = Config.serverNsec.trim()
    if (raw.isEmpty()) {
        val signer = NostrSignerSync()
        warn("SERVER_NSEC is not set - using a one-run identity ${signer.keyPair.pubKey.toNpub()} (run `sot init`, or set SERVER_NSEC, to keep one)")
        return signer
    }
    return Identity.signerFromSecret(raw) ?: run {
        err("SERVER_NSEC is set but is neither an nsec nor a 64-hex secret key.")
        exitProcess(1)
    }
}

/** The identity component over [serverSigner]: self-publish seeds from the service config. */
internal fun serverIdentity(): Identity =
    Identity(
        signer = serverSigner(),
        selfRelayUrl = publicRelayUrl(),
        name = Config.serverName,
        about = Config.serverDescription,
        icon = Config.serverIcon.ifBlank { null },
    )

/** The house account's hex pubkey from HOUSE_NPUB; null (with a warning) when unset or unresolvable. */
internal fun housePubkey(): String? {
    val raw = Config.houseNpub.trim()
    if (raw.isEmpty()) {
        warn("no HOUSE_NPUB - unauthenticated searches rank untrusted until a user AUTHs (or set one via `sot init`)")
        return null
    }
    return resolvePubkey(raw) ?: run {
        warn("HOUSE_NPUB '$raw' is not a resolvable pubkey - ignoring the house account")
        null
    }
}

/**
 * The house account (pubkey + home relay), or exit. `index` and `serve` REQUIRE
 * it: the house is the trust ROOT the whole sync builds from — its own 10002 and
 * 10040 bootstrap the chain, and every other user we index is a subject its trust
 * graph names. Without it there is nothing to root trust at, so we refuse to
 * start rather than sync an empty or unanchored graph.
 */
internal fun requireHouse(): HouseAccount {
    val raw = Config.houseNpub.trim()
    if (raw.isEmpty()) {
        err("HOUSE_NPUB is required: the house account is the trust root the sync builds from. Set HOUSE_NPUB and HOUSE_RELAY (run `sot init`, or edit .env).")
        exitProcess(1)
    }
    val pubkey =
        resolvePubkey(raw) ?: run {
            err("HOUSE_NPUB '$raw' is not a resolvable pubkey (npub or hex).")
            exitProcess(1)
        }
    val relay =
        RelayUrlNormalizer.normalizeOrNull(Config.houseRelay) ?: run {
            err("HOUSE_RELAY is required and must be a valid relay url - the house's kind-10002/10040 bootstrap from it. Got '${Config.houseRelay}'.")
            exitProcess(1)
        }
    return HouseAccount(pubkey, relay)
}

/** The background/foreground sync composition over a shared [stack], rooted at [house]. */
internal fun syncService(
    stack: Stack,
    identity: Identity,
    house: HouseAccount,
): SyncService =
    SyncService(
        store = stack.store,
        crawl = stack.crawl,
        identity = identity,
        house = house,
        seedRelays = Config.seedRelays,
        extraObservers = emptySet(),
        statePath = Config.syncStatePath,
        opts = SyncOptions(relayConcurrency = Config.syncRelayConcurrency),
        log = { println(styleLogLine(it)) },
        gauges = listOf(stack.feedGauge(), IngestStats::gauge),
    )
