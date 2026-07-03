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
package com.vitorpamplona.sot.v2.cli

import com.vitorpamplona.quartz.nip01Core.relay.normalizer.NormalizedRelayUrl
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.RelayUrlNormalizer
import com.vitorpamplona.quartz.nip01Core.signers.NostrSignerSync
import com.vitorpamplona.quartz.nip19Bech32.toNpub
import com.vitorpamplona.sot.v2.profile.TrustProjection
import com.vitorpamplona.sot.v2.store.VespaEventStore
import com.vitorpamplona.sot.v2.sync.HouseAccount
import com.vitorpamplona.sot.v2.sync.Identity
import com.vitorpamplona.sot.v2.sync.SyncOptions
import com.vitorpamplona.sot.v2.sync.SyncService
import com.vitorpamplona.sot.v2.vespa.VespaEventIndex
import com.vitorpamplona.sot.v2.vespa.VespaProfileIndex
import kotlin.system.exitProcess

/*
 * The v2 composition root: how the pieces plug together, in one place.
 *
 *   VespaEventIndex  (events over Vespa HTTP)
 *        └─ TrustProjection            (:v2:profile — watches 30382/10040 puts
 *           └─ VespaProfileIndex        and removes, rewrites the ranking parents)
 *   VespaEventStore(TrustProjection)   (:v2:store — Nostr semantics, ONE store)
 *        ├─ SotRelayServer             (:v2:relay — serves it)
 *        └─ SyncService                (:v2:sync — fills it)
 *
 * Because the projection sits UNDER the store, every insert path — a sync
 * download, a relay publish, a kind-5 — updates ranking with no extra wiring.
 */

/** The one store: Vespa event index, trust-projection-decorated, Nostr semantics on top. */
internal fun openStore(): VespaEventStore {
    val index = TrustProjection(VespaEventIndex(Config.vespaUrl), VespaProfileIndex(Config.vespaUrl))
    return VespaEventStore(index, relay = publicRelayUrl())
}

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

/** The background/foreground sync composition over a shared [store]. */
internal fun syncService(
    store: VespaEventStore,
    identity: Identity,
): SyncService {
    val pubkey = housePubkey()
    val homeRelay = RelayUrlNormalizer.normalizeOrNull(Config.houseRelay)
    val house = if (pubkey != null && homeRelay != null) HouseAccount(pubkey, homeRelay) else null
    if (pubkey != null && homeRelay == null) {
        warn("HOUSE_RELAY '${Config.houseRelay}' is not a valid relay url - the house 10002 will only resolve via the index relays")
    }
    return SyncService(
        store = store,
        identity = identity,
        house = house,
        seedRelays = Config.seedRelays,
        // A house account without a usable home relay still syncs as a plain observer.
        extraObservers = setOfNotNull(pubkey.takeIf { house == null }),
        statePath = Config.syncStatePath,
        opts = SyncOptions(),
        log = { println(styleLogLine(it)) },
    )
}
