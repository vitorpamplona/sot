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

import com.vitorpamplona.quartz.nip01Core.core.hexToByteArray
import com.vitorpamplona.quartz.nip01Core.crypto.KeyPair
import com.vitorpamplona.quartz.nip01Core.signers.NostrSignerSync
import com.vitorpamplona.quartz.nip19Bech32.Nip19Parser
import com.vitorpamplona.quartz.nip19Bech32.entities.NSec
import com.vitorpamplona.quartz.nip19Bech32.toNpub
import com.vitorpamplona.quartz.utils.Hex
import com.vitorpamplona.sot.config.Config
import kotlin.system.exitProcess

/*
 * The relay's OWN Nostr identity (`SERVER_NSEC`): its pubkey is the NIP-11
 * `self`, and the key signs NIP-42 AUTH replies when upstream relays challenge
 * our sync connections (auth-required relays serve nothing without it).
 * `sot init` generates one; unset means a fresh one-run identity.
 */

/** Parse an `nsec1...` or 64-hex secret key into a signer; null if it's neither. */
internal fun signerFromSecret(raw: String): NostrSignerSync? {
    val hex =
        when {
            raw.startsWith("nsec1") -> (runCatching { Nip19Parser.uriToRoute(raw)?.entity }.getOrNull() as? NSec)?.hex ?: return null
            Hex.isHex64(raw) -> raw
            else -> return null
        }
    return NostrSignerSync(KeyPair(privKey = hex.hexToByteArray()))
}

/** The configured identity, or a one-run one (announced) when `SERVER_NSEC` is unset. */
internal fun serverSigner(): NostrSignerSync {
    val raw = Config.serverNsec.trim()
    if (raw.isEmpty()) {
        val signer = NostrSignerSync()
        warn("SERVER_NSEC is not set - using a one-run identity ${signer.keyPair.pubKey.toNpub()} (run `sot init`, or set SERVER_NSEC, to keep one)")
        return signer
    }
    return signerFromSecret(raw) ?: run {
        err("SERVER_NSEC is set but is neither an nsec nor a 64-hex secret key.")
        exitProcess(1)
    }
}
