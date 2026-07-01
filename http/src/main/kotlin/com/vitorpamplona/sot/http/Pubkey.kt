package com.vitorpamplona.sot.http

import com.vitorpamplona.quartz.nip19Bech32.Nip19Parser
import com.vitorpamplona.quartz.nip19Bech32.entities.NPub
import com.vitorpamplona.quartz.utils.Hex

/**
 * Resolve a pubkey identifier to a 64-char hex pubkey, or null if it isn't one.
 * Accepts a raw hex key or a NIP-19 `npub1…`. Used both for the `observer`
 * parameter and to detect when the query text is itself a pubkey (doc lookup).
 */
internal fun resolvePubkey(text: String): String? =
    when {
        Hex.isHex64(text) -> text.lowercase()
        text.startsWith("npub1") -> (Nip19Parser.uriToRoute(text)?.entity as? NPub)?.hex
        else -> null
    }
