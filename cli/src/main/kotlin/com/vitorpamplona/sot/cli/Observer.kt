package com.vitorpamplona.sot.cli

import com.vitorpamplona.quartz.nip19Bech32.Nip19Parser
import com.vitorpamplona.quartz.nip19Bech32.entities.IPubKeyEntity
import com.vitorpamplona.quartz.utils.Hex
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive

/**
 * Resolves the ranking observer from any of the forms a user might type:
 *  - a 64-char hex pubkey
 *  - a NIP-19 `npub1…` or `nprofile1…`
 *  - a NIP-05 identifier (`name@domain`), looked up over HTTPS
 *
 * The search core keys trust by hex pubkey, so everything funnels down to hex.
 */

/** Resolve [input] to a hex pubkey, or null if it isn't a recognizable observer. */
internal fun resolveObserver(input: String): String? {
    val s = input.trim()
    if (s.isEmpty()) return null
    if (Hex.isHex64(s)) return s.lowercase()
    if (s.startsWith("npub1") || s.startsWith("nprofile1")) {
        return (Nip19Parser.uriToRoute(s)?.entity as? IPubKeyEntity)?.hex
    }
    if ("@" in s) return resolveNip05(s)
    return null
}

/** NIP-05: GET https://<domain>/.well-known/nostr.json?name=<local> -> names[<local>]. */
private fun resolveNip05(id: String): String? {
    val at = id.indexOf('@')
    val local = id.substring(0, at).ifEmpty { "_" }
    val domain = id.substring(at + 1)
    if (domain.isEmpty()) return null
    val body = httpGet("https://$domain/.well-known/nostr.json?name=$local") ?: return null
    return runCatching {
        Json.parseToJsonElement(body).jsonObject["names"]?.jsonObject?.get(local)?.jsonPrimitive?.content
    }.getOrNull()
}

/**
 * Resolve the `--observer` flag (or DEFAULT_OBSERVER) to a hex pubkey. Warns and
 * returns "" (untrusted, every score 0) when nothing usable is given.
 */
internal fun observerOrWarn(args: List<String>): String {
    val raw = flag(args, "--observer", DEFAULT_OBSERVER)
    if (raw.isBlank()) {
        System.err.println(
            "note: no observer set - pass --observer <hex|npub|nprofile|nip05> or set " +
                "DEFAULT_OBSERVER; results are not trust-ranked (every score is 0).",
        )
        return ""
    }
    val hex = resolveObserver(raw)
    if (hex == null) {
        System.err.println("warning: could not resolve observer '$raw' (hex/npub/nprofile/nip05); results are not trust-ranked.")
        return ""
    }
    return hex
}
