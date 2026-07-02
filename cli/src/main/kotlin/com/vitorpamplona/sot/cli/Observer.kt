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

import com.vitorpamplona.quartz.nip05DnsIdentifiers.Nip05Client
import com.vitorpamplona.quartz.nip05DnsIdentifiers.OkHttpNip05Fetcher
import com.vitorpamplona.quartz.nip05DnsIdentifiers.resolveUserHexOrNull
import kotlinx.coroutines.runBlocking
import okhttp3.OkHttpClient

/**
 * Resolves the ranking observer to a hex pubkey. Quartz's [resolveUserHexOrNull]
 * accepts any of the forms a user might type — a 64-char hex pubkey, a NIP-19
 * `npub`/`nprofile`, or a NIP-05 identifier (`name@domain`, looked up over
 * HTTPS) — and the search core keys trust by hex.
 */
private val nip05 = Nip05Client(OkHttpNip05Fetcher { OkHttpClient() })

/** Resolve [input] to a hex pubkey, or null if it isn't a recognizable observer. */
internal fun resolveObserver(input: String): String? {
    val s = input.trim()
    if (s.isEmpty()) return null
    return runBlocking { resolveUserHexOrNull(s, nip05) }
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
