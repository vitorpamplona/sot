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
import com.vitorpamplona.quartz.utils.Hex
import kotlinx.coroutines.runBlocking
import okhttp3.OkHttpClient

/*
 * Key/identifier resolution shared by the interactive `sot init` and the
 * composition root: users type whatever form they have on hand.
 */

private val nip05 = Nip05Client(OkHttpNip05Fetcher { OkHttpClient() })

/**
 * Resolve a user identifier to a hex pubkey: 64-hex, NIP-19 `npub`/`nprofile`,
 * or a NIP-05 `name@domain` (looked up over HTTPS). Returns null when it isn't
 * one of those.
 */
internal fun resolvePubkey(input: String): String? {
    val s = input.trim()
    if (s.isEmpty()) return null
    // Quartz's decoder is lenient (partial hex decodes to a short string);
    // only a full 64-hex key is a pubkey.
    return runCatching { runBlocking { resolveUserHexOrNull(s, nip05) } }
        .getOrNull()
        ?.takeIf(Hex::isHex64)
}
