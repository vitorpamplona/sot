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
package com.vitorpamplona.sot.relay

import com.vitorpamplona.quartz.nip11RelayInfo.relayInformation

private const val SOFTWARE = "https://github.com/vitorpamplona/sot"
private const val VERSION = "2.0"

/**
 * The NIP-11 relay information document (served on `GET /` with Accept:
 * application/nostr+json). Identity comes from the composition root's config;
 * the technical fields describe what [SotRelayServer] actually implements.
 */
fun relayInfoJson(
    name: String = "sot",
    description: String? = null,
    icon: String? = null,
    contactPubkey: String? = null,
    selfPubkey: String? = null,
): String =
    relayInformation {
        this.name = name
        description.ifSet { this.description = it }
        icon.ifSet { this.icon = it }
        contactPubkey.ifSet { pubkey = it } // admin contact
        selfPubkey.ifSet { self = it } // the relay's OWN key
        software = SOFTWARE
        version = VERSION
        // 01 filters+publishes, 09 deletion, 11 this doc, 40 expiration,
        // 42 optional auth (picks the ranking observer), 45 COUNT,
        // 50 search, 62 vanish, 77 negentropy — all store- or engine-enforced.
        supports(1, 9, 11, 40, 42, 45, 50, 62, 77)
        limitation {
            authRequired = false // OptionalAuthPolicy: auth only switches the ranking observer
            paymentRequired = false
            restrictedWrites = false // VerifyPolicy-gated EVENT publishes go to the store
        }
    }.toJson()

/** Run [set] with this string only when it's present and non-blank. */
private inline fun String?.ifSet(set: (String) -> Unit) = this?.takeIf(String::isNotBlank)?.let(set)
