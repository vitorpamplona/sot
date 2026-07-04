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

import com.vitorpamplona.quartz.nip01Core.relay.client.NostrClient
import com.vitorpamplona.quartz.nip01Core.relay.client.accessories.fetchAllPages
import com.vitorpamplona.quartz.nip01Core.relay.client.auth.EmptyIAuthStatus
import com.vitorpamplona.quartz.nip01Core.relay.client.auth.IAuthStatus
import com.vitorpamplona.quartz.nip01Core.relay.filters.Filter
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.NormalizedRelayUrl
import kotlinx.coroutines.delay
import kotlinx.coroutines.withTimeoutOrNull
import java.util.concurrent.ConcurrentHashMap

/**
 * The NIP-42 first-contact concern, lifted out of the sync path: when an
 * [auth] signer is configured, the very first download to each relay must let
 * the AUTH challenge handshake (AUTH -> signed reply -> OK) settle first, or
 * the sync's opening REQs race it and come back empty. Idempotent per relay
 * (connections persist across the kinds a pass syncs), so [awaitOnFirstContact]
 * is a no-op after the first call — the syncer can call it before every
 * download without thinking about it.
 */
internal class NostrAuthHandshake(
    private val client: NostrClient,
    private val auth: IAuthStatus,
) {
    // Relays whose NIP-42 first contact already ran (connections persist across kinds).
    private val authenticated = ConcurrentHashMap.newKeySet<NormalizedRelayUrl>()

    /**
     * First contact with [relay] when a NIP-42 signer is configured: open the
     * connection with a throwaway probe and give the challenge handshake a
     * bounded window. Without this, an auth-required relay rejects the sync's
     * opening downloads — they race the handshake and come back empty. Relays
     * that never challenge just cost the probe.
     */
    suspend fun awaitOnFirstContact(relay: NormalizedRelayUrl) {
        if (auth === EmptyIAuthStatus || !authenticated.add(relay)) return
        runCatching {
            withTimeoutOrNull(AUTH_PROBE_MS + 500) {
                client.fetchAllPages(relay, listOf(Filter(kinds = listOf(0), limit = 1)), timeoutMs = AUTH_PROBE_MS) { }
            }
        }
        // "No auth pending" also describes the instant BEFORE the async challenge
        // reply is recorded - so hold a short grace period unconditionally, then
        // wait (bounded) for the recorded reply's OK.
        val settleUntil = System.currentTimeMillis() + AUTH_GRACE_MS
        val deadline = System.currentTimeMillis() + AUTH_WAIT_MS
        while (System.currentTimeMillis() < deadline) {
            if (System.currentTimeMillis() >= settleUntil && auth.hasFinishedAuthentication(relay)) return
            delay(50)
        }
    }

    private companion object {
        // First-contact NIP-42 handshake: probe REQ bound, settle grace for the
        // async challenge reply to be recorded, total wait for its OK.
        const val AUTH_PROBE_MS = 1_500L
        const val AUTH_GRACE_MS = 500L
        const val AUTH_WAIT_MS = 5_000L
    }
}
