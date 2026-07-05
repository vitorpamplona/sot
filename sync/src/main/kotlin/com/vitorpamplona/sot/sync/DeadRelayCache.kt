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

import com.vitorpamplona.quartz.nip01Core.relay.normalizer.NormalizedRelayUrl

/**
 * Remembers relays that just failed so the discovery crawl doesn't keep paying
 * the connect/timeout cost on them. Of the thousands of relay URLs a broad
 * harvest turns up, most are dead — this keeps the sweep from drowning in dead
 * sockets, both within a pass and (because the marks live in the persisted
 * [SyncState]) across passes, so a periodic re-run doesn't re-time-out every
 * dead relay from scratch.
 *
 * A mark is a TEMPORARY skip, never a permanent ban: it expires after [ttlMs],
 * at which point the relay is retried, so a relay that was merely having downtime
 * comes back into rotation on its own. (A future refinement could escalate the
 * TTL for relays that stay dead across several re-checks.)
 */
internal class DeadRelayCache(
    private val state: SyncState,
    private val ttlMs: Long = DEFAULT_TTL_MS,
    private val clock: () -> Long = System::currentTimeMillis,
) {
    fun isDead(relay: NormalizedRelayUrl): Boolean = state.isRelayDead(relay, clock())

    fun markDead(relay: NormalizedRelayUrl) = state.markRelayDead(relay, clock() + ttlMs)

    fun size(): Int = state.activeDeadCount(clock())

    companion object {
        // How long a dead relay is skipped before it is re-checked. Long enough
        // that a periodic sync stops re-paying the connect timeout on the same
        // dead relays every pass, short enough that a day's downtime doesn't hide
        // a relay for much longer than a day.
        const val DEFAULT_TTL_MS = 6 * 60 * 60_000L
    }
}
