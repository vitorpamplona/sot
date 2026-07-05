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

import com.vitorpamplona.quartz.nip01Core.relay.normalizer.RelayUrlNormalizer
import kotlin.test.Test
import kotlin.test.assertFalse
import kotlin.test.assertTrue

class DeadRelayCacheTest {
    private val relay = RelayUrlNormalizer.normalize("wss://dead.test")

    @Test
    fun `a marked relay is dead until its ttl expires, then retried`() {
        var now = 1_000L
        val cache = DeadRelayCache(ttlMs = 100, clock = { now })

        assertFalse(cache.isDead(relay), "unknown relay is not dead")
        cache.markDead(relay)
        assertTrue(cache.isDead(relay), "just marked")

        now += 50
        assertTrue(cache.isDead(relay), "still within the ttl")

        now += 60 // now 110ms after the mark, past ttl=100
        assertFalse(cache.isDead(relay), "past the ttl - retried")
        assertFalse(cache.isDead(relay), "and cleared, so still not dead")
    }
}
