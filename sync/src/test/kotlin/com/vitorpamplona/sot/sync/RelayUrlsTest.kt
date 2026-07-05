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
import kotlin.test.assertEquals

class RelayUrlsTest {
    private fun n(u: String) = RelayUrlNormalizer.normalize(u)

    @Test
    fun `per-user path variants collapse onto the bare host when it is observed`() {
        val relays =
            setOf(
                n("wss://relay.band"),
                n("wss://relay.band/npub1abc"),
                n("wss://relay.band/trusted"),
            )
        assertEquals(setOf(n("wss://relay.band")), RelayUrls.collapse(relays), "all paths merge onto the advertised bare host")
    }

    @Test
    fun `a path is kept when no shorter form is observed`() {
        // relay.minds.com/nostr/v1/ws is a real endpoint; nobody advertises bare
        // relay.minds.com, so we must NOT strip it to a host that may not serve.
        val relays = setOf(n("wss://relay.minds.com/nostr/v1/ws"))
        assertEquals(relays, RelayUrls.collapse(relays))
    }

    @Test
    fun `different hosts never merge`() {
        val relays = setOf(n("wss://a.test"), n("wss://b.test/me"))
        assertEquals(relays, RelayUrls.collapse(relays))
    }

    @Test
    fun `collapses onto an intermediate observed prefix, not just the bare host`() {
        // /a is observed but bare host is not: /a/b merges to /a; the sibling /c stays.
        val relays = setOf(n("wss://r.test/a"), n("wss://r.test/a/b"), n("wss://r.test/c"))
        assertEquals(setOf(n("wss://r.test/a"), n("wss://r.test/c")), RelayUrls.collapse(relays))
    }

    @Test
    fun `a segment boundary is respected so sibling paths do not merge`() {
        // /ab must NOT swallow /abc — they are different relays.
        val relays = setOf(n("wss://r.test/ab"), n("wss://r.test/abc"))
        assertEquals(relays, RelayUrls.collapse(relays))
    }
}
