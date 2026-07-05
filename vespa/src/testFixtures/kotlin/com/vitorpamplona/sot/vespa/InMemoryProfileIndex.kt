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
package com.vitorpamplona.sot.vespa
import com.vitorpamplona.sot.vespa.doc.ProfileDoc
import com.vitorpamplona.sot.vespa.doc.ProfileIndex
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicInteger

/** The in-memory reference [ProfileIndex] — what projection tests assert against. */
class InMemoryProfileIndex : ProfileIndex {
    val docs = ConcurrentHashMap<String, ProfileDoc>()

    /** Count of mutating writes (put/remove) — lets tests prove a recompute was (not) issued. */
    val writes = AtomicInteger(0)

    override suspend fun get(pubkey: String): ProfileDoc? = docs[pubkey]

    override suspend fun put(profile: ProfileDoc) {
        writes.incrementAndGet()
        docs[profile.pubkey] = profile
    }

    override suspend fun remove(pubkey: String) {
        writes.incrementAndGet()
        docs.remove(pubkey)
    }

    override fun close() {}
}
