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

/**
 * A run-scoped "already handled this event" filter. A broad relay walk re-receives
 * the same event from dozens of relays (a widely-mirrored 10002, a popular note):
 * on the last run ~95% of everything received was a duplicate we already had. This
 * drops those the moment they arrive — BEFORE signature verification and BEFORE the
 * store's per-batch Vespa existence check — turning each duplicate from
 * "verify + a Vespa read" into a nanosecond array probe.
 *
 * Event ids are SHA-256 hashes, i.e. uniform random 256-bit values, so their first
 * 128 bits (two longs of the 32-byte id) are themselves a perfect hash. Keying on
 * those, the odds of two distinct ids colliding across tens of millions of events
 * is ~1e-22 — it never wrongly skips a real event, unlike a Bloom filter.
 *
 * Backed by one open-addressed [LongArray] (two longs per slot, `(0,0)` = empty),
 * so there are NO per-entry objects: tens of millions of ids cost ~1 GB instead of
 * the ~6 GB a `HashSet<String>` of 64-char hex would. add() is O(1) and
 * `@Synchronized`; the lock is held for nanoseconds, so the concurrent relay
 * producers barely contend.
 *
 * Scope is ONE pass ([reset] between passes) so it can't grow without bound on a
 * long-running `serve`; cross-pass duplicates are caught by the persisted cursor
 * and the store anyway.
 */
internal class SeenIds(
    initialSlotsPow2: Int = 20,
) {
    private var mask = 0
    private var table = LongArray(0)
    private var count = 0 // non-zero-key entries held in [table]
    private var zeroSeen = false // the (0,0) key, tracked apart from the empty sentinel
    private var resizeAt = 0

    init {
        allocate(1 shl initialSlotsPow2)
    }

    private fun allocate(slots: Int) {
        table = LongArray(slots * 2)
        mask = slots - 1
        resizeAt = (slots * LOAD).toInt()
        count = 0
    }

    /**
     * Records [idHex] (a 64-char hex event id); returns true if it is NEW this pass
     * (caller should process it), false if already seen (caller should skip it). A
     * malformed id returns true — it flows through and verification drops it.
     */
    @Synchronized
    fun add(idHex: String): Boolean {
        val hi: Long
        val lo: Long
        try {
            hi = java.lang.Long.parseUnsignedLong(idHex, 0, 16, 16)
            lo = java.lang.Long.parseUnsignedLong(idHex, 16, 32, 16)
        } catch (_: Exception) {
            return true
        }
        return addKey(hi, lo)
    }

    private fun addKey(
        hi: Long,
        lo: Long,
    ): Boolean {
        if (hi == 0L && lo == 0L) {
            // (0,0) is [table]'s empty sentinel, so this one key is tracked apart.
            if (zeroSeen) return false
            zeroSeen = true
            return true
        }
        if (count >= resizeAt) grow()
        var i = (mix(hi, lo).toInt() and mask)
        while (true) {
            val s = i * 2
            val h = table[s]
            val l = table[s + 1]
            if (h == 0L && l == 0L) {
                table[s] = hi
                table[s + 1] = lo
                count++
                return true
            }
            if (h == hi && l == lo) return false
            i = (i + 1) and mask
        }
    }

    private fun grow() {
        val old = table
        allocate((mask + 1) shl 1) // resets count; zeroSeen is untouched
        var j = 0
        while (j < old.size) {
            val h = old[j]
            val l = old[j + 1]
            if (h != 0L || l != 0L) addKey(h, l)
            j += 2
        }
    }

    @Synchronized
    fun reset() {
        allocate(1 shl INITIAL_POW2)
        zeroSeen = false
    }

    @Synchronized
    fun size() = count + if (zeroSeen) 1 else 0

    // Ids are already uniform, but avalanche the two halves so the low bits used for
    // the slot index don't correlate with any particular byte of the hash.
    private fun mix(
        hi: Long,
        lo: Long,
    ): Long {
        var h = hi xor (lo * -0x61c8864680b583ebL)
        h = h xor (h ushr 32)
        h *= -0x7ee3623a03d3f7d7L
        h = h xor (h ushr 29)
        return h
    }

    companion object {
        private const val LOAD = 0.7
        private const val INITIAL_POW2 = 20
    }
}
