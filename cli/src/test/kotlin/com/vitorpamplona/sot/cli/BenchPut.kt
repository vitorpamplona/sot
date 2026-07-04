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

import com.vitorpamplona.sot.vespa.EventDoc
import com.vitorpamplona.sot.vespa.VespaEventIndex
import kotlinx.coroutines.runBlocking

/** Throwaway: isolate putAll dispatch behavior against the local Vespa. */
object BenchPut {
    @JvmStatic
    fun main(args: Array<String>) {
        val n = args.getOrNull(0)?.toIntOrNull() ?: 100
        runBlocking {
            val index = VespaEventIndex()
            val docs = (0 until n).map { i -> EventDoc(id = "bb%060x".format(i), pubkey = "cc".repeat(32), createdAt = 1_700_000_000L + i, kind = 1, tags = emptyList(), content = "bench $i", sig = "") }
            val t0 = System.currentTimeMillis()
            index.putAll(docs)
            println("putAll($n) took ${System.currentTimeMillis() - t0}ms")
            index.close()
        }
    }
}
