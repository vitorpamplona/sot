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

import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.sync.Semaphore
import kotlinx.coroutines.sync.withPermit

/**
 * How many engine queries a batch stage keeps in flight. Serialized round trips
 * starve a batch, but UNBOUNDED fan-out is worse: a dozen concurrent
 * multi-thousand-summary queries time out proton's summary stage (`504 Summary
 * data is incomplete`). This is measured, not hypothetical.
 */
const val QUERY_FANOUT = 4

/** Map [items] through [f] with at most [concurrency] in flight; results keep item order. */
suspend fun <T, R> List<T>.mapBounded(
    concurrency: Int,
    f: suspend (T) -> R,
): List<R> =
    coroutineScope {
        val gate = Semaphore(concurrency.coerceAtLeast(1))
        map { item -> async { gate.withPermit { f(item) } } }.awaitAll()
    }
