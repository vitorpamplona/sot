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

import kotlinx.coroutines.runBlocking

/**
 * `sot index` — ONE pass of the trust-sync chain (seed hints -> observer
 * 10002s -> outboxes -> providers -> orphan sweep) into Vespa, then exit. The
 * first run also self-publishes the identity's kind 0/10002/10086. Don't run it
 * while `sot serve` is mid-pass against the same Vespa: two writers
 * double-download. Semantics stay correct, but bandwidth is wasted.
 */
internal fun index(args: List<String>) {
    ensureVespaIsUp(args)
    val identity = serverIdentity()
    val stack = openStack()
    val store = stack.store
    val sync = syncService(stack, identity)
    try {
        runBlocking { sync.runOnce() }
        ok("pass complete.")
    } finally {
        sync.close()
        store.close()
    }
}
