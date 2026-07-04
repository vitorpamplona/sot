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

import com.vitorpamplona.quartz.nip01Core.relay.filters.Filter

/**
 * Incrementality bookkeeping: how a [Filter] maps to its persisted `since`
 * cursor key, and how a stored cursor scopes the next download. This is the one
 * place the cursor's identity is defined. Change the key here and every
 * relay/scope pair re-syncs from scratch, so it pays to keep it small and easy
 * to see.
 */
internal object CursorScope {
    /**
     * Cursor scope key: all of the filter's kinds, plus authors so per-provider
     * 30382 syncs don't share a cursor. Joining every kind (not just the first)
     * keeps a multi-kind filter's cursor distinct from any single-kind one. A
     * single-kind filter still scopes to exactly its kind, so existing per-kind
     * cursors are byte-identical.
     */
    fun of(filter: Filter): String {
        val kinds = filter.kinds?.joinToString(",") ?: "-1"
        val authors = filter.authors?.let { ":" + it.joinToString(",") } ?: ""
        return "$kinds$authors"
    }

    /** Scope [filter] to the persisted [cursor] — minus [slackSecs] to absorb back-dated events — if one exists. */
    fun since(
        filter: Filter,
        cursor: Long?,
        slackSecs: Long,
    ): Filter {
        val since = cursor?.minus(slackSecs) ?: return filter
        return filter.copy(since = since)
    }
}
