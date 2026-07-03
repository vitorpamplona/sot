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
package com.vitorpamplona.sot.v2.vespa

/**
 * The engine port an event store talks to: document-keyed get/put/remove plus
 * [EventQuery] recall. Implementations: the real Vespa client (document API +
 * feed + `/search/`) and the in-memory reference in this module's testFixtures
 * — which doubles as the executable spec of [EventQuery]'s matching semantics.
 *
 * [get]/[put]/[remove] must be read-your-writes consistent per document, and
 * an acked [put] must be visible to [search] — Vespa's proton gives both (the
 * memory index is updated on the write path), which is what makes
 * query-then-write semantics sound under a single writer.
 */
interface EventIndex : AutoCloseable {
    suspend fun get(id: String): EventDoc?

    suspend fun put(doc: EventDoc)

    suspend fun remove(id: String)

    /** Docs matching [query]: newest first (`created_at` desc, id asc tiebreak) unless ranked by a search term. */
    suspend fun search(query: EventQuery): List<EventDoc>

    suspend fun count(query: EventQuery): Int
}
