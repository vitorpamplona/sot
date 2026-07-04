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

    /**
     * Bulk [put]: same contract (all acked and visible on return), but an
     * implementation may pipeline the writes — the real client keeps them all
     * in flight at once, which is what makes million-event ingest feasible.
     */
    suspend fun putAll(docs: List<EventDoc>) = docs.forEach { put(it) }

    suspend fun remove(id: String)

    /** Docs matching [query]: newest first (`created_at` desc, id asc tiebreak) unless ranked by a search term. */
    suspend fun search(query: EventQuery): List<EventDoc>

    /**
     * Stream EVERY match's (id, created_at) — the full-corpus walk behind
     * negentropy snapshots and sync reconcile diffs. Unlike [search] there is
     * no result cap: the real client pages through Vespa's document-API visit
     * (a streaming scan, not a query), calling [onPage] per page; order across
     * pages is engine-defined, and callers must not assume recency.
     * [withDTag] additionally projects each doc's `d` tag — what an
     * addressable-corpus walk (rebuilding the trust projection) keys on. This
     * default rides [search] and is only complete where search is uncapped
     * (the in-memory reference).
     */
    suspend fun visitIds(
        query: EventQuery,
        withDTag: Boolean = false,
        onPage: suspend (List<DocRef>) -> Unit,
    ) = onPage(search(query).map { DocRef(it.id, it.createdAt, if (withDTag) it.dTag() else null) })

    suspend fun count(query: EventQuery): Int
}

/** The (id, created_at[, d tag]) projection [EventIndex.visitIds] streams — all a sync diff or projection walk needs. */
data class DocRef(
    val id: String,
    val createdAt: Long,
    val dTag: String? = null,
)
