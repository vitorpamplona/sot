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
package com.vitorpamplona.sot.vespa.client
import com.vitorpamplona.sot.vespa.doc.EventDoc
import com.vitorpamplona.sot.vespa.query.EventQuery

/**
 * The engine port an event store talks to: document-keyed get/put/remove plus
 * [EventQuery] recall. There are two implementations: the real Vespa client
 * (document API + feed + `/search/`) and the in-memory reference in this
 * module's testFixtures. The reference also serves as the executable spec of
 * [EventQuery]'s matching semantics.
 *
 * [get]/[put]/[remove] must be read-your-writes consistent per document, and an
 * acked [put] must be visible to [search]. Vespa's proton gives both, because
 * the memory index is updated on the write path. That is what makes
 * query-then-write semantics sound under a single writer.
 */
interface EventIndex : AutoCloseable {
    suspend fun get(id: String): EventDoc?

    suspend fun put(doc: EventDoc)

    /**
     * Bulk [put]: same contract (all acked and visible on return), but an
     * implementation may pipeline the writes. The real client keeps them all in
     * flight at once, which is what makes million-event ingest feasible.
     */
    suspend fun putAll(docs: List<EventDoc>) = docs.forEach { put(it) }

    suspend fun remove(id: String)

    /** Bulk [remove]; the real client pipelines the deletes over HTTP/2 (a big sweep is O(1) round trips, not O(N)). */
    suspend fun removeAll(ids: List<String>) = ids.forEach { remove(it) }

    /** Docs matching [query]: newest first (`created_at` desc, id asc tiebreak) unless ranked by a search term. */
    suspend fun search(query: EventQuery): List<EventDoc>

    /**
     * Stream EVERY match's (id, created_at). This is the full-corpus walk
     * behind negentropy snapshots and sync reconcile diffs. Unlike [search]
     * there is no result cap: the real client pages through Vespa's
     * document-API visit (a streaming scan, not a query), calling [onPage] per
     * page. Order across pages is engine-defined, so callers must not assume
     * recency.
     *
     * [onPage] returns whether to CONTINUE; false stops the walk early. (A
     * capped snapshot needn't scan a 10M corpus to learn it exceeds the cap.)
     * [withDTag] also projects each doc's `d` tag, which is what an
     * addressable-corpus walk (rebuilding the trust projection) keys on.
     *
     * This default rides [search], so it is only complete where search is
     * uncapped (the in-memory reference).
     */
    suspend fun visitIds(
        query: EventQuery,
        withDTag: Boolean = false,
        onPage: suspend (List<DocRef>) -> Boolean,
    ) {
        onPage(search(query).map { DocRef(it.id, it.createdAt, if (withDTag) it.dTag() else null) })
    }

    suspend fun count(query: EventQuery): Int

    /**
     * The number of DISTINCT authors (pubkeys) among the matches — what `sot
     * status` reports as "pubkeys with content". The default rides [search], so
     * it is exact only where search is uncapped (the in-memory reference); the
     * real client overrides it with a grouping count over the full match set.
     */
    suspend fun countDistinctAuthors(query: EventQuery): Int = search(query).map { it.pubkey }.distinct().size

    /**
     * How many docs match [query] per kind (kind -> count) — the corpus shape
     * `sot status` prints as "top kinds". The default rides [search] (exact only
     * where uncapped, the in-memory reference); the real client overrides it
     * with a grouping over the full match set.
     */
    suspend fun countByKind(query: EventQuery): Map<Int, Int> = search(query).groupingBy { it.kind }.eachCount()

    /**
     * The DISTINCT `pubkey`s (event authors) across [query]'s match set — the
     * actual author set, not just its size ([countDistinctAuthors]). The default
     * rides [search] (exact only where uncapped, the in-memory reference); the
     * real client overrides it with a server-side grouping over the full match
     * set, so the orphan-score sweep gets the distinct 30382 authors out of
     * millions of docs without reconstructing them (which times search out). A
     * decorator MUST delegate to its inner index, not this default, or it would
     * ride the capped search.
     */
    suspend fun distinctAuthors(query: EventQuery): Set<String> = search(query).mapTo(HashSet()) { it.pubkey }
}

/** The (id, created_at[, d tag]) projection [EventIndex.visitIds] streams — all a sync diff or projection walk needs. */
data class DocRef(
    val id: String,
    val createdAt: Long,
    val dTag: String? = null,
)
