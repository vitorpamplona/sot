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
 * The in-memory reference [EventIndex]: a map of docs plus a direct
 * interpretation of [EventQuery]'s matching semantics. It is the executable
 * spec the real Vespa client must agree with (same fields [EventYql] queries),
 * and what store/relay tests run against without a Vespa container.
 *
 * Search-term matching is a naive case-insensitive substring over the derived
 * search fields — recall-equivalent for tests (docs with no search fields are
 * invisible to search, like SQLite's FTS table); real ranking is Vespa's job.
 */
class InMemoryEventIndex : EventIndex {
    private val docs = LinkedHashMap<String, EventDoc>()

    override suspend fun get(id: String): EventDoc? = docs[id]

    override suspend fun put(doc: EventDoc) {
        docs[doc.id] = doc
    }

    override suspend fun remove(id: String) {
        docs.remove(id)
    }

    override suspend fun search(query: EventQuery): List<EventDoc> {
        val hits = docs.values.filter { it.matches(query) }.sortedWith(NEWEST_FIRST)
        return query.limit?.let(hits::take) ?: hits
    }

    override suspend fun count(query: EventQuery): Int = docs.values.count { it.matches(query) }

    override fun close() {}

    fun size(): Int = docs.size

    private fun EventDoc.matches(q: EventQuery): Boolean {
        val pairs = tagIndex()
        return (q.ids.isEmpty() || id in q.ids) &&
            (q.kinds.isEmpty() || kind in q.kinds) &&
            (q.authors.isEmpty() || pubkey in q.authors) &&
            (q.owners.isEmpty() || owner in q.owners) &&
            q.tags.all { (name, values) -> values.any { v -> "$name:$v" in pairs } } &&
            q.tagsAll.all { (name, values) -> values.all { v -> "$name:$v" in pairs } } &&
            (q.since == null || createdAt >= q.since) &&
            (q.until == null || createdAt <= q.until) &&
            (q.expiresBefore == null || (expiresAt()?.let { it < q.expiresBefore } == true)) &&
            (q.notExpiredAt == null || (expiresAt() ?: EventDoc.NO_EXPIRATION) > q.notExpiredAt) &&
            (q.search.isNullOrBlank() || search.matches(q.search.trim()))
    }

    private companion object {
        val NEWEST_FIRST = compareByDescending(EventDoc::createdAt).thenBy(EventDoc::id)
    }
}
