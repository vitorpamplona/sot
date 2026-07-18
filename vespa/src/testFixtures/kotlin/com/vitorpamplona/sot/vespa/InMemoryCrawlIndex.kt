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
import com.vitorpamplona.sot.vespa.doc.CrawlDoc
import com.vitorpamplona.sot.vespa.doc.CrawlIndex
import java.util.concurrent.ConcurrentHashMap

/**
 * The in-memory reference [CrawlIndex] — what the convergence + coverage tests
 * assert against. `HexKey` is a typealias for `String`; this fixture source set
 * has no quartz dependency, so it spells the key type `String` directly.
 */
class InMemoryCrawlIndex : CrawlIndex {
    val docs = ConcurrentHashMap<String, CrawlDoc>()

    override suspend fun get(pubkey: String): CrawlDoc? = docs[pubkey]

    override suspend fun markSynced(
        authors: Collection<String>,
        atSecs: Long,
    ) = stamp(authors) { it.copy(contentSyncedAt = atSecs) }

    override suspend fun markOutboxChecked(
        authors: Collection<String>,
        atSecs: Long,
    ) = stamp(authors) { it.copy(outboxCheckedAt = atSecs) }

    private inline fun stamp(
        authors: Collection<String>,
        crossinline update: (CrawlDoc) -> CrawlDoc,
    ) {
        for (pk in authors) docs.compute(pk) { _, cur -> update(cur ?: CrawlDoc(pk)) }
    }

    override suspend fun syncedSince(cutoffSecs: Long): Set<String> = docs.values.filter { it.contentSyncedAt >= cutoffSecs }.mapTo(HashSet()) { it.pubkey }

    override suspend fun syncedCount(): Int = docs.values.count { it.contentSyncedAt > 0 }

    override suspend fun dueForRefresh(
        cutoffSecs: Long,
        limit: Int,
    ): List<String> =
        docs.values
            .filter { it.contentSyncedAt in 1..cutoffSecs }
            .sortedBy { it.contentSyncedAt }
            .take(limit)
            .map { it.pubkey }

    override suspend fun outboxCheckedSet(): Set<String> = docs.values.filter { it.outboxCheckedAt > 0 }.mapTo(HashSet()) { it.pubkey }

    override fun close() {}
}
