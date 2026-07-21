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

import com.vitorpamplona.quartz.nip01Core.core.HexKey
import kotlinx.serialization.Serializable
import kotlinx.serialization.builtins.MapSerializer
import kotlinx.serialization.builtins.serializer
import kotlinx.serialization.json.Json
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.StandardCopyOption
import java.util.concurrent.ConcurrentHashMap

/**
 * A [CrawlIndex] backed by a local JSON file: the whole map lives in memory and
 * is rewritten atomically after each batch of stamps. Crawl state is sync
 * bookkeeping (not events, not ranking), so it belongs next to the sync cursors
 * on disk rather than in the event store.
 *
 * The full-file rewrite is fine at roster scale (a batch of stamps per pass); a
 * far larger roster would want a real embedded store instead.
 */
class FileCrawlIndex(
    private val path: Path,
) : CrawlIndex {
    @Serializable
    private data class Row(
        val contentSyncedAt: Long = 0,
        val outboxCheckedAt: Long = 0,
    )

    private val serializer = MapSerializer(String.serializer(), Row.serializer())
    private val json = Json { ignoreUnknownKeys = true }
    private val docs = ConcurrentHashMap<String, CrawlDoc>()
    private val writeLock = Any()

    init {
        runCatching {
            if (Files.exists(path)) {
                json.decodeFromString(serializer, Files.readString(path)).forEach { (pk, r) ->
                    docs[pk] = CrawlDoc(pk, r.contentSyncedAt, r.outboxCheckedAt)
                }
            }
        }
    }

    override suspend fun get(pubkey: HexKey): CrawlDoc? = docs[pubkey]

    override suspend fun markSynced(
        authors: Collection<HexKey>,
        atSecs: Long,
    ) = stamp(authors) { it.copy(contentSyncedAt = atSecs) }

    override suspend fun markOutboxChecked(
        authors: Collection<HexKey>,
        atSecs: Long,
    ) = stamp(authors) { it.copy(outboxCheckedAt = atSecs) }

    private inline fun stamp(
        authors: Collection<HexKey>,
        crossinline update: (CrawlDoc) -> CrawlDoc,
    ) {
        synchronized(writeLock) {
            for (pk in authors) docs.compute(pk) { _, cur -> update(cur ?: CrawlDoc(pk)) }
            persist()
        }
    }

    override suspend fun syncedSince(cutoffSecs: Long): Set<HexKey> = docs.values.filter { it.contentSyncedAt >= cutoffSecs }.mapTo(HashSet()) { it.pubkey }

    override suspend fun syncedCount(): Int = docs.values.count { it.contentSyncedAt > 0 }

    override suspend fun dueForRefresh(
        cutoffSecs: Long,
        limit: Int,
    ): List<HexKey> =
        if (limit <= 0) {
            emptyList()
        } else {
            docs.values
                .filter { it.contentSyncedAt in 1..cutoffSecs }
                .sortedBy { it.contentSyncedAt }
                .take(limit)
                .map { it.pubkey }
        }

    override suspend fun outboxCheckedSet(): Set<HexKey> = docs.values.filter { it.outboxCheckedAt > 0 }.mapTo(HashSet()) { it.pubkey }

    /** Atomic full-file rewrite: temp sibling + move, so a crash never leaves a half-written file. */
    private fun persist() {
        val snapshot = docs.mapValues { Row(it.value.contentSyncedAt, it.value.outboxCheckedAt) }
        val dir = path.toAbsolutePath().parent
        if (dir != null) Files.createDirectories(dir)
        val tmp = Files.createTempFile(dir, ".crawl", ".tmp")
        Files.writeString(tmp, json.encodeToString(serializer, snapshot))
        Files.move(tmp, path, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE)
    }

    override fun close() {}
}
