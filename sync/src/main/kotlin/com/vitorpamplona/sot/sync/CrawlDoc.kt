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

/**
 * One pubkey's CRAWL bookkeeping — how completely the sync has indexed this
 * author's content. This is sync state, not event data or ranking state, so it
 * lives OUTSIDE the event store (a local file; see [FileCrawlIndex]).
 *
 * [contentSyncedAt] is the epoch-second of the last CLEAN outbox reconcile
 * (`0` = never); [outboxCheckedAt] the last time the author's 10002 was resolved
 * (`0` = never looked). Both default to `0` so an absent entry reads as "brand
 * new, nothing done yet".
 */
data class CrawlDoc(
    val pubkey: HexKey,
    val contentSyncedAt: Long = 0,
    val outboxCheckedAt: Long = 0,
)

/**
 * The port for the crawl bookkeeping. All writes are create-if-missing single-
 * field stamps, so the two writers ([markSynced], [markOutboxChecked]) never
 * clobber each other's field. [FileCrawlIndex] is the persistent implementation;
 * [InMemoryCrawlIndex] (test sources) is the reference the ledger tests assert
 * against.
 */
interface CrawlIndex : AutoCloseable {
    suspend fun get(pubkey: HexKey): CrawlDoc?

    /** Stamp `content_synced_at = [atSecs]` for [authors] (a clean outbox reconcile). */
    suspend fun markSynced(
        authors: Collection<HexKey>,
        atSecs: Long,
    )

    /** Stamp `outbox_checked_at = [atSecs]` for [authors] (their 10002 was resolved, found or not). */
    suspend fun markOutboxChecked(
        authors: Collection<HexKey>,
        atSecs: Long,
    )

    /**
     * The pubkeys whose `content_synced_at >= [cutoffSecs]` — the set the current
     * pass may SKIP because they were reconciled recently enough. Loaded once at
     * pass start (the roster-scale "already done" set, TTL-bounded so stale
     * syncs re-enter the backlog).
     */
    suspend fun syncedSince(cutoffSecs: Long): Set<HexKey>

    /** How many authors have a clean content sync (`content_synced_at > 0`). */
    suspend fun syncedCount(): Int

    /**
     * Up to [limit] already-synced authors whose last clean sync is OLDEST (and at
     * or before [cutoffSecs]) — the stalest first, so a bounded refresh slice
     * re-pulls the most out-of-date content each pass. Excludes never-synced
     * authors (`content_synced_at > 0`).
     */
    suspend fun dueForRefresh(
        cutoffSecs: Long,
        limit: Int,
    ): List<HexKey>

    /**
     * The pubkeys whose outbox we RESOLVED at least once (`outbox_checked_at > 0`).
     * Lets the coverage report tell "we looked and found no 10002" (no-outbox)
     * from "we haven't looked yet" (unresolved) during an in-progress load.
     */
    suspend fun outboxCheckedSet(): Set<HexKey>
}
