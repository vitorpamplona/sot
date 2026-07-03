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
package com.vitorpamplona.sot.store

import com.vitorpamplona.quartz.nip01Core.relay.normalizer.NormalizedRelayUrl
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.RelayUrlNormalizer
import com.vitorpamplona.quartz.nip01Core.store.ObservableEventStore
import com.vitorpamplona.quartz.nip01Core.store.sqlite.DefaultIndexingStrategy
import com.vitorpamplona.quartz.nip01Core.store.sqlite.EventStore
import com.vitorpamplona.sot.config.Config

/*
 * The single place that opens the shared Nostr event store, so every opener agrees on
 * the two things that are easy to get wrong (and were, before this module):
 *
 *  - Indexing strategy: the SQLite full-text index is OFF (search is Vespa's job).
 *    Opening the same DB with a different strategy changes its schema — corruption.
 *  - Relay identity: the store's own relay URL, from Config. NIP-62 Request-to-Vanish
 *    events scoped to a relay only match when the store knows that relay; a null or
 *    foreign relay silently drops those deletions.
 *
 * `dbPath` still varies (the CLI's `--db` overrides Config.eventsDb), so it's a param.
 */

private val NO_FTS = DefaultIndexingStrategy(indexFullTextSearch = false)

/** This store's own relay identity (`RELAY_URL`), for relay-scoped NIP-62 vanish. */
fun relayIdentity(): NormalizedRelayUrl = RelayUrlNormalizer.normalize(Config.relayUrl)

/** Open the shared event store — plain read/write, no change feed. */
fun openEventStore(dbPath: String = Config.eventsDb): EventStore = EventStore(dbPath, relayIdentity(), NO_FTS)

/** Open it wrapped for its `changes` feed (Insert / Delete / Expire) — used to project into Vespa. */
fun openObservableStore(dbPath: String = Config.eventsDb): ObservableEventStore = ObservableEventStore(openEventStore(dbPath))
