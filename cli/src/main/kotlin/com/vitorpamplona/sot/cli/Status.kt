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

import com.vitorpamplona.quartz.nip01Core.core.Event
import com.vitorpamplona.quartz.nip01Core.metadata.MetadataEvent
import com.vitorpamplona.quartz.nip01Core.relay.filters.Filter
import com.vitorpamplona.quartz.nip09Deletions.DeletionEvent
import com.vitorpamplona.quartz.nip19Bech32.toNpub
import com.vitorpamplona.quartz.nip51Lists.followList.FollowListEvent
import com.vitorpamplona.quartz.nip51Lists.relayLists.IndexerRelayListEvent
import com.vitorpamplona.quartz.nip62RequestToVanish.RequestToVanishEvent
import com.vitorpamplona.quartz.nip65RelayList.AdvertisedRelayListEvent
import com.vitorpamplona.quartz.nip85TrustedAssertions.list.TrustProviderListEvent
import com.vitorpamplona.quartz.nip85TrustedAssertions.list.tags.ProviderTypes
import com.vitorpamplona.quartz.nip85TrustedAssertions.users.ContactCardEvent
import com.vitorpamplona.quartz.utils.Hex
import com.vitorpamplona.sot.store.ObserverContext
import com.vitorpamplona.sot.store.VespaEventStore
import com.vitorpamplona.sot.sync.Identity
import com.vitorpamplona.sot.sync.SyncState
import com.vitorpamplona.sot.vespa.client.VespaEventIndex
import com.vitorpamplona.sot.vespa.query.EventQuery
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withContext

/**
 * The trust/relay/profile plumbing kinds. Everything else is "content": the
 * searchable events whose count (and distinct-author count) is the real measure
 * of how big the engine is getting.
 */
private val PLUMBING_KINDS =
    listOf(
        MetadataEvent.KIND, // 0     profiles
        FollowListEvent.KIND, // 3     follow lists
        AdvertisedRelayListEvent.KIND, // 10002 relay lists
        IndexerRelayListEvent.KIND, // 10086 indexer configuration
        TrustProviderListEvent.KIND, // 10040 observers
        ContactCardEvent.KIND, // 30382 scores
    )

/** The search term the health probe runs — a common word almost any real corpus should surface. */
private const val SEARCH_PROBE = "nostr"

/** How many kinds the by-kind histogram prints before collapsing the tail into "other". */
private const val TOP_KINDS = 12

/** Human labels for the kinds worth naming; everything else prints as "kind N". */
private val KIND_LABELS =
    mapOf(
        0 to "profile",
        1 to "note",
        3 to "follows",
        5 to "deletion",
        6 to "repost",
        7 to "reaction",
        1063 to "file",
        1111 to "comment",
        1222 to "voice",
        9735 to "zap",
        10002 to "relay-list",
        10040 to "observer",
        10086 to "indexer",
        30023 to "article",
        30311 to "live-event",
        30402 to "classified",
        34550 to "community",
    )

/** `sot status` — is Vespa up, and how healthy is what it holds? */
internal fun status(args: List<String>) {
    val vespaUrl = flag(args, "--vespa", Config.vespaUrl)
    val vespaUp = ping("$vespaUrl/ApplicationStatus")
    if (vespaUp) ok("vespa: up at $vespaUrl") else err("vespa: NOT reachable at $vespaUrl")

    val serverUp = ping(Config.serverUrl, accept = "application/nostr+json")
    if (serverUp) ok("server: up at ${Config.serverUrl}") else warn("server: not reachable at ${Config.serverUrl} (is `sot serve` running?)")

    if (!vespaUp) return
    val stack = openStack()
    try {
        runBlocking {
            val store = stack.store
            val index = stack.vespa
            val house = housePubkey()

            corpus(store, index)
            byKind(index)
            trustGraph(store, house)
            searchHealth(store, house)
            freshness()
            hygiene(index)
            config(store, house)
        }
    } finally {
        stack.close()
    }
}

/** Overall size: raw event count, content vs plumbing, and how many distinct authors carry that content. */
private suspend fun corpus(
    store: VespaEventStore,
    index: VespaEventIndex,
) {
    val content = EventQuery(notKinds = PLUMBING_KINDS)
    val events = store.count(Filter())
    val contentEvents = index.count(content)
    val pubkeys = index.countDistinctAuthors(content)

    section("corpus")
    row("events", num(events))
    row("content", "${num(contentEvents)}  (all but plumbing ${PLUMBING_KINDS.sorted().joinToString("/")})")
    row("pubkeys", "${num(pubkeys)}  (distinct authors with content)")
    if (pubkeys > 0) row("avg/pubkey", "%.1f content events".format(contentEvents.toDouble() / pubkeys))
}

/** The corpus shape: the biggest kinds by volume, tail collapsed. */
private suspend fun byKind(index: VespaEventIndex) {
    val hist = index.countByKind(EventQuery()).entries.sortedByDescending { it.value }
    if (hist.isEmpty()) return

    section("by kind")
    val shown = hist.take(TOP_KINDS)
    for ((kind, count) in shown) row(kindLabel(kind), num(count))
    val tail = hist.drop(TOP_KINDS)
    if (tail.isNotEmpty()) row("other (${tail.size} kinds)", num(tail.sumOf { it.value }))
}

/**
 * The trust graph that ranking runs on: observers, whether their outbox (10002)
 * is known, the score providers behind them, and orphan scores (30382s no stored
 * 10040 attributes). Then each imported observer with its name, npub, and score count.
 */
private suspend fun trustGraph(
    store: VespaEventStore,
    house: String?,
) {
    val lists = store.query<TrustProviderListEvent>(Filter(kinds = listOf(TrustProviderListEvent.KIND)))
    val observers = lists.map { it.pubKey }.distinct()
    val serviceKeys = lists.flatMap { it.rankServiceKeys() }.distinct()

    val totalScores = store.count(Filter(kinds = listOf(ContactCardEvent.KIND)))
    val attributed = if (serviceKeys.isEmpty()) 0 else store.count(Filter(kinds = listOf(ContactCardEvent.KIND), authors = serviceKeys))
    val withOutbox = if (observers.isEmpty()) 0 else store.count(Filter(kinds = listOf(AdvertisedRelayListEvent.KIND), authors = observers))

    section("trust graph")
    row("observers", "${num(observers.size)}  (kind 10040)")
    row("with outbox", "${num(withOutbox)}/${num(observers.size)}  (have a 10002 relay list)")
    row("providers", "${num(serviceKeys.size)}  (distinct 30382 signer keys named)")
    row("scores", "${num(totalScores)}  (kind 30382)")
    row("orphan scores", "${num(totalScores - attributed)}  (30382s no stored 10040 attributes)")
    // Users whose content we've pulled to completion (their contentUnit finished
    // cleanly), from the persisted sync state — the "fully indexed" count.
    val contentDone = runCatching { SyncState.load(Config.syncStatePath).contentDoneCount() }.getOrDefault(0)
    row("content complete", "${num(contentDone)}  (users we've fully pulled content for)")

    if (lists.isEmpty()) return
    val names =
        store
            .query<MetadataEvent>(Filter(kinds = listOf(MetadataEvent.KIND), authors = observers))
            .associate { it.pubKey to it.contactMetaData()?.let { md -> md.name ?: md.displayName }?.takeIf(String::isNotBlank) }

    println()
    println("    imported observers (name  npub  scores):")
    for (list in lists.sortedBy { names[it.pubKey]?.lowercase() ?: "~" }) {
        val keys = list.rankServiceKeys()
        val scores = if (keys.isEmpty()) 0 else store.count(Filter(kinds = listOf(ContactCardEvent.KIND), authors = keys))
        val name = names[list.pubKey] ?: "(no profile)"
        val marker = if (list.pubKey == house) "  ← house" else ""
        println("      $name  ${Hex.decode(list.pubKey).toNpub()}  — ${num(scores)} scores$marker")
    }
}

/**
 * The one question a corpus count can't answer: does a ranked search actually
 * return anything? Ranked search is trust-gated, so it probes as the house
 * observer and contrasts gated hits with raw (ungated) text recall.
 */
private suspend fun searchHealth(
    store: VespaEventStore,
    house: String?,
) {
    section("search health")
    if (house == null) {
        warn("  no house observer (HOUSE_NPUB unset) — unauthenticated ranked search returns nothing until a user AUTHs")
        return
    }
    runCatching {
        val ranked = withContext(ObserverContext(house)) { store.query<Event>(Filter(kinds = listOf(1), search = SEARCH_PROBE, limit = 500)).size }
        val raw = store.count(Filter(kinds = listOf(1), search = SEARCH_PROBE))
        val hits = if (ranked >= 500) "500+" else num(ranked)
        row("probe \"$SEARCH_PROBE\"", "$hits ranked hits as house  (of ${num(raw)} raw text matches)")
        if (ranked == 0) warn("  ranked search returns 0 — the house trust chain has no scores yet (sync incomplete, or no providers)")
    }.onFailure { row("probe \"$SEARCH_PROBE\"", "failed: ${it.message}") }
}

/** Sync freshness + throughput, read from the persisted SyncState (the only cross-process source). */
private fun freshness() {
    section("sync")
    val path = Config.syncStatePath
    val state = runCatching { SyncState.load(path) }.getOrNull()
    if (state == null || (state.relays.isEmpty() && state.lastPass == null)) {
        warn("  no sync state at $path yet — run `sot index` or `sot serve` (SYNC_INTERVAL>0)")
        return
    }
    val now = nowSecs()
    state.lastPass?.let { p ->
        row("last pass", "${ago(now, p.endedAtSecs)}  (took ${p.durationSecs}s, received ${num(p.received)}, inserted ${num(p.inserted)})")
    }
    val cursors = state.relays.values.flatMap { it.lastSyncedAt.values }
    if (cursors.isNotEmpty()) {
        row("newest cursor", ago(now, cursors.max()))
        row("oldest cursor", ago(now, cursors.min()))
    }
    val negentropy = state.relays.values.count { it.negentropyCapable == true }
    row("relays tracked", "${num(state.relays.size)}  ($negentropy negentropy-capable)")
}

/** NIP-40/09/62 hygiene backlog: events past expiry not yet swept, deletions, and vanish requests. */
private suspend fun hygiene(index: VespaEventIndex) {
    section("hygiene")
    val expired = index.count(EventQuery(expiresBefore = nowSecs()))
    row("expired unswept", "${num(expired)}  (NIP-40, past expiry, awaiting sweep)")
    row("deletions", "${num(index.count(EventQuery(kinds = listOf(DeletionEvent.KIND))))}  (kind 5)")
    row("vanish requests", "${num(index.count(EventQuery(kinds = listOf(RequestToVanishEvent.KIND))))}  (kind 62)")
}

/** Echo the resolved identity/relay config so an operator can confirm it's pointed where they think. */
private suspend fun config(
    store: VespaEventStore,
    house: String?,
) {
    section("config")
    row("house", if (house != null) "${Hex.decode(house).toNpub()}  (resolved)" else "${Config.houseNpub.ifBlank { "(unset)" }}  — NOT resolved")
    row("relay id", "${Config.serverName}  ${Config.relayUrl}")
    row("seed relays", Config.seedRelays.joinToString(", ").ifBlank { "(none)" })

    val serverHex =
        Config.serverNsec
            .trim()
            .takeIf { it.isNotEmpty() }
            ?.let { runCatching { Identity.signerFromSecret(it)?.pubKey }.getOrNull() }
    val indexRelays =
        serverHex?.let { hex ->
            store
                .query<IndexerRelayListEvent>(Filter(kinds = listOf(IndexerRelayListEvent.KIND), authors = listOf(hex)))
                .maxByOrNull { it.createdAt }
                ?.publicRelays()
                ?.map { it.url }
        }
    row("index relays", indexRelays?.joinToString(", ")?.ifBlank { null } ?: "(none stored — the newest kind-10086 is the config)")
}

/** The observer service keys a 10040 names for ranking (its `30382:rank` provider entries). */
private fun TrustProviderListEvent.rankServiceKeys(): List<String> = serviceProviders().filter { it.service == ProviderTypes.rank }.map { it.pubkey }.distinct()

private fun nowSecs(): Long = System.currentTimeMillis() / 1000

private fun kindLabel(kind: Int): String = KIND_LABELS[kind]?.let { "$it ($kind)" } ?: "kind $kind"

private fun num(v: Int): String = "%,d".format(v)

private fun num(v: Long): String = "%,d".format(v)

/** A coarse "Xd Yh / Xh Ym / Xm Ys / Xs ago" for a past epoch-seconds timestamp. */
private fun ago(
    nowSecs: Long,
    thenSecs: Long,
): String {
    val d = (nowSecs - thenSecs).coerceAtLeast(0)
    val human =
        when {
            d < 60 -> "${d}s"
            d < 3600 -> "${d / 60}m ${d % 60}s"
            d < 86400 -> "${d / 3600}h ${(d % 3600) / 60}m"
            else -> "${d / 86400}d ${(d % 86400) / 3600}h"
        }
    return "$human ago"
}

private fun section(title: String) {
    println()
    println("  $title:")
}

private fun row(
    label: String,
    value: String,
) = println("    ${label.padEnd(16)}$value")
