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

import com.vitorpamplona.quartz.nip01Core.metadata.MetadataEvent
import com.vitorpamplona.quartz.nip01Core.relay.filters.Filter
import com.vitorpamplona.quartz.nip19Bech32.toNpub
import com.vitorpamplona.quartz.nip85TrustedAssertions.list.TrustProviderListEvent
import com.vitorpamplona.quartz.nip85TrustedAssertions.list.tags.ProviderTypes
import com.vitorpamplona.quartz.nip85TrustedAssertions.users.ContactCardEvent
import com.vitorpamplona.quartz.utils.Hex
import com.vitorpamplona.sot.store.VespaEventStore
import com.vitorpamplona.sot.vespa.query.EventQuery
import kotlinx.coroutines.runBlocking

/**
 * The trust/relay/profile plumbing kinds. Everything else is "content": the
 * searchable events whose count (and distinct-author count) is the real measure
 * of how big the engine is getting.
 */
private val PLUMBING_KINDS =
    listOf(
        MetadataEvent.KIND, // 0     profiles
        3, // follow lists
        10002, // relay lists
        10086, // indexer configuration
        TrustProviderListEvent.KIND, // 10040 observers
        ContactCardEvent.KIND, // 30382 scores
    )

/** `sot status` — is Vespa up, and what does the store hold? */
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

            suspend fun countOf(kind: Int) = store.count(Filter(kinds = listOf(kind)))
            println("  events:    ${store.count(Filter())}")
            println("  profiles:  ${countOf(0)} (kind 0)")
            println("  relaylists:${countOf(10002)} (kind 10002)")
            println("  observers: ${countOf(10040)} (kind 10040)")
            println("  scores:    ${countOf(30382)} (kind 30382)")

            // "Content" = everything but the plumbing kinds above. Counted over the
            // raw index because a NIP-01 Filter can't express "kind not in (…)".
            val content = EventQuery(notKinds = PLUMBING_KINDS)
            println("  content:   ${stack.vespa.count(content)} (all but plumbing kinds ${PLUMBING_KINDS.joinToString("/")})")
            println("  pubkeys:   ${stack.vespa.countDistinctAuthors(content)} (distinct authors with content)")

            printObservers(store)
        }
    } finally {
        stack.close()
    }
}

/** Each imported kind-10040 (its author's name + npub) and how many 30382 scores its named rank providers have indexed. */
private suspend fun printObservers(store: VespaEventStore) {
    val lists = store.query<TrustProviderListEvent>(Filter(kinds = listOf(TrustProviderListEvent.KIND)))
    if (lists.isEmpty()) return

    val names =
        store
            .query<MetadataEvent>(Filter(kinds = listOf(MetadataEvent.KIND), authors = lists.map { it.pubKey }.distinct()))
            .associate { it.pubKey to it.contactMetaData()?.let { md -> md.name ?: md.displayName }?.takeIf(String::isNotBlank) }

    println()
    println("  observers imported (kind 10040):")
    for (list in lists.sortedBy { names[it.pubKey]?.lowercase() ?: "~" }) {
        val serviceKeys =
            list
                .serviceProviders()
                .filter { it.service == ProviderTypes.rank }
                .map { it.pubkey }
                .distinct()
        val scores = if (serviceKeys.isEmpty()) 0 else store.count(Filter(kinds = listOf(ContactCardEvent.KIND), authors = serviceKeys))
        val name = names[list.pubKey] ?: "(no profile)"
        println("    $name  ${Hex.decode(list.pubKey).toNpub()}  — $scores scores (kind 30382)")
    }
}
