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

import com.vitorpamplona.quartz.nip01Core.relay.normalizer.NormalizedRelayUrl
import java.net.URI

/**
 * Relay-URL housekeeping for the discovery crawl.
 *
 * The write relays harvested from 10002s are full of per-user path variants of
 * the SAME server — `filter.nostr.wine/npub1…?broadcast=true`, `relay.band/npub…`,
 * `relay.band/trusted` — plus the occasional user typo. Sweeping each as a
 * distinct relay would hammer one server dozens of times and inflate the fan-out
 * from the ~1k live relays that actually exist to many thousands.
 */
internal object RelayUrls {
    /**
     * EVIDENCE-GUIDED path collapse: each URL is mapped to the SHORTEST URL in
     * [relays] that shares its host and whose path is a prefix of it — down to
     * the bare host. Crucially, the target must ALSO be present in [relays]: we
     * never invent a shorter relay by stripping a path, because a bare host we
     * never saw advertised might not be a working relay (and the path might be a
     * user's own mistake). We only merge onto a shorter form the network itself
     * vouches for. The returned set is the deduplicated representatives.
     */
    fun collapse(relays: Collection<NormalizedRelayUrl>): Set<NormalizedRelayUrl> {
        val reps = LinkedHashSet<NormalizedRelayUrl>()
        for ((_, group) in relays.groupBy { authority(it.url) }) {
            val byPathLen = group.sortedBy { path(it.url).length }
            for (u in group) {
                val here = path(u.url)
                reps += byPathLen.firstOrNull { isPathPrefix(path(it.url), here) } ?: u
            }
        }
        return reps
    }

    /** `scheme://host[:port]` — everything a same-server test needs; the raw URL is its own bucket if it won't parse. */
    private fun authority(url: String): String = runCatching { URI(url).let { "${it.scheme}://${it.authority}".lowercase() } }.getOrDefault(url)

    /** The path + query after the authority, trailing slash trimmed so `/` and `` compare equal. */
    private fun path(url: String): String =
        runCatching {
            URI(url).let { (it.rawPath ?: "").trimEnd('/') + (it.rawQuery?.let { q -> "?$q" } ?: "") }
        }.getOrDefault("")

    /** [short] is a path-prefix of [long] at a segment boundary (empty prefix = the bare host, a prefix of everything). */
    private fun isPathPrefix(
        short: String,
        long: String,
    ): Boolean = short.isEmpty() || short == long || long.startsWith("$short/")
}
