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
import com.vitorpamplona.sot.vespa.client.VespaEventIndex
import com.vitorpamplona.sot.vespa.client.VespaProfileIndex
import java.net.URI

/**
 * The library front door. [open] wires a ready [VespaStore] over a running Vespa
 * in one call — the whole VespaEventStore(TrustProjection(VespaEventIndex,
 * VespaProfileIndex)) stack, plus (by default) a first-run schema deploy.
 *
 * Vespa itself is a prerequisite, like a database: point [open] at one that is
 * already running.
 */
object VespaEventStores {
    /**
     * Open a store over the Vespa at [url] (its query/document endpoint).
     *
     * With [autoDeploy] (the default), the bundled schema is deployed to
     * [configUrl] the first time — a fresh Vespa becomes queryable with no
     * separate deploy step. Turn it off when an operator owns schema deployment
     * out of band; then a missing schema surfaces as a query error, not a silent
     * one. [configUrl] defaults to the config server's conventional :19071 on the
     * same host as [url].
     *
     * [relay] is the store's own relay url (NIP-62 vanish scope / NIP-42 identity)
     * when it sits behind a relay; leave it null for a bare store.
     */
    fun open(
        url: String = "http://localhost:8080",
        relay: NormalizedRelayUrl? = null,
        autoDeploy: Boolean = true,
        configUrl: String = deriveConfigUrl(url),
    ): VespaStore {
        if (autoDeploy) SchemaDeployer(configUrl).deployIfAbsent(url)
        val events = VespaEventIndex(url)
        val store = VespaEventStore(TrustProjection(events, VespaProfileIndex(url)), relay = relay)
        return VespaStore(store, events)
    }

    /** The config server sits on :19071 by convention, on the same host as the :8080 query endpoint. */
    internal fun deriveConfigUrl(queryUrl: String): String {
        val u = URI.create(queryUrl)
        return URI(u.scheme, null, u.host, 19071, null, null, null).toString()
    }
}
