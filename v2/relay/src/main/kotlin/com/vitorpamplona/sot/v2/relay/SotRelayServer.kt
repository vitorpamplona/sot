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
package com.vitorpamplona.sot.v2.relay

import com.vitorpamplona.quartz.nip01Core.core.Event
import com.vitorpamplona.quartz.nip01Core.relay.commands.toClient.CountResult
import com.vitorpamplona.quartz.nip01Core.relay.filters.Filter
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.NormalizedRelayUrl
import com.vitorpamplona.quartz.nip01Core.relay.server.RelayServerBase
import com.vitorpamplona.quartz.nip01Core.relay.server.RelayServerListener
import com.vitorpamplona.quartz.nip01Core.relay.server.backend.IngestQueue
import com.vitorpamplona.quartz.nip01Core.relay.server.backend.LiveEventStore
import com.vitorpamplona.quartz.nip01Core.relay.server.backend.RequestContext
import com.vitorpamplona.quartz.nip01Core.relay.server.backend.SessionBackend
import com.vitorpamplona.quartz.nip01Core.relay.server.policies.OptionalAuthPolicy
import com.vitorpamplona.quartz.nip01Core.relay.server.policies.PolicyStack
import com.vitorpamplona.quartz.nip01Core.relay.server.policies.RelayLimits
import com.vitorpamplona.quartz.nip01Core.relay.server.policies.VerifyPolicy
import com.vitorpamplona.quartz.nip01Core.store.IEventStore
import com.vitorpamplona.quartz.nip01Core.store.IdAndTime
import com.vitorpamplona.quartz.nip77Negentropy.NegentropySettings
import com.vitorpamplona.sot.v2.store.ObserverContext
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.withContext
import kotlin.coroutines.CoroutineContext

/**
 * The v2 Nostr relay: Quartz's protocol engine over the Vespa-backed store.
 * One store, full NIP-01 filters, NIP-50 search, live subscriptions, EVENT
 * publishes, NIP-45 COUNT, and server-side NIP-77 negentropy — all inherited
 * from [RelayServerBase] + [LiveEventStore]; the store supplies storage
 * semantics and `snapshotIdsForNegentropy`.
 *
 * Per connection the policy stack is [VerifyPolicy] (published events must
 * carry a valid id + signature — the store itself never verifies) then
 * [OptionalAuthPolicy] (a NIP-42 challenge on connect, nothing gated on it).
 * What auth DOES change is ranking: [ObserverRoutingBackend] resolves the
 * observer — the authenticated pubkey, else [defaultObserver] — for every
 * REQ/COUNT, so a search runs through the caller's own web of trust.
 *
 * [close] shuts the connections and the ingest writer down but NOT the store:
 * the composition root owns it (the sync service shares it).
 */
class SotRelayServer(
    store: IEventStore,
    defaultObserver: String?,
    relayUrl: NormalizedRelayUrl,
    parentContext: CoroutineContext = SupervisorJob(),
    listener: RelayServerListener = RelayServerListener.None,
    limits: RelayLimits? = null,
    // The product loop's seam: fires with each authenticated pubkey observed
    // on a ranked read, so the composition root can enroll NIP-42 logins as
    // sync observers (SyncService.enroll dedups).
    onObserver: ((String) -> Unit)? = null,
) : RelayServerBase(
        policyBuilder = { PolicyStack(VerifyPolicy, OptionalAuthPolicy(relayUrl)) },
        parentContext = parentContext,
        negentropySettings = NegentropySettings.Default,
        listener = listener,
        limits = limits,
    ) {
    private val ingest = IngestQueue(store = store, parentContext = parentContext)

    override val backend: SessionBackend = ObserverRoutingBackend(LiveEventStore(store, ingest), defaultObserver, onObserver)

    override fun close() {
        closeConnections()
        ingest.close()
        scope.cancel()
    }
}

/**
 * Delegates everything to [LiveEventStore], wrapping each read in an
 * [ObserverContext] carrying the session's ranking observer: the first
 * NIP-42-authenticated pubkey, else the operator's default. The store reads
 * the element back out when it builds the Vespa query — the seam that lets a
 * per-connection fact cross the caller-agnostic `IEventStore` interface.
 */
internal class ObserverRoutingBackend(
    private val inner: LiveEventStore,
    private val defaultObserver: String?,
    private val onObserver: ((String) -> Unit)? = null,
) : SessionBackend {
    override suspend fun query(
        ctx: RequestContext,
        filters: List<Filter>,
        onEach: (Event) -> Unit,
        onEose: () -> Unit,
    ) = ranked(ctx) { inner.query(ctx, filters, onEach, onEose) }

    override suspend fun count(
        ctx: RequestContext,
        filters: List<Filter>,
    ): Int = ranked(ctx) { inner.count(ctx, filters) }

    override suspend fun countResult(
        ctx: RequestContext,
        filters: List<Filter>,
    ): CountResult = ranked(ctx) { inner.countResult(ctx, filters) }

    override suspend fun submit(
        event: Event,
        onComplete: (IEventStore.InsertOutcome) -> Unit,
    ) = inner.submit(event, onComplete)

    override suspend fun snapshotIdsForNegentropy(
        filters: List<Filter>,
        maxEntries: Int?,
    ): List<IdAndTime> = inner.snapshotIdsForNegentropy(filters, maxEntries)

    private suspend fun <T> ranked(
        ctx: RequestContext,
        block: suspend () -> T,
    ): T {
        val authenticated = ctx.authenticatedUsers.firstOrNull()
        authenticated?.let { onObserver?.invoke(it) }
        val observer = authenticated ?: defaultObserver
        return if (observer == null) block() else withContext(ObserverContext(observer)) { block() }
    }
}
