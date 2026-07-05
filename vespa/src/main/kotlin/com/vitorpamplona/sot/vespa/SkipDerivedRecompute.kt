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

import kotlin.coroutines.AbstractCoroutineContextElement
import kotlin.coroutines.CoroutineContext

/**
 * A hint the store sets around an [com.vitorpamplona.sot.vespa.client.EventIndex]
 * mutation whose DERIVED state provably does not change — a decorator that keeps
 * such state (the trust projection's ranking tensors) may skip its recompute for
 * that mutation and do only the base write.
 *
 * The one case today: a kind-30382 supersession whose rank/follower tags are
 * identical to the version it replaces. The event document is still written (the
 * store's supersession and negentropy both need it); only the projection's
 * re-derive is skipped, so the engine is never asked to rewrite a ranking tensor
 * to the value it already holds. See `TrustProjection`.
 *
 * It is a coroutine-context element because the seam it crosses — the
 * `EventIndex` put/remove/putAll/removeAll port — has no per-call parameter,
 * matching how `ObserverContext`/`OriginalFilters` cross `IEventStore`. Base
 * `EventIndex` implementations ignore it; only a recomputing decorator reads it.
 */
class SkipDerivedRecompute : AbstractCoroutineContextElement(SkipDerivedRecompute) {
    companion object Key : CoroutineContext.Key<SkipDerivedRecompute>
}
