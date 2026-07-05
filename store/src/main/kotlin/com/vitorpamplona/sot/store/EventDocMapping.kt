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

import com.vitorpamplona.quartz.nip01Core.core.Address
import com.vitorpamplona.quartz.nip01Core.core.Event
import com.vitorpamplona.quartz.nip01Core.core.isAddressable
import com.vitorpamplona.quartz.nip01Core.core.isReplaceable
import com.vitorpamplona.quartz.nip01Core.tags.dTag.dTag
import com.vitorpamplona.quartz.nip59Giftwrap.wraps.GiftWrapEvent
import com.vitorpamplona.quartz.nip85TrustedAssertions.users.ContactCardEvent
import com.vitorpamplona.sot.vespa.doc.EventDoc

/*
 * Event <-> EventDoc and derived-field helpers. Covers the exact stored form
 * plus the owner (gift-wrap recipient or author), the NIP-01
 * replaceable/addressable address, and the doc-side d-tag reader. Pure, with no
 * store state.
 */

/**
 * The event's exact stored form plus two derived fields: [EventDoc.owner] (the
 * gift-wrap recipient or the author) and [EventDoc.search] (the kind-specific
 * decomposition from [SearchExtractors]).
 */
internal fun Event.toDoc(): EventDoc =
    EventDoc(
        id = id,
        pubkey = pubKey,
        createdAt = createdAt,
        kind = kind,
        tags = tags.map { it.toList() },
        content = content,
        sig = sig,
        owner = owner(),
        search = SearchExtractors.extract(this),
    )

/** The pubkey Nostr semantics key off: the gift-wrap recipient, else the author. */
internal fun Event.owner(): String = (this as? GiftWrapEvent)?.recipientPubKey() ?: pubKey

/**
 * The NIP-01 address for replaceable/addressable kinds; null for regular
 * events. Replaceables use the fixed empty d-tag regardless of stray d tags,
 * matching Quartz's BaseReplaceableEvent.FIXED_D_TAG.
 */
internal fun Event.addressOrNull(): String? =
    when {
        kind.isReplaceable() -> Address.assemble(kind, pubKey)
        kind.isAddressable() -> Address.assemble(kind, pubKey, tags.dTag())
        else -> null
    }

/**
 * The only inputs the trust projection reads off a kind-30382: its rank and
 * follower-count tags. Two versions with the same pair derive the SAME ranking
 * tensor cell, so replacing one with the other is a no-op for ranking.
 */
private fun ContactCardEvent.trustCell() = rank() to followerCount()

/**
 * True when storing [incoming] over the versions it [superseded] cannot move any
 * ranking tensor: it is a kind-30382 replacing kind-30382s that ALL carry the
 * same rank + follower tags. A first-seen address (nothing superseded) is never
 * neutral — its cell must still be created. Lets the store write the event but
 * skip the projection's re-derive via [com.vitorpamplona.sot.vespa.SkipDerivedRecompute].
 */
internal fun isTrustNeutralSupersession(
    incoming: Event,
    superseded: List<EventDoc>,
): Boolean {
    if (incoming !is ContactCardEvent || superseded.isEmpty()) return false
    val cell = incoming.trustCell()
    return superseded.all { doc ->
        val old = Event.fromJsonOrNull(doc.toEventJson()) as? ContactCardEvent
        old != null && old.trustCell() == cell
    }
}
