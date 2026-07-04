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
import com.vitorpamplona.sot.vespa.EventDoc

/*
 * Event <-> EventDoc and derived-field helpers: the exact stored form plus the
 * owner (gift-wrap recipient or author), the NIP-01 replaceable/addressable
 * address, and the doc-side d-tag reader. Pure, no store state.
 */

/**
 * The event's exact stored form plus the two derived fields: [EventDoc.owner]
 * (gift-wrap recipient or author) and [EventDoc.search] (the kind-specific
 * decomposition from [SearchExtractors] — SQLite's FTS row, tiered).
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

/** The pubkey Nostr semantics key off (SQLite's pubkey_owner_hash): the gift-wrap recipient, else the author. */
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

/** The doc-side twin of Quartz's TagArray.dTag() — EventDoc tags are plain lists, not TagArrays. */
internal fun dTagOf(tags: List<List<String>>): String = tags.firstOrNull { it.size >= 2 && it[0] == "d" }?.get(1) ?: ""
