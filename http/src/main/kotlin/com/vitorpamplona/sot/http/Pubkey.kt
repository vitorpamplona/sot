package com.vitorpamplona.sot.http

import com.vitorpamplona.quartz.nip05DnsIdentifiers.Nip05Client
import com.vitorpamplona.quartz.nip05DnsIdentifiers.OkHttpNip05Fetcher
import com.vitorpamplona.quartz.nip05DnsIdentifiers.resolveUserHexOrNull
import okhttp3.OkHttpClient

/**
 * Resolve a pubkey identifier to a 64-char hex pubkey, or null if it isn't one.
 * Accepts hex, a NIP-19 `npub`/`nprofile`, or a NIP-05 `name@domain` (looked up
 * over HTTPS) — the same forms the CLI accepts, via Quartz's [resolveUserHexOrNull].
 * Used for the `observer` parameter and to detect when the query text is itself a
 * pubkey (a direct doc lookup instead of a text search).
 */
private val nip05 = Nip05Client(OkHttpNip05Fetcher { OkHttpClient() })

internal suspend fun resolvePubkey(text: String): String? = resolveUserHexOrNull(text, nip05)
