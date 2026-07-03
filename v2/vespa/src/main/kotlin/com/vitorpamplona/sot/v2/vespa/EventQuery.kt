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
package com.vitorpamplona.sot.v2.vespa

/**
 * A NIP-01 filter as plain values — what the relay module maps a REQ's Filter
 * into (this module stays Nostr-library-agnostic). Empty lists mean "no
 * constraint"; a filter that arrived with a present-but-empty list matches
 * nothing and must be handled by the caller before building.
 */
data class EventQuery(
    /** 64-hex event ids. */
    val ids: List<String> = emptyList(),
    val kinds: List<Int> = emptyList(),
    /** 64-hex pubkeys. */
    val authors: List<String> = emptyList(),
    /** 64-hex owner pubkeys (the semantic owner: gift-wrap recipient or author). */
    val owners: List<String> = emptyList(),
    /** Single-letter tag name -> values. OR within a name, AND across names. */
    val tags: Map<String, List<String>> = emptyMap(),
    /** Like [tags], but EVERY value must be present (Quartz's `tagsAll`). */
    val tagsAll: Map<String, List<String>> = emptyMap(),
    val since: Long? = null,
    val until: Long? = null,
    /** Match docs whose NIP-40 expiration is strictly before this — the expiry sweep. */
    val expiresBefore: Long? = null,
    /** Exclude docs already expired at this time (NIP-40: never serve expired events). */
    val notExpiredAt: Long? = null,
    val limit: Int? = null,
    /** NIP-50 search term; null/blank = plain recall ordered by recency. */
    val search: String? = null,
    /**
     * RANKING context, never recall: the 64-hex pubkey whose web-of-trust
     * weighs search hits (the NIP-42-authenticated user, or the operator's
     * default). Only emitted alongside a search term, as the `user_q` ranking
     * feature.
     */
    val observer: String? = null,
)

/** A ready-to-send Vespa query: the YQL, its query parameters, and the rank profile. */
data class VespaQuery(
    val yql: String,
    val params: Map<String, String>,
    val ranking: String,
)

/**
 * [EventQuery] -> YQL over the `event` schema. Returns null when the query
 * provably matches nothing (an id/author constraint with no valid 64-hex
 * entries, a non-single-letter tag name, limit 0) — the caller answers with an
 * empty result (EOSE) instead of asking Vespa.
 *
 * Injection safety: ids and authors only reach the YQL after 64-hex
 * validation; every other caller-supplied string is either escaped ([quote])
 * or passed out-of-band as a query parameter (the search term).
 */
object EventYql {
    /** Vespa's built-in no-scoring profile — filters without a search term. */
    const val RANK_UNRANKED = "unranked"

    /** The `text` profile in event.sd — filters with a search term. */
    const val RANK_TEXT = "text"

    fun build(q: EventQuery): VespaQuery? {
        val clauses = ArrayList<String>()

        if (q.ids.isNotEmpty()) clauses += hexIn("id", q.ids) ?: return null
        if (q.kinds.isNotEmpty()) clauses += "kind in (${q.kinds.joinToString(", ")})"
        if (q.authors.isNotEmpty()) clauses += hexIn("pubkey", q.authors) ?: return null
        if (q.owners.isNotEmpty()) clauses += hexIn("owner", q.owners) ?: return null
        for ((name, values) in q.tags) {
            clauses += tagClause(name, values, "or") ?: return null
        }
        for ((name, values) in q.tagsAll) {
            clauses += tagClause(name, values, "and") ?: return null
        }
        q.since?.let { clauses += "created_at >= $it" }
        q.until?.let { clauses += "created_at <= $it" }
        q.expiresBefore?.let { clauses += "expires_at < $it" }
        q.notExpiredAt?.let { clauses += "expires_at > $it" }

        val term = q.search?.trim().orEmpty()
        val params = LinkedHashMap<String, String>()
        if (term.isNotEmpty()) {
            clauses += "({defaultIndex:\"search_text\"}userInput(@search))"
            params["search"] = term
            q.observer
                ?.lowercase()
                ?.takeIf(HEX64::matches)
                ?.let { params["ranking.features.query(user_q)"] = "{$it:1.0}" }
        }

        val where = if (clauses.isEmpty()) "true" else clauses.joinToString(" and ")
        // No search term = plain relay REQ semantics: newest first, no scoring.
        val order = if (term.isEmpty()) " order by created_at desc" else ""
        val limit = q.limit?.let { if (it <= 0) return null else " limit $it" } ?: ""
        return VespaQuery(
            yql = "select * from event where $where$order$limit",
            params = params,
            ranking = if (term.isEmpty()) RANK_UNRANKED else RANK_TEXT,
        )
    }

    /**
     * One tag constraint: values joined with [op] ("or" = NIP-01 tags, "and" =
     * tagsAll). Null when it can't match: tag_index only holds single-letter
     * names, and a present-but-empty value list matches nothing.
     */
    private fun tagClause(
        name: String,
        values: List<String>,
        op: String,
    ): String? {
        if (name.length != 1 || (name[0] !in 'a'..'z' && name[0] !in 'A'..'Z')) return null
        if (values.isEmpty()) return null
        return values.joinToString(" $op ", prefix = "(", postfix = ")") { v -> "tag_index contains ${quote("$name:$v")}" }
    }

    /**
     * `field in (…)` over the valid 64-hex entries of [values] (normalized to
     * lowercase). Invalid entries can never match and are dropped — but if
     * nothing valid remains the constraint is unsatisfiable: null.
     */
    private fun hexIn(
        field: String,
        values: List<String>,
    ): String? {
        val hexes = values.map { it.lowercase() }.filter(HEX64::matches).distinct()
        if (hexes.isEmpty()) return null
        return "$field in (${hexes.joinToString(", ") { "\"$it\"" }})"
    }

    /** YQL string literal with backslash/quote/control escaping — for caller-supplied text. */
    private fun quote(s: String): String =
        "\"" +
            s
                .replace("\\", "\\\\")
                .replace("\"", "\\\"")
                .replace("\n", "\\n")
                .replace("\r", "\\r")
                .replace("\t", "\\t") +
            "\""

    private val HEX64 = Regex("^[0-9a-f]{64}$")
}
