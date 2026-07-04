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

import com.vitorpamplona.quartz.utils.Hex

/**
 * Builds YQL over the `event` schema from an [EventQuery]. Returns null when
 * the query provably matches nothing, so the caller can answer with an empty
 * result (EOSE) instead of asking Vespa. That happens for an id/author
 * constraint with no valid 64-hex entries, a non-single-letter tag name, or
 * limit 0.
 *
 * Injection safety: ids and authors only reach the YQL after 64-hex
 * validation. Every other caller-supplied string is either escaped ([quote])
 * or passed out-of-band as a query parameter (the search words). The one
 * exception is the trigram literals, which are filtered to alphanumeric
 * characters only.
 */
object EventYql {
    /** Vespa's built-in no-scoring profile — filters without a search term. */
    const val RANK_UNRANKED = "unranked"

    /** The DEFAULT search profile in event.sd: text relevance combined with concave trust. */
    const val RANK_SEARCH = "search"

    /** Pure text relevance, no trust (`sort:text`). */
    const val RANK_TEXT = "text"

    /** Text order with the trust floor applied (`filter:rank:…` without a sort). */
    const val RANK_FILTERED = "rank_filtered"

    /** Trust-sorted within each match tier, descending (`sort:rank`). */
    const val RANK_DESC = "rank_desc"

    /** Ascending trust within each (still-descending) match tier (`sort:rank:asc`). */
    const val RANK_ASC = "rank_asc"

    /** Follower-count ranking (`sort:followers`). */
    const val RANK_FOLLOWERS = "sort_followers"

    /** YQL caps at this many query words; the rest add nothing and are dropped. */
    const val MAX_QUERY_WORDS = 6

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

        val params = LinkedHashMap<String, String>()
        val words =
            q.search
                ?.trim()
                .orEmpty()
                .split(WHITESPACE)
                .filter { it.isNotEmpty() }
                .take(MAX_QUERY_WORDS)
        if (words.isNotEmpty()) {
            clauses += BrainstormWordGroup.clause(words, params)
            // Short queries lean harder on the trigram safety net.
            params["ranking.features.query(w_gram)"] = if (BrainstormWordGroup.leansOnGrams(words)) "8.0" else "2.0"
        }

        val ranking = q.ranking ?: if (words.isEmpty()) RANK_UNRANKED else RANK_SEARCH
        if (ranking != RANK_UNRANKED) {
            q.observer
                ?.lowercase()
                ?.takeIf(Hex::isHex64)
                ?.let { params["ranking.features.query(user_q)"] = "{$it:1.0}" }
        }
        q.minRank?.let { params["ranking.features.query(min_rank)"] = it.toString() }

        val where = if (clauses.isEmpty()) "true" else clauses.joinToString(" and ")
        // No text and no rank profile = plain relay REQ semantics: newest
        // first, no scoring. Anything ranked keeps Vespa's score order.
        val order = if (ranking == RANK_UNRANKED) " order by created_at desc" else ""
        val limit = q.limit?.let { if (it <= 0) return null else " limit $it" } ?: ""
        return VespaQuery(
            yql = "select * from event where $where$order$limit",
            params = params,
            ranking = ranking,
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
        if (!isSingleLetterTagName(name)) return null
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
        val hexes = values.map { it.lowercase() }.filter(Hex::isHex64).distinct()
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
}
