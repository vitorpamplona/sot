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
    /**
     * Rank-profile override (the NIP-50 `sort:` extension): one of the
     * schema's profiles — [EventYql.RANK_DESC] / [EventYql.RANK_ASC] /
     * [EventYql.RANK_FILTERED] / [EventYql.RANK_FOLLOWERS] /
     * [EventYql.RANK_TEXT]. Null = the default ([EventYql.RANK_SEARCH] with a
     * term, unranked recency without). A non-null ranking with no term is a
     * trust-ordered match-all ("who does my observer rank highest").
     */
    val ranking: String? = null,
    /**
     * The per-observer trust floor — emitted as query(min_rank), which every
     * trust profile gates on (and the default profile's wot_mult() zeroes
     * below). Set from NIP-50 `filter:rank:…`, or the spam-filter default
     * (Brainstorm's onlyRanked) that `include:spam` switches off.
     */
    val minRank: Double? = null,
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
 * or passed out-of-band as a query parameter (the search words), except the
 * trigram literals, which are filtered to alphanumeric characters only.
 */
object EventYql {
    /** Vespa's built-in no-scoring profile — filters without a search term. */
    const val RANK_UNRANKED = "unranked"

    /** The DEFAULT search profile in event.sd (Brainstorm §12: text × concave trust). */
    const val RANK_SEARCH = "search"

    /** Pure text relevance, no trust (`sort:text` / Brainstorm text_relevance). */
    const val RANK_TEXT = "text"

    /** Text order with the trust floor applied (`filter:rank:…` without a sort). */
    const val RANK_FILTERED = "rank_filtered"

    /** Trust-sorted within each match tier, descending (`sort:rank`). */
    const val RANK_DESC = "rank_desc"

    /** Ascending trust within each (still-descending) match tier (`sort:rank:asc`). */
    const val RANK_ASC = "rank_asc"

    /** Brainstorm's default profile under its own name (`sort:followers`). */
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
            clauses += searchGroup(words, params)
            // Short queries lean harder on the trigram safety net (Brainstorm vespa.py).
            params["ranking.features.query(w_gram)"] = if (words.minOf { it.length } <= 3) "8.0" else "2.0"
        }

        val ranking = q.ranking ?: if (words.isEmpty()) RANK_UNRANKED else RANK_SEARCH
        if (ranking != RANK_UNRANKED) {
            q.observer
                ?.lowercase()
                ?.takeIf(HEX64::matches)
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

    // ---- the search group: Brainstorm's per-word fuzzy recall --------------
    // Port of brainstorm_server vespa_query.py build_query()/_word_group(),
    // extended with the generic tier fields. One OR group per query word
    // (each word matching ANY field recalls the doc; ranking sorts it out),
    // plus a joined-CamelCase variant (≥2 words: "John Carvalho" finds
    // @johncarvalho) and adjacent-pair concatenations (≥3 words). Words go
    // out-of-band as @w0..@w5 / @wj / @wp0.. query parameters.

    /** All word groups OR'd into one parenthesized clause, filling [params]. */
    private fun searchGroup(
        words: List<String>,
        params: MutableMap<String, String>,
    ): String {
        val groups = ArrayList<String>()
        words.forEachIndexed { i, word ->
            params["w$i"] = word
            groups += wordGroup("@w$i", word, withGrams = true)
        }
        if (words.size >= 2) {
            val joined = words.joinToString("")
            params["wj"] = joined
            groups += wordGroup("@wj", joined, withGrams = false)
        }
        if (words.size >= 3) {
            for (i in 0 until words.size - 1) {
                val pair = words[i] + words[i + 1]
                params["wp$i"] = pair
                groups += wordGroup("@wp$i", pair, withGrams = false)
            }
        }
        return "(${groups.joinToString(" or ")})"
    }

    /** One word's match clauses across every search field, plus its trigram safety net. */
    private fun wordGroup(
        param: String,
        literal: String,
        withGrams: Boolean,
    ): String {
        val maxEdits = wordMaxEdits(literal)
        val clauses = ArrayList<String>()
        for (field in SEARCH_FIELDS) clauses += fieldClauses(field, param, maxEdits, roleOf(field))
        if (withGrams) {
            for (gramField in OR_GRAM_FIELDS) orGramClause(literal, gramField)?.let { clauses += it }
            andAboutGramClause(literal)?.let { clauses += it }
        }
        return "(${clauses.joinToString(" or ")})"
    }

    /**
     * Match clauses for one (field, word): exact, prefix, and the
     * length-gated fuzzy tiers (Meilisearch's typo budget: <4 chars exact
     * or prefix only, ≥4 one edit, ≥9 two; prefixLength:2 = the first two
     * characters must match exactly). Labels feed the schema's match_quality
     * ladder on primary-role fields (known-inert today; kept verbatim).
     */
    private fun fieldClauses(
        field: String,
        param: String,
        maxEdits: Int,
        role: Role,
    ): List<String> {
        fun ann(
            extra: String?,
            label: String?,
        ): String {
            val parts = ArrayList<String>(3)
            parts += "defaultIndex:\"$field\""
            extra?.let { parts += it }
            label?.let { parts += "label:\"$it\"" }
            return parts.joinToString(",", prefix = "{", postfix = "}")
        }

        val exactLabel =
            if (role == Role.PRIMARY) {
                "mtch_exact"
            } else if (role == Role.AFFILIATION) {
                "mtch_affil"
            } else {
                null
            }
        val clauses = ArrayList<String>(4)
        clauses += "(${ann(null, exactLabel)}userInput($param))"
        clauses += "(${ann("prefix:true", if (role == Role.PRIMARY) "mtch_prefix" else null)}userInput($param))"
        if (maxEdits >= 1) {
            clauses += "(${ann("fuzzy:{maxEditDistance:1,prefixLength:2}", if (role == Role.PRIMARY) "mtch_fz1" else null)}userInput($param))"
        }
        if (maxEdits >= 2) {
            clauses += "(${ann("fuzzy:{maxEditDistance:2,prefixLength:2}", if (role == Role.PRIMARY) "mtch_fz2" else null)}userInput($param))"
        }
        return clauses
    }

    /** OR of the word's trigrams against a gram field — the recall safety net. */
    private fun orGramClause(
        word: String,
        gramField: String,
    ): String? {
        val grams = trigrams(word.lowercase()).distinct().sorted()
        if (grams.isEmpty()) return null
        return grams.joinToString(" or ", prefix = "(", postfix = ")") { "$gramField contains \"$it\"" }
    }

    /** AND of the word's trigrams against about_gram (discriminative, unlike the OR nets). */
    private fun andAboutGramClause(word: String): String? {
        val grams = trigrams(word)
        if (grams.isEmpty()) return null
        return grams.joinToString(" and ", prefix = "(", postfix = ")") { "about_gram contains \"$it\"" }
    }

    /** Alphanumeric-only trigrams — safe to embed in YQL without escaping. */
    private fun trigrams(word: String): List<String> =
        (0..word.length - 3)
            .map { word.substring(it, it + 3) }
            .filter { gram -> gram.all(Char::isLetterOrDigit) }

    private fun wordMaxEdits(word: String): Int =
        when {
            word.length >= 9 -> 2
            word.length >= 4 -> 1
            else -> 0
        }

    private enum class Role { PRIMARY, AFFILIATION, RECALL }

    /**
     * Field roles (vespa_query.py): primary = the name-tier fields whose
     * clauses carry the match-quality labels (nip05/lud16 are @-address
     * identity fields; search_primary is the tier twin), affiliation =
     * bio + website (exact clause labeled mtch_affil), recall = everything
     * that matches without labeling.
     */
    private fun roleOf(field: String): Role =
        when (field) {
            "name", "display_name", "nip05", "lud16", "search_primary" -> Role.PRIMARY
            "about", "website" -> Role.AFFILIATION
            else -> Role.RECALL
        }

    private val SEARCH_FIELDS =
        listOf("name", "display_name", "about", "nip05", "lud16", "website", "search_primary", "search_secondary", "search_text")

    private val OR_GRAM_FIELDS = listOf("name_gram", "display_name_gram", "search_primary_gram")

    private val WHITESPACE = Regex("\\s+")

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
