package com.vitorpamplona.sot.vespa

/** Max query words we label / parametrize (mirrors the upstream server). */
const val MAX_QUERY_WORDS = 6

/**
 * Builds the Vespa YQL for a profile search — the candidate-set half of the
 * "search equations". The *ranking* half lives in `vespa/schemas/doc.sd`.
 *
 * Each query word fans out into exact / prefix / (bounded) fuzzy `userInput`
 * clauses across name/display_name/about, plus discriminative trigram clauses
 * (every trigram must be present) against the `*_gram` fields.
 */
object Yql {
    /**
     * AND of one word's trigrams against a `*_gram` field. Requiring *every*
     * trigram (not any single shared one) keeps the near-miss long tail out.
     * Empty string if the word yields no usable trigrams.
     */
    fun gramAndClause(word: String, gramField: String, gramSize: Int = 3): String {
        val w = word.lowercase()
        if (w.length < gramSize) return ""
        val grams =
            (0..(w.length - gramSize))
                .map { w.substring(it, it + gramSize) }
                .filter { g -> g.all { it.isLetterOrDigit() } }
        if (grams.isEmpty()) return ""
        return "(" + grams.joinToString(" and ") { "$gramField contains \"$it\"" } + ")"
    }

    /** Per-word fuzzy budget: 1 edit for >=4 chars, disabled below (typo noise). */
    fun wordMaxEdits(word: String): Int = if (word.length < 4) 0 else 1

    fun fieldClauses(field: String, varName: String, maxEdits: Int): List<String> {
        val parts =
            mutableListOf(
                "({defaultIndex:\"$field\"}userInput($varName))",
                "({defaultIndex:\"$field\",prefix:true}userInput($varName))",
            )
        if (maxEdits > 0) {
            // prefixLength:2 anchors the first two characters so the typo is inside the word.
            parts.add("({defaultIndex:\"$field\",fuzzy:{maxEditDistance:$maxEdits,prefixLength:2}}userInput($varName))")
        }
        return parts
    }

    /** All match clauses for one query word across name/display_name/about + grams. */
    fun wordGroup(varName: String, literal: String, withGrams: Boolean = true): String {
        val me = wordMaxEdits(literal)
        val clauses = mutableListOf<String>()
        for (field in listOf("name", "display_name", "about")) clauses += fieldClauses(field, varName, me)
        if (withGrams) {
            for (gramField in listOf("name_gram", "display_name_gram", "about_gram")) {
                val gc = gramAndClause(literal, gramField)
                if (gc.isNotEmpty()) clauses.add(gc)
            }
        }
        return "(" + clauses.joinToString(" or ") + ")"
    }

    /**
     * Per-word groups OR'd together, plus an optional joined-CamelCase variant
     * (whole-token only) so "vitor pamplona" still hits a doc named "VitorPamplona".
     */
    fun build(words: List<String>, joined: String?): String {
        val parts =
            words.take(MAX_QUERY_WORDS).mapIndexed { i, w -> wordGroup("@w$i", w) }.toMutableList()
        if (joined != null) parts.add(wordGroup("@wj", joined, withGrams = false))
        return "select * from doc where " + parts.joinToString(" or ")
    }
}
