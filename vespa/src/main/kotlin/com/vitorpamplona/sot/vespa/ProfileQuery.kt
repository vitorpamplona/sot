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

/** Max query words we label / parametrize (mirrors the upstream server). */
const val MAX_QUERY_WORDS = 6

/**
 * Builds the query that selects candidate profile documents — the recall half of
 * the "search equations" (the YQL `where`). The *ranking* half lives in
 * `app/schemas/doc.sd` (in this module).
 *
 * Each query word fans out into exact / prefix / (bounded) fuzzy `userInput`
 * clauses across name/display_name/about, plus discriminative trigram clauses
 * (every trigram must be present) against the `*_gram` fields.
 */
object ProfileQuery {
    private val TEXT_FIELDS = listOf("name", "display_name", "about")
    private val GRAM_FIELDS = listOf("name_gram", "display_name_gram", "about_gram")
    private const val GRAM_SIZE = 3

    /**
     * Per-word groups OR'd together, plus an optional joined-CamelCase variant
     * (whole token, no grams) so "vitor pamplona" still hits a doc named "VitorPamplona".
     */
    fun build(words: List<String>, joined: String?): String {
        val groups = buildList {
            words.take(MAX_QUERY_WORDS).forEachIndexed { i, word -> add(wordGroup("@w$i", word)) }
            if (joined != null) add(wordGroup("@wj", joined, withGrams = false))
        }
        return "select * from doc where " + groups.joinToString(" or ")
    }

    /** Every match clause for one query word — OR'd — across the text fields (+ grams). */
    fun wordGroup(varName: String, literal: String, withGrams: Boolean = true): String {
        val maxEdits = wordMaxEdits(literal)
        val clauses = buildList {
            TEXT_FIELDS.forEach { field -> addAll(fieldClauses(field, varName, maxEdits)) }
            if (withGrams) {
                GRAM_FIELDS.forEach { field -> gramAndClause(literal, field).ifEmpty { null }?.let(::add) }
            }
        }
        return clauses.joinToString(" or ", prefix = "(", postfix = ")")
    }

    /** exact + prefix (+ fuzzy, when [maxEdits] > 0) `userInput` matches of one word against one field. */
    fun fieldClauses(field: String, varName: String, maxEdits: Int): List<String> = buildList {
        add(userInput(field, varName))
        add(userInput(field, varName, "prefix:true"))
        // prefixLength:2 anchors the first two characters so the typo is inside the word.
        if (maxEdits > 0) add(userInput(field, varName, "fuzzy:{maxEditDistance:$maxEdits,prefixLength:2}"))
    }

    /**
     * AND of one word's trigrams against a `*_gram` field. Requiring *every*
     * trigram (not any single shared one) keeps the near-miss long tail out.
     * Empty string if the word yields no usable trigrams.
     */
    fun gramAndClause(word: String, gramField: String, gramSize: Int = GRAM_SIZE): String {
        val grams = trigrams(word.lowercase(), gramSize)
        if (grams.isEmpty()) return ""
        return grams.joinToString(" and ", prefix = "(", postfix = ")") { gram -> "$gramField contains \"$gram\"" }
    }

    /** Per-word fuzzy budget: 1 edit for >=4 chars, disabled below (typo noise). */
    fun wordMaxEdits(word: String): Int = if (word.length < 4) 0 else 1

    /** A Vespa `userInput()` term against one field, e.g. `({defaultIndex:"name",prefix:true}userInput(@w0))`. */
    private fun userInput(field: String, varName: String, vararg options: String): String {
        val annotations = (listOf("defaultIndex:\"$field\"") + options).joinToString(",")
        return "({$annotations}userInput($varName))"
    }

    /** The alphanumeric trigrams (sliding windows of [size]) of [word]. */
    private fun trigrams(word: String, size: Int): List<String> {
        if (word.length < size) return emptyList()
        return (0..word.length - size)
            .map { start -> word.substring(start, start + size) }
            .filter { gram -> gram.all(Char::isLetterOrDigit) }
    }
}
