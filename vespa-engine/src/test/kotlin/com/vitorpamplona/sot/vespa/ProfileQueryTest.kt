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

import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue

class ProfileQueryTest {
    @Test
    fun `fuzzy budget disabled below 4 chars, 1 edit otherwise`() {
        assertEquals(0, ProfileQuery.wordMaxEdits("abc"))
        assertEquals(0, ProfileQuery.wordMaxEdits("ab"))
        assertEquals(1, ProfileQuery.wordMaxEdits("abcd"))
        assertEquals(1, ProfileQuery.wordMaxEdits("vitorpamplona"))
    }

    @Test
    fun `gram clause ANDs every trigram`() {
        assertEquals(
            "(about_gram contains \"nos\" and about_gram contains \"osf\" and " +
                "about_gram contains \"sfa\" and about_gram contains \"fab\")",
            ProfileQuery.gramAndClause("nosfab", "about_gram"),
        )
    }

    @Test
    fun `gram clause empty for words shorter than gram size`() {
        assertEquals("", ProfileQuery.gramAndClause("ab", "name_gram"))
        assertEquals("", ProfileQuery.gramAndClause("", "name_gram"))
    }

    @Test
    fun `short word has no fuzzy clause`() {
        val clauses = ProfileQuery.fieldClauses("name", "@w0", ProfileQuery.wordMaxEdits("abc"))
        assertEquals(2, clauses.size)
        assertFalse(clauses.any { it.contains("fuzzy") })
    }

    @Test
    fun `long word has exact, prefix and fuzzy across the three text fields`() {
        val group = ProfileQuery.wordGroup("@w0", "vitor")
        for (field in listOf("name", "display_name", "about")) {
            assertTrue(group.contains("{defaultIndex:\"$field\"}userInput(@w0)"), "exact on $field")
            assertTrue(group.contains("{defaultIndex:\"$field\",prefix:true}userInput(@w0)"), "prefix on $field")
            assertTrue(group.contains("fuzzy:{maxEditDistance:1,prefixLength:2}"), "fuzzy present")
        }
        // grams for a 5-char word
        assertTrue(group.contains("name_gram contains \"vit\""))
    }

    @Test
    fun `build joins per-word groups and a whole-token joined variant without grams`() {
        val yql = ProfileQuery.build(listOf("vitor", "pamplona"), "vitorpamplona")
        assertTrue(yql.startsWith("select * from doc where "))
        assertTrue(yql.contains("userInput(@w0)"))
        assertTrue(yql.contains("userInput(@w1)"))
        assertTrue(yql.contains("userInput(@wj)"))
        // the joined variant must not add gram clauses
        val joinedPart = yql.substringAfterLast("userInput(@wj)")
        assertFalse(joinedPart.contains("_gram contains"))
    }
}
