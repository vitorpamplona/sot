package com.vitorpamplona.sot.vespa

import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue

class YqlTest {
    @Test
    fun `fuzzy budget disabled below 4 chars, 1 edit otherwise`() {
        assertEquals(0, Yql.wordMaxEdits("abc"))
        assertEquals(0, Yql.wordMaxEdits("ab"))
        assertEquals(1, Yql.wordMaxEdits("abcd"))
        assertEquals(1, Yql.wordMaxEdits("vitorpamplona"))
    }

    @Test
    fun `gram clause ANDs every trigram`() {
        assertEquals(
            "(about_gram contains \"nos\" and about_gram contains \"osf\" and " +
                "about_gram contains \"sfa\" and about_gram contains \"fab\")",
            Yql.gramAndClause("nosfab", "about_gram"),
        )
    }

    @Test
    fun `gram clause empty for words shorter than gram size`() {
        assertEquals("", Yql.gramAndClause("ab", "name_gram"))
        assertEquals("", Yql.gramAndClause("", "name_gram"))
    }

    @Test
    fun `short word has no fuzzy clause`() {
        val clauses = Yql.fieldClauses("name", "@w0", Yql.wordMaxEdits("abc"))
        assertEquals(2, clauses.size)
        assertFalse(clauses.any { it.contains("fuzzy") })
    }

    @Test
    fun `long word has exact, prefix and fuzzy across the three text fields`() {
        val group = Yql.wordGroup("@w0", "vitor")
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
        val yql = Yql.build(listOf("vitor", "pamplona"), "vitorpamplona")
        assertTrue(yql.startsWith("select * from doc where "))
        assertTrue(yql.contains("userInput(@w0)"))
        assertTrue(yql.contains("userInput(@w1)"))
        assertTrue(yql.contains("userInput(@wj)"))
        // the joined variant must not add gram clauses
        val joinedPart = yql.substringAfterLast("userInput(@wj)")
        assertFalse(joinedPart.contains("_gram contains"))
    }
}
