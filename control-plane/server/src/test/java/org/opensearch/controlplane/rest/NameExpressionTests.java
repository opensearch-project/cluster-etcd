/*
 * Copyright OpenSearch Software Foundation
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.controlplane.rest;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** The comma-separated wildcard convention shared by index, alias, template, pipeline and setting names. */
class NameExpressionTests {

    private boolean matches(String glob, String name) {
        return NameExpression.toPattern(glob).matcher(name).matches();
    }

    @Test
    void starMatchesAnyRun() {
        assertTrue(matches("cp-*", "cp-a"));
        assertTrue(matches("cp-*", "cp-"), "including nothing at all");
        assertTrue(matches("*", "anything"));
        assertTrue(matches("a*b*c", "axxbyyc"));
        assertFalse(matches("cp-*", "other"));
    }

    /**
     * {@code *} is the only wildcard — OpenSearch's {@code Regex.isSimpleMatchPattern} tests for it
     * alone — so {@code ?} is an ordinary character rather than a single-character match.
     */
    @Test
    void questionMarkIsLiteral() {
        assertTrue(matches("a?b", "a?b"));
        assertFalse(matches("a?b", "axb"));
    }

    /** Everything outside a wildcard is quoted, so a name full of metacharacters is not a pattern. */
    @Test
    void regexMetacharactersAreLiteral() {
        assertTrue(matches("a.b", "a.b"));
        assertFalse(matches("a.b", "axb"));
        assertTrue(matches("index.number_*", "index.number_of_shards"));
        assertFalse(matches("index.number_*", "indexXnumber_of_shards"), "the dot is not any-character");
        assertTrue(matches("a+b", "a+b"), "a literal plus, as in a pipeline ID");
        assertTrue(matches("[x](y)", "[x](y)"));
    }

    @Test
    void parseSplitsOnCommasAndDropsBlanks() {
        List<java.util.regex.Pattern> patterns = NameExpression.parse(" a , , b-* ");

        assertEquals(2, patterns.size());
        assertTrue(NameExpression.matchesAny(patterns, "a"));
        assertTrue(NameExpression.matchesAny(patterns, "b-1"));
        assertFalse(NameExpression.matchesAny(patterns, "c"));
    }

    @Test
    void parsingNullOrBlankGivesNothingToMatch() {
        assertTrue(NameExpression.parse(null).isEmpty());
        assertTrue(NameExpression.parse("").isEmpty());
        assertFalse(NameExpression.matchesAny(NameExpression.parse(""), "anything"),
            "no patterns matches nothing, rather than everything");
    }
}
