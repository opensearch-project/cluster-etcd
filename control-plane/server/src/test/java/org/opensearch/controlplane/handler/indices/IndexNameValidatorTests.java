/*
 * Copyright OpenSearch Software Foundation
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.controlplane.handler.indices;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;

/** Every message and ordering here was verified against OpenSearch 3.9.0. */
class IndexNameValidatorTests {

    private static String reason(String name) {
        return IndexNameValidator.validate(name).orElse(null);
    }

    @Test
    void acceptsOrdinaryNames() {
        for (String name : List.of("my-index", "logs-000001", "a", "a.b", "a+b", "index_1", "ünïcodé")) {
            assertEquals(Optional.empty(), IndexNameValidator.validate(name), name);
        }
    }

    @Test
    void rejectsInvalidCharacters() {
        String expected = "must not contain the following characters [ , \", *, \\, <, |, ,, >, /, ?]";
        for (String name : List.of("a b", "a\"b", "a*b", "a\\b", "a<b", "a|b", "a,b", "a>b", "a/b", "a?b")) {
            assertEquals(expected, reason(name), name);
        }
    }

    @Test
    void rejectsHashAndColonSeparately() {
        assertEquals("must not contain '#'", reason("a#b"));
        assertEquals("must not contain ':'", reason("a:b"));
    }

    @Test
    void rejectsLeadingUnderscoreHyphenAndPlus() {
        for (String name : List.of("_a", "-a", "+a")) {
            assertEquals("must not start with '_', '-', or '+'", reason(name), name);
        }
        // Only the first character is restricted.
        assertEquals(Optional.empty(), IndexNameValidator.validate("a_b"));
    }

    @Test
    void rejectsUppercase() {
        assertEquals("must be lowercase", reason("MyIndex"));
        assertEquals("must be lowercase", reason("a-B"));
    }

    @Test
    void rejectsDotAndDotDot() {
        assertEquals("must not be '.' or '..'", reason("."));
        assertEquals("must not be '.' or '..'", reason(".."));
        assertEquals(Optional.empty(), IndexNameValidator.validate("..."));
    }

    @Test
    void rejectsEmpty() {
        assertEquals("must not be empty", reason(""));
        assertEquals("must not be empty", reason(null));
    }

    @Test
    void enforcesTheByteLengthLimit() {
        assertEquals(Optional.empty(), IndexNameValidator.validate("a".repeat(255)));
        assertEquals("index name is too long, (256 > 255)", reason("a".repeat(256)));
        // Measured in UTF-8 bytes, not characters: 'é' is two bytes.
        assertEquals("index name is too long, (256 > 255)", reason("é".repeat(128)));
    }

    /**
     * The orderings that matter. A name breaking several rules yields exactly one message, and which
     * one was determined empirically rather than assumed.
     */
    @Test
    void appliesRulesInTheServersOrder() {
        assertEquals(
            "must not contain the following characters [ , \", *, \\, <, |, ,, >, /, ?]",
            reason("CP a"),
            "invalid characters beat lowercase"
        );
        assertEquals("must not contain '#'", reason("_cp#a"), "hash beats the leading-character rule");
        assertEquals("must not contain '#'", reason("cp:#a"), "hash beats colon");
        assertEquals("must not contain ':'", reason("_cp:a"), "colon beats the leading-character rule");
        assertEquals(
            "must not start with '_', '-', or '+'",
            reason("_CPa"),
            "leading-character rule beats lowercase"
        );
    }

    @Test
    void formatsTheFullMessage() {
        assertEquals(
            "Invalid index name [MyIndex], must be lowercase",
            IndexNameValidator.message("MyIndex", "must be lowercase")
        );
    }

    /**
     * Case is validateIndexName's alone and runs after the shared rules, length among them, so an
     * over-long upper-case name reports its length. Aliases skip the case rule entirely.
     */
    @Test
    void caseIsCheckedAfterTheSharedRulesAndOnlyForIndices() {
        assertEquals("index name is too long, (256 > 255)", reason("A".repeat(256)));
        assertEquals(Optional.empty(), IndexNameValidator.validateIndexOrAliasName("MyAlias"));
    }
}
