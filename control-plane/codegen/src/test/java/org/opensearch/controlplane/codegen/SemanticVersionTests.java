/*
 * Copyright OpenSearch Software Foundation
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.controlplane.codegen;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SemanticVersionTests {

    @Test
    void coercesTheFormsTheSpecActuallyUses() {
        // The spec mixes two- and three-component forms: '2.11' next to 2.10.0.
        assertEquals("2.11.0", SemanticVersion.parse("2.11").toString());
        assertEquals("2.10.0", SemanticVersion.parse("2.10.0").toString());
        assertEquals("1.0.0", SemanticVersion.parse("1.0").toString());
        assertEquals("3.0.0", SemanticVersion.parse("3").toString());
        assertEquals("2.17.1", SemanticVersion.parse("2.17.1").toString());
    }

    @Test
    void dropsPreReleaseAndBuildSuffixes() {
        // A snapshot build must compare equal to the release, not sort before it.
        assertEquals("3.9.0", SemanticVersion.parse("3.9.0-SNAPSHOT").toString());
        assertEquals(SemanticVersion.parse("3.9.0"), SemanticVersion.parse("3.9.0-SNAPSHOT"));
        assertEquals("2.19.0", SemanticVersion.parse("2.19.0+build.7").toString());
    }

    @Test
    void ordersNumericallyNotLexicographically() {
        // The bug a string comparison would introduce: "2.9" > "2.11" as text.
        assertTrue(SemanticVersion.parse("2.11").isAtLeast(SemanticVersion.parse("2.9")));
        assertFalse(SemanticVersion.parse("2.9").isAtLeast(SemanticVersion.parse("2.11")));

        List<String> ascending = List.of("1.0", "1.3", "2.0", "2.9", "2.10", "2.11", "3.0", "3.5", "3.9.0");
        for (int i = 1; i < ascending.size(); i++) {
            SemanticVersion lower = SemanticVersion.parse(ascending.get(i - 1));
            SemanticVersion higher = SemanticVersion.parse(ascending.get(i));
            assertTrue(lower.isBefore(higher), lower + " should sort before " + higher);
        }
    }

    @Test
    void comparesPatchComponent() {
        assertTrue(SemanticVersion.parse("2.17.1").isAtLeast(SemanticVersion.parse("2.17.0")));
        assertTrue(SemanticVersion.parse("2.17.0").isBefore(SemanticVersion.parse("2.17.1")));
        assertEquals(SemanticVersion.parse("2.17"), SemanticVersion.parse("2.17.0"));
    }

    @Test
    void isAtLeastIsInclusive() {
        SemanticVersion target = SemanticVersion.parse("3.9.0");
        assertTrue(target.isAtLeast(target));
        assertFalse(target.isBefore(target));
    }

    /**
     * The exact predicates the codegen applies: include when the target is at or after
     * {@code x-version-added}, and strictly before {@code x-version-removed}.
     */
    @Test
    void modelsTheInclusionRules() {
        SemanticVersion target = SemanticVersion.parse("3.9.0");

        assertFalse(target.isBefore(SemanticVersion.parse("3.5")), "added in 3.5 -> available at 3.9");
        assertTrue(SemanticVersion.parse("2.11").isBefore(SemanticVersion.parse("3.5")),
            "added in 3.5 -> unavailable at 2.11");

        // Removal is exclusive: removed in 3.0 means gone from 3.0 onwards.
        assertFalse(target.isBefore(SemanticVersion.parse("3.0")), "removed in 3.0 -> gone at 3.9");
        assertTrue(SemanticVersion.parse("2.19").isBefore(SemanticVersion.parse("3.0")),
            "removed in 3.0 -> still present at 2.19");
        assertFalse(SemanticVersion.parse("3.0").isBefore(SemanticVersion.parse("3.0")),
            "removed in 3.0 -> gone at exactly 3.0");
    }

    @Test
    void rejectsStringsWithNoVersionInThem() {
        assertThrows(IllegalArgumentException.class, () -> SemanticVersion.parse("latest"));
        assertThrows(IllegalArgumentException.class, () -> SemanticVersion.parse(""));
        assertThrows(NullPointerException.class, () -> SemanticVersion.parse(null));
    }
}
