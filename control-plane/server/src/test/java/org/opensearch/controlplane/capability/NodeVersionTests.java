/*
 * Copyright OpenSearch Software Foundation
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.controlplane.capability;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class NodeVersionTests {

    /** Upstream's ids, from {@code Version.java}: {@code new Version(3100099, ...)} with the mask XORed in. */
    @Test
    void encodesAsUpstreamsVersionId() {
        assertEquals(137317827, NodeVersion.parse("3.10.0").encodedId());
        assertEquals(137217827, NodeVersion.parse("3.0.0").encodedId());
        assertEquals(136407927, NodeVersion.parse("2.19.1").encodedId());
    }

    @Test
    void aQualifierIsDropped() {
        assertEquals(NodeVersion.parse("3.10.0"), NodeVersion.parse("3.10.0-SNAPSHOT"));
        assertEquals("3.10.0", NodeVersion.parse("3.10.0-SNAPSHOT").toString());
    }

    /** By number, not text: 3.9.0 is older than 3.10.0. */
    @Test
    void comparesNumerically() {
        List<NodeVersion> sorted = Stream.of("3.10.0", "3.9.0", "2.19.1", "3.10.1")
            .map(NodeVersion::parse).sorted().toList();
        assertEquals("[2.19.1, 3.9.0, 3.10.0, 3.10.1]", sorted.toString());
        assertTrue(NodeVersion.parse("3.9.0").compareTo(NodeVersion.parse("3.10.0")) < 0);
    }

    @Test
    void rejectsWhatIsNotAVersion() {
        for (String bad : List.of("", "3.10", "three", "3.10.0.1", "v3.10.0")) {
            assertThrows(IllegalArgumentException.class, () -> NodeVersion.parse(bad), bad);
        }
    }
}
