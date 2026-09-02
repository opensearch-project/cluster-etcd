/*
 * Copyright OpenSearch Software Foundation
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.controlplane.rest;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Semantics verified against OpenSearch 3.9.0; see {@link FilterPath}. */
class FilterPathTests {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private static final String DOC = """
        {
          "name": "n1",
          "cluster_name": "c1",
          "version": {"number": "3.9.0", "build_type": "tar", "distribution": "opensearch"},
          "tagline": "hello"
        }
        """;

    private static String filter(String json, String filterPath) throws IOException {
        return FilterPath.apply(MAPPER.readTree(json), filterPath).toString();
    }

    @Test
    void selectsASingleTopLevelField() throws IOException {
        assertEquals("{\"cluster_name\":\"c1\"}", filter(DOC, "cluster_name"));
    }

    @Test
    void keepsNestingRatherThanFlattening() throws IOException {
        assertEquals("{\"version\":{\"number\":\"3.9.0\"}}", filter(DOC, "version.number"));
    }

    @Test
    void unionsCommaSeparatedPaths() throws IOException {
        assertEquals("{\"name\":\"n1\",\"cluster_name\":\"c1\"}", filter(DOC, "name,cluster_name"));
    }

    @Test
    void singleStarMatchesOneSegment() throws IOException {
        JsonNode result = MAPPER.readTree(filter(DOC, "version.*"));
        assertEquals(3, result.get("version").size(), "every child of version");
        assertFalse(result.has("name"));
    }

    @Test
    void starMatchesMapKeys() throws IOException {
        // The nodes.*.name shape: a map keyed by an id we do not know in advance.
        String nodes = "{\"nodes\":{\"abc\":{\"name\":\"n1\",\"roles\":[]},\"def\":{\"name\":\"n2\",\"roles\":[]}}}";
        assertEquals(
            "{\"nodes\":{\"abc\":{\"name\":\"n1\"},\"def\":{\"name\":\"n2\"}}}",
            filter(nodes, "nodes.*.name")
        );
    }

    @Test
    void doubleStarMatchesZeroOrMoreSegments() throws IOException {
        JsonNode result = MAPPER.readTree(filter(DOC, "version.**"));
        assertEquals(3, result.get("version").size());
    }

    @Test
    void exclusionKeepsEverythingElse() throws IOException {
        JsonNode result = MAPPER.readTree(filter(DOC, "-version"));

        assertFalse(result.has("version"));
        assertTrue(result.has("name") && result.has("cluster_name") && result.has("tagline"));
    }

    @Test
    void exclusionCombinesWithInclusion() throws IOException {
        assertEquals("{\"version\":{\"number\":\"3.9.0\"}}", filter(DOC, "version.number,-version.build_type"));
    }

    @Test
    void exclusionPrunesInsideAnIncludedSubtree() throws IOException {
        JsonNode result = MAPPER.readTree(filter(DOC, "version,-version.build_type"));

        assertTrue(result.get("version").has("number"));
        assertTrue(result.get("version").has("distribution"));
        assertFalse(result.get("version").has("build_type"), "an exclusion applies beneath an inclusion");
    }

    @Test
    void nothingMatchingYieldsAnEmptyObject() throws IOException {
        assertEquals("{}", filter(DOC, "nonexistent"));
    }

    @Test
    void blankFilterLeavesTheDocumentAlone() throws IOException {
        assertEquals(MAPPER.readTree(DOC), FilterPath.apply(MAPPER.readTree(DOC), null));
        assertEquals(MAPPER.readTree(DOC), FilterPath.apply(MAPPER.readTree(DOC), "   "));
    }

    /**
     * The detail most likely to be guessed wrong: an array index is not a path segment, so a filter
     * reaches straight into array elements and {@code *.field} matches nothing.
     */
    @Test
    void arraysAreTransparentToPathMatching() throws IOException {
        String array = "[{\"index\":\"a\",\"health\":\"green\"},{\"index\":\"b\",\"health\":\"red\"}]";

        assertEquals("[{\"index\":\"a\"},{\"index\":\"b\"}]", filter(array, "index"));
        assertEquals("[]", filter(array, "*.index"));
    }

    @Test
    void filtersArraysNestedInObjects() throws IOException {
        String doc = "{\"hits\":{\"hits\":[{\"_id\":\"1\",\"_score\":1.0},{\"_id\":\"2\",\"_score\":2.0}]}}";

        assertEquals("{\"hits\":{\"hits\":[{\"_id\":\"1\"},{\"_id\":\"2\"}]}}", filter(doc, "hits.hits._id"));
    }

    @Test
    void dropsArrayElementsThatMatchNothing() throws IOException {
        assertEquals("{}", filter("{\"a\":[{\"x\":1}]}", "a.y"));
    }

    @Test
    void leavesScalarsAndEmptyContainersAlone() throws IOException {
        assertEquals("{\"a\":[]}", filter("{\"a\":[],\"b\":1}", "a"));
        assertEquals("{\"b\":1}", filter("{\"a\":[],\"b\":1}", "b"));
    }
}
