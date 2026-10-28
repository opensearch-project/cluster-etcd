/*
 * Copyright OpenSearch Software Foundation
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.controlplane.store.zookeeper;

import org.apache.zookeeper.common.PathUtils;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The key-to-znode-path encoding.
 *
 * <p>Runs without a ZooKeeper server, and is the more valuable half of the testing for it: the encoding
 * is where a backend silently corrupts keys, and every path it produces is checked against ZooKeeper's
 * <em>own</em> {@link PathUtils#validatePath} rather than against my reading of the rules.
 */
class ZooKeeperKeysTests {

    /** Keys the store contract exercises, plus the ones ZooKeeper has opinions about. */
    private static final List<String> AWKWARD_KEYS = List.of(
        "a",
        "my-index",
        "logs.2026.01.01",
        "tenant/default/indices/my-index",
        "t/acme/idx/a+b",
        "t/acme/idx/a.b",
        "t/acme/idx/a%2Fb",
        "t/acme/idx/..",
        "t/acme/idx/.",
        "t/acme/idx/...",
        "t/acme/idx/with space",
        "t/acme/idx/ünïcodé",
        "tenant/acme/../globex/x",
        "a//b",
        "a/",
        "trailing/slash/",
        "%",
        "%%",
        "%2E",
        "zookeeper",
        "ab",
        "emoji/😀",
        "ingest/pipeline/a",
        "ingest/pipelinex"
    );

    @Test
    void everySegmentRoundTrips() {
        for (String key : AWKWARD_KEYS) {
            for (String segment : ZooKeeperKeys.split(key)) {
                assertEquals(segment, ZooKeeperKeys.decodeSegment(ZooKeeperKeys.encodeSegment(segment)),
                    "segment [" + segment + "] of key [" + key + "]");
            }
        }
    }

    /** The whole key round-trips, which is what the store relies on when decoding a scan. */
    @Test
    void everyKeyRoundTrips() {
        for (String key : AWKWARD_KEYS) {
            String path = ZooKeeperKeys.pathOf("/cp", key);
            String recovered = String.join("/",
                java.util.Arrays.stream(path.substring("/cp".length()).split("/", -1))
                    .skip(1)
                    .map(ZooKeeperKeys::decodeSegment)
                    .toList());
            assertEquals(key, recovered, "key [" + key + "] via path [" + path + "]");
        }
    }

    /** ZooKeeper's own validator is the authority on what it will accept. */
    @Test
    void everyEncodedPathIsAcceptedByZooKeeper() {
        for (String key : AWKWARD_KEYS) {
            String path = ZooKeeperKeys.pathOf("/cp", key);
            assertDoesNotThrow(() -> PathUtils.validatePath(path), "key [" + key + "] gave path [" + path + "]");
        }
    }

    /** The two cases the general percent-encoding rule does not cover. */
    @Test
    void dotOnlySegmentsEscapeTheirFirstDot() {
        assertEquals("%2E", ZooKeeperKeys.encodeSegment("."));
        assertEquals("%2E.", ZooKeeperKeys.encodeSegment(".."));
        assertEquals("%2E..", ZooKeeperKeys.encodeSegment("..."));
    }

    @Test
    void anEmptySegmentIsALonePercent() {
        assertEquals("%", ZooKeeperKeys.encodeSegment(""));
        assertEquals("", ZooKeeperKeys.decodeSegment("%"));
    }

    /** A dot inside a longer name is left alone, so ordinary names stay readable in zkCli. */
    @Test
    void ordinaryNamesAreNotEscaped() {
        assertEquals("my-index", ZooKeeperKeys.encodeSegment("my-index"));
        assertEquals("logs.2026.01.01", ZooKeeperKeys.encodeSegment("logs.2026.01.01"));
        assertEquals("a_b~c", ZooKeeperKeys.encodeSegment("a_b~c"));
    }

    /** The escape character itself must escape, or decoding is ambiguous. */
    @Test
    void percentIsEscaped() {
        assertEquals("%25", ZooKeeperKeys.encodeSegment("%"));
        assertEquals("a%252Fb", ZooKeeperKeys.encodeSegment("a%2Fb"));
        assertEquals("a%2Fb", ZooKeeperKeys.decodeSegment("a%252Fb"));
    }

    /** Control characters are illegal in znode names and must not survive as themselves. */
    @Test
    void controlCharactersAreEscaped() {
        String encoded = ZooKeeperKeys.encodeSegment("ab");

        assertEquals("a%01b", encoded);
        assertEquals("ab", ZooKeeperKeys.decodeSegment(encoded));
        assertDoesNotThrow(() -> PathUtils.validatePath("/cp/" + encoded));
    }

    /** Non-ASCII goes through as UTF-8 bytes, so a multi-byte character is several escapes. */
    @Test
    void nonAsciiIsEncodedAsUtf8() {
        assertEquals("%C3%BC", ZooKeeperKeys.encodeSegment("ü"));
        assertEquals("ü", ZooKeeperKeys.decodeSegment("%C3%BC"));
        // A surrogate pair is four UTF-8 bytes; ZooKeeper rejects the raw form.
        assertEquals("😀", ZooKeeperKeys.decodeSegment(ZooKeeperKeys.encodeSegment("😀")));
    }

    /** Splitting keeps empty segments, so a/ and a stay different keys. */
    @Test
    void splittingKeepsEmptySegments() {
        assertEquals(List.of("a", "", "b"), ZooKeeperKeys.split("a//b"));
        assertEquals(List.of("a", ""), ZooKeeperKeys.split("a/"));
        assertEquals(List.of("a"), ZooKeeperKeys.split("a"));
        assertEquals(List.of(""), ZooKeeperKeys.split(""));
    }

    @Test
    void aKeyBecomesAPathUnderTheRoot() {
        assertEquals("/cp/tenant/default/indices/my-index",
            ZooKeeperKeys.pathOf("/cp", "tenant/default/indices/my-index"));
    }

    /** Sibling keys must not become parent and child, or scanPrefix would over-match. */
    @Test
    void similarKeysStaySiblings() {
        assertEquals("/cp/ingest/pipelinex", ZooKeeperKeys.pathOf("/cp", "ingest/pipelinex"));
        assertEquals("/cp/ingest/pipeline/a", ZooKeeperKeys.pathOf("/cp", "ingest/pipeline/a"));
        assertTrue(ZooKeeperKeys.pathOf("/cp", "ingest/pipelinex")
            .startsWith("/cp/ingest/pipeline") , "sanity: they share a textual prefix");
    }
}
