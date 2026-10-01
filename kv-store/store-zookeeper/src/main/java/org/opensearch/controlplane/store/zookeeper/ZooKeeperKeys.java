/*
 * Copyright OpenSearch Software Foundation
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.controlplane.store.zookeeper;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/**
 * Maps control plane keys onto ZooKeeper paths, one key segment to one znode.
 *
 * <h2>Why hierarchical, where the filesystem store is flat</h2>
 *
 * <p>{@code FileSystemKeyValueStore} deliberately flattens the whole key into one filename, and gives a
 * good reason: keys carry user input, so a segment of {@code ..} would escape the directory. <b>That
 * argument does not transfer to ZooKeeper.</b> ZooKeeper does not resolve {@code .} or {@code ..} — its
 * {@code PathUtils.validatePath} rejects them outright — so there is no traversal to defend against,
 * only a name to encode.
 *
 * <p>What does transfer is the cost of a flat layout: {@code scanPrefix} would have to list every key in
 * the store on every call. That is fine for a directory of a few hundred files and not fine here, because
 * resolving a single index wildcard goes through {@code scanPrefix}. Keeping the hierarchy means a scan
 * lists one parent instead of the world.
 *
 * <h2>The encoding</h2>
 *
 * <p>Per segment, percent-encoding of everything outside RFC 3986's unreserved set
 * {@code [A-Za-z0-9._~-]}, over UTF-8 bytes. So ordinary names stay readable — {@code my-index} and
 * {@code logs.2026} are unchanged — while anything awkward becomes an escape.
 *
 * <p>Two cases the general rule does not cover, because ZooKeeper forbids the result:
 *
 * <ul>
 *   <li><b>A segment of only dots.</b> {@code .} and {@code ..} are illegal znode names, so the first
 *       dot is escaped: {@code ..} becomes {@code %2E.}. Escaping only the first keeps the common
 *       {@code a.b} case untouched, and is enough because a name starting {@code %} can never be read as
 *       a dot-name.</li>
 *   <li><b>An empty segment.</b> Illegal as a znode name — ZooKeeper has no {@code //} — and reachable
 *       from a key like {@code a//b}. Encoded as a lone {@code %}, which is unambiguous because every
 *       escape this produces is a {@code %} followed by exactly two hex digits.</li>
 * </ul>
 *
 * <p>The encoding is total and reversible: {@link #decodeSegment} of {@link #encodeSegment} is the
 * identity for every input, which is what {@code ZooKeeperKeysTests} checks over the awkward cases the
 * store contract uses.
 */
final class ZooKeeperKeys {

    /** RFC 3986 unreserved characters, all legal in a znode name and all left alone. */
    private static final String UNRESERVED =
        "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_.~";

    private static final char ESCAPE = '%';

    /** A lone {@code %}: the encoding of an empty segment. */
    private static final String EMPTY_SEGMENT = "%";

    private static final char[] HEX = "0123456789ABCDEF".toCharArray();

    private ZooKeeperKeys() {}

    /**
     * The znode path for a key, beneath {@code root}.
     *
     * <p>A key never ends up with a trailing slash in its path, and a key of {@code ""} maps to the root
     * itself — which cannot hold a value, so the store rejects an empty key rather than writing there.
     */
    static String pathOf(String root, String key) {
        StringBuilder path = new StringBuilder(root);
        for (String segment : split(key)) {
            path.append('/').append(encodeSegment(segment));
        }
        return path.toString();
    }

    /**
     * Splits a key on {@code /}, keeping empty segments.
     *
     * <p>{@code String.split} drops trailing empties, which would silently make {@code a/} and {@code a}
     * the same key. They are different keys and have to stay different.
     */
    static List<String> split(String key) {
        List<String> segments = new ArrayList<>();
        int start = 0;
        for (int i = 0; i < key.length(); i++) {
            if (key.charAt(i) == '/') {
                segments.add(key.substring(start, i));
                start = i + 1;
            }
        }
        segments.add(key.substring(start));
        return segments;
    }

    static String encodeSegment(String segment) {
        if (segment.isEmpty()) {
            return EMPTY_SEGMENT;
        }

        StringBuilder encoded = new StringBuilder(segment.length() + 8);
        byte[] utf8 = segment.getBytes(StandardCharsets.UTF_8);
        for (byte b : utf8) {
            char c = (char) (b & 0xFF);
            if (b >= 0 && UNRESERVED.indexOf(c) >= 0) {
                encoded.append(c);
            } else {
                encoded.append(ESCAPE).append(HEX[(b >> 4) & 0xF]).append(HEX[b & 0xF]);
            }
        }

        // Only reached for a segment that is entirely dots, since a dot is unreserved and survives the
        // loop untouched. Escaping the first one is enough to stop it being a dot-name.
        if (isAllDots(encoded)) {
            return "%2E" + encoded.substring(1);
        }
        return encoded.toString();
    }

    static String decodeSegment(String encoded) {
        if (encoded.equals(EMPTY_SEGMENT)) {
            return "";
        }
        java.io.ByteArrayOutputStream decoded = new java.io.ByteArrayOutputStream(encoded.length());
        for (int i = 0; i < encoded.length(); i++) {
            char c = encoded.charAt(i);
            if (c == ESCAPE) {
                if (i + 2 >= encoded.length()) {
                    throw new IllegalArgumentException("truncated escape in znode name [" + encoded + "]");
                }
                decoded.write((hex(encoded.charAt(i + 1)) << 4) | hex(encoded.charAt(i + 2)));
                i += 2;
            } else {
                decoded.write(c);
            }
        }
        return new String(decoded.toByteArray(), StandardCharsets.UTF_8);
    }

    private static boolean isAllDots(CharSequence value) {
        for (int i = 0; i < value.length(); i++) {
            if (value.charAt(i) != '.') {
                return false;
            }
        }
        return value.length() > 0;
    }

    private static int hex(char c) {
        int value = Character.digit(c, 16);
        if (value < 0) {
            throw new IllegalArgumentException("bad escape character [" + c + "] in znode name");
        }
        return value;
    }
}
