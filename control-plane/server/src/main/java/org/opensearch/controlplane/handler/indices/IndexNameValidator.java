/*
 * Copyright OpenSearch Software Foundation
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.controlplane.handler.indices;

import java.nio.charset.StandardCharsets;
import java.util.Optional;

/**
 * Index name validation, reproducing OpenSearch's rules and messages.
 *
 * <p>The <em>order</em> of the checks is part of the contract, not an implementation detail: a name
 * breaking several rules produces exactly one message, and which one it is was determined
 * empirically against OpenSearch 3.9.0 rather than guessed. Confirmed orderings:
 *
 * <ul>
 *   <li>{@code CP a} reports invalid characters, not "must be lowercase"</li>
 *   <li>{@code _cp#a} reports {@code '#'}, not the leading-character rule</li>
 *   <li>{@code cp:#a} reports {@code '#'}, not {@code ':'}</li>
 *   <li>{@code _cp:a} reports {@code ':'}, not the leading-character rule</li>
 *   <li>{@code _CPa} reports the leading-character rule, not "must be lowercase"</li>
 * </ul>
 *
 * <p>So: invalid characters, then {@code #}, then {@code :}, then leading character, then case. The
 * remaining rules (the length cap, then {@code .}/{@code ..}) are ordered from the server source, which
 * also puts case after both: it belongs to {@code validateIndexName} alone, while everything else is
 * {@code validateIndexOrAliasName}, shared with aliases.
 */
public final class IndexNameValidator {

    /**
     * Rendered into the message exactly as OpenSearch renders its {@code INVALID_FILENAME_CHARS}
     * set, including the leading space entry and the unusual ordering — clients matching on the
     * string would otherwise see a difference.
     */
    private static final char[] INVALID_CHARS = { ' ', '"', '*', '\\', '<', '|', ',', '>', '/', '?' };

    private static final String INVALID_CHARS_MESSAGE =
        "must not contain the following characters [ , \", *, \\, <, |, ,, >, /, ?]";

    /** OpenSearch's MAX_INDEX_NAME_BYTES. Measured in UTF-8 bytes, not characters. */
    static final int MAX_NAME_BYTES = 255;

    private IndexNameValidator() {}

    /**
     * @return the reason the name is invalid, or empty if it is acceptable. The caller turns this
     *         into an {@code invalid_index_name_exception}.
     */
    public static Optional<String> validate(String name) {
        Optional<String> invalid = validateIndexOrAliasName(name);
        if (invalid.isPresent()) {
            return invalid;
        }
        // MetadataCreateIndexService.validateIndexName, after the shared rules: aliases may be upper case.
        if (name.toLowerCase(java.util.Locale.ROOT).equals(name) == false) {
            return Optional.of("must be lowercase");
        }
        return Optional.empty();
    }

    /**
     * The rules index and alias names share, in {@code MetadataCreateIndexService.validateIndexOrAliasName}'s
     * order. The length rule says "index name" for an alias too; that is the server's wording.
     *
     * @return the reason the name is invalid, or empty if it is acceptable
     */
    public static Optional<String> validateIndexOrAliasName(String name) {
        if (name == null) {
            return Optional.of("must not be empty");
        }
        for (char invalid : INVALID_CHARS) {
            if (name.indexOf(invalid) >= 0) {
                return Optional.of(INVALID_CHARS_MESSAGE);
            }
        }
        if (name.isEmpty()) {
            return Optional.of("must not be empty");
        }
        if (name.indexOf('#') >= 0) {
            return Optional.of("must not contain '#'");
        }
        if (name.indexOf(':') >= 0) {
            return Optional.of("must not contain ':'");
        }
        char first = name.charAt(0);
        if (first == '_' || first == '-' || first == '+') {
            return Optional.of("must not start with '_', '-', or '+'");
        }
        int byteCount = name.getBytes(StandardCharsets.UTF_8).length;
        if (byteCount > MAX_NAME_BYTES) {
            return Optional.of("index name is too long, (" + byteCount + " > " + MAX_NAME_BYTES + ")");
        }
        if (name.equals(".") || name.equals("..")) {
            return Optional.of("must not be '.' or '..'");
        }
        return Optional.empty();
    }

    /** The full {@code reason} string, as OpenSearch formats it. */
    public static String message(String name, String reason) {
        return "Invalid index name [" + name + "], " + reason;
    }
}
