/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.clusterless.contract;

import org.opensearch.common.xcontent.XContentHelper;
import org.opensearch.common.xcontent.XContentType;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Reading the documents the supervisor and the control plane write, tolerantly.
 *
 * <p><b>Parsed to a map rather than to a typed object graph</b>, and then walked. That is not laziness: the
 * contract's forward-compatibility rule is that <em>unknown fields are ignored</em>, because a rolling upgrade
 * routinely puts one side ahead of the other and failing a parse loses the whole document. A map-based reader
 * ignores what it does not ask for by construction, which is the behaviour that has to be true; a strict
 * binder has to be talked out of the opposite.
 *
 * <p>Every accessor here answers the same question in the same way: <b>absent and wrong-typed are the same
 * thing as far as a reader is concerned</b> — both mean "this document does not tell me", and both get the
 * caller's default. A field the caller genuinely cannot proceed without is fetched with a {@code require}
 * variant, which throws {@link MalformedDocumentException} and lets the caller decide what a bad document
 * costs.
 */
final class Documents {

    private Documents() {}

    /**
     * @throws MalformedDocumentException if the bytes are not a JSON object at all
     */
    static Map<String, Object> parse(byte[] bytes, String what) {
        if (bytes == null || bytes.length == 0) {
            // An empty value is a legal thing for a store to hold -- the contract requires it to be
            // distinguishable from an absent key -- but it is not a document.
            throw new MalformedDocumentException(what, "the value is empty");
        }
        try {
            return XContentHelper.convertToMap(XContentType.JSON.xContent(), bytes, 0, bytes.length, false);
        } catch (Exception e) {
            throw new MalformedDocumentException(what, e);
        }
    }

    @SuppressWarnings("unchecked")
    static Map<String, Object> object(Map<String, Object> parent, String field) {
        Object value = parent.get(field);
        return value instanceof Map ? (Map<String, Object>) value : Map.of();
    }

    static String string(Map<String, Object> parent, String field, String fallback) {
        Object value = parent.get(field);
        return value == null ? fallback : String.valueOf(value);
    }

    static String requireString(Map<String, Object> parent, String field, String what) {
        Object value = parent.get(field);
        if (value == null || String.valueOf(value).isEmpty()) {
            throw new MalformedDocumentException(what, "[" + field + "] is missing");
        }
        return String.valueOf(value);
    }

    /**
     * A number, from either a JSON number or a string holding one.
     *
     * <p>Both forms occur and neither is a mistake: the supervisor writes {@code config_revision} as a number,
     * while the control plane's index settings are <b>flat and entirely string-valued</b> — {@code "3"}, never
     * {@code 3}. Reading {@code "3"} with something that expects a number is the silent-zero bug
     * {@code KEY_LAYOUT.md} warns about, so this accepts both rather than trusting either.
     */
    static long number(Map<String, Object> parent, String field, long fallback, String what) {
        Object value = parent.get(field);
        if (value == null) {
            return fallback;
        }
        if (value instanceof Number number) {
            return number.longValue();
        }
        try {
            return Long.parseLong(String.valueOf(value).trim());
        } catch (NumberFormatException e) {
            throw new MalformedDocumentException(what, "[" + field + "] is not a number: [" + value + "]");
        }
    }

    /**
     * A map whose values are all strings, which is the shape the control plane's settings block has.
     *
     * <p>Values are stringified rather than rejected when they are not strings. The control plane promises
     * strings and a hand-edited document might not deliver them, and {@code Settings.builder().loadFromMap}
     * would take either — refusing here would fail an index over something the server would have accepted.
     */
    static Map<String, String> stringMap(Map<String, Object> parent, String field) {
        Map<String, Object> raw = object(parent, field);
        if (raw.isEmpty()) {
            return Map.of();
        }
        Map<String, String> flat = new LinkedHashMap<>(raw.size());
        raw.forEach((key, value) -> {
            if (value != null) {
                flat.put(key, String.valueOf(value));
            }
        });
        return Collections.unmodifiableMap(flat);
    }
}
