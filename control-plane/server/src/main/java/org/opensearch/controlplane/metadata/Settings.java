/*
 * Copyright OpenSearch Software Foundation
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.controlplane.metadata;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.NullNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TextNode;
import org.opensearch.controlplane.rest.Json;

import java.util.Map;
import java.util.NavigableMap;
import java.util.Objects;
import java.util.Optional;
import java.util.TreeMap;

/**
 * A settings document, in the one representation OpenSearch actually keeps.
 *
 * <p>There is exactly one, and it is not any of the three shapes a client may send: a sorted map from
 * <b>dotted key</b> to <b>string</b>. Everything normalises into it on the way in, and the shapes seen
 * on the wire are renderings chosen on the way out. Verified against OpenSearch 3.9.0 — all three of
 * these store {@code index.number_of_shards = "2"}, and all three come back identically:
 *
 * <pre>
 *   {"number_of_shards": 2}                  unprefixed, native type
 *   {"index.number_of_shards": 2}            dotted
 *   {"index": {"number_of_shards": 2}}       nested
 * </pre>
 *
 * <p>The same normalisation applies to <em>templates</em> — index, component and legacy alike — not
 * just to index metadata, which is why this is shared rather than living in one handler. The
 * server's own error message is the tell: sending {@code {"totally_bogus": 1}} is rejected as
 * {@code unknown setting [index.totally_bogus]}, so the key was normalised before it was validated.
 *
 * <h2>Value rules, all verified</h2>
 *
 * <ul>
 *   <li>Numbers and booleans are <b>stringified</b>: {@code 2} becomes {@code "2"}, {@code true}
 *       becomes {@code "true"}.</li>
 *   <li><b>Lists stay lists of strings</b> in both renderings —
 *       {@code index.analysis.analyzer.my.filter: ["lowercase", "stop"]}. So a value is a string or a
 *       list of them, which is why this holds {@link JsonNode} rather than {@link String}.</li>
 *   <li><b>{@code null} stays a real JSON null</b>, not the string {@code "null"}.</li>
 *   <li>A <b>dotted key beats the nested spelling</b> of the same setting, and does so regardless of
 *       which appeared first in the document — it is not last-writer-wins. {@link #flatten} reproduces
 *       that by descending into objects before applying leaves.</li>
 * </ul>
 *
 * <h2>Why flat is the stored form</h2>
 *
 * <p>Because merging is the operation that matters. Composing a template means overriding settings key
 * by key, and on a flat map that is a map {@code put} — exact, and independent of the order the keys
 * were written in. A structural merge over the nested form only gets it right when both sides happen to
 * spell a setting the same way; a component saying {@code {"index": {"number_of_replicas": 9}}} beneath
 * a template block saying {@code {"number_of_replicas": 5}} yields two keys rather than one.
 *
 * <h2>One deliberate divergence: key order</h2>
 *
 * <p>Rendering nested, this emits keys in sorted order. OpenSearch's nested rendering goes through a
 * {@code HashMap} ({@code Settings.getAsStructuredMap}), so its order is neither sorted nor the order
 * the settings were supplied in — reproducing it would mean reproducing Java's hash iteration order.
 * JSON object order is not semantic and the conformance harness compares parsed structures, so sorted
 * is the better answer. Recorded in the README's considered divergences. The flat rendering is sorted
 * on a real cluster too, so that one agrees.
 */
public final class Settings {

    public static final Settings EMPTY = new Settings(new TreeMap<>());

    /** Index settings are implicitly under {@code index.} when sent without a prefix. */
    private static final String INDEX_PREFIX = "index.";

    private final NavigableMap<String, JsonNode> values;

    private Settings(NavigableMap<String, JsonNode> values) {
        this.values = values;
    }

    /**
     * Normalises a settings document written in any of the accepted shapes.
     *
     * <p>Named for the scope rather than being general, because the implicit {@code index.} prefix is
     * specific to index settings: cluster settings have no such prefix and will want their own factory.
     *
     * <p>Idempotent, so re-reading an already-normalised document — which is what every read of stored
     * metadata does — is a no-op rather than a second round of prefixing.
     *
     * @param settings the {@code settings} value; anything that is not an object gives {@link #EMPTY}
     */
    public static Settings forIndex(JsonNode settings) {
        NavigableMap<String, JsonNode> flat = new TreeMap<>();
        flatten("", settings, flat);
        if (flat.isEmpty()) {
            return EMPTY;
        }
        NavigableMap<String, JsonNode> prefixed = new TreeMap<>();
        flat.forEach((key, value) -> prefixed.put(key.startsWith(INDEX_PREFIX) ? key : INDEX_PREFIX + key, value));
        return new Settings(prefixed);
    }

    /**
     * Renders back to JSON.
     *
     * @param flat dotted keys, as {@code flat_settings=true} asks for; otherwise nested under each
     *             dot-separated segment, which is what a cluster returns by default
     */
    public ObjectNode toJson(boolean flat) {
        ObjectNode out = Json.object();
        if (flat) {
            values.forEach((key, value) -> out.set(key, value.deepCopy()));
            return out;
        }
        values.forEach((key, value) -> {
            String[] segments = key.split("\\.");
            ObjectNode cursor = out;
            for (int i = 0; i < segments.length - 1; i++) {
                JsonNode next = cursor.get(segments[i]);
                // A leaf where a branch is needed means two settings whose keys prefix one another,
                // which a real cluster rejects at validation. Deepen rather than throw: we do not
                // validate, and dropping the more specific key would be the worse of the two answers.
                cursor = next instanceof ObjectNode object ? object : cursor.putObject(segments[i]);
            }
            cursor.set(segments[segments.length - 1], value.deepCopy());
        });
        return out;
    }

    /** This document with {@code override} applied key by key; the override wins. */
    public Settings mergedWith(Settings override) {
        if (override.values.isEmpty()) {
            return this;
        }
        if (values.isEmpty()) {
            return override;
        }
        NavigableMap<String, JsonNode> merged = new TreeMap<>(values);
        merged.putAll(override.values);
        return new Settings(merged);
    }

    /** This document with {@code key} set to {@code value} if it is not already present. */
    public Settings withDefault(String key, String value) {
        if (values.containsKey(key)) {
            return this;
        }
        NavigableMap<String, JsonNode> extended = new TreeMap<>(values);
        extended.put(key, TextNode.valueOf(value));
        return new Settings(extended);
    }

    /**
     * A single-valued setting.
     *
     * <p>Empty for an absent setting and for an explicit null, which are the same thing to a reader:
     * OpenSearch treats a null as "unset" and falls back to the default.
     */
    public Optional<String> get(String key) {
        JsonNode value = values.get(key);
        return value == null || value.isNull() ? Optional.empty() : Optional.of(value.asText());
    }

    /** Values are stored as text, so a reader that wants a boolean parses one. */
    public boolean getAsBoolean(String key, boolean defaultValue) {
        return get(key).map(Boolean::parseBoolean).orElse(defaultValue);
    }

    public boolean isEmpty() {
        return values.isEmpty();
    }

    /**
     * Flattens one level of a settings document into dotted keys.
     *
     * <p>Objects are descended into before leaves are applied, which is what makes a dotted key beat
     * the nested spelling of the same setting in either document order — a dotted key is always a leaf.
     */
    private static void flatten(String prefix, JsonNode node, Map<String, JsonNode> out) {
        if (node == null || node.isObject() == false) {
            return;
        }
        node.properties().forEach(entry -> {
            if (entry.getValue().isObject()) {
                flatten(prefix + entry.getKey() + ".", entry.getValue(), out);
            }
        });
        node.properties().forEach(entry -> {
            if (entry.getValue().isObject() == false) {
                out.put(prefix + entry.getKey(), normalizeValue(entry.getValue()));
            }
        });
    }

    private static JsonNode normalizeValue(JsonNode value) {
        if (value.isNull()) {
            return NullNode.getInstance();
        }
        if (value.isArray()) {
            ArrayNode strings = Json.array();
            value.forEach(element -> {
                if (element.isNull()) {
                    strings.addNull();
                } else if (element.isValueNode()) {
                    strings.add(element.asText());
                } else {
                    // A list of objects is not a setting a cluster would accept. Keep it verbatim as
                    // text rather than silently dropping it, so the rejection stays the server's to make.
                    strings.add(Json.write(element, false));
                }
            });
            return strings;
        }
        return TextNode.valueOf(value.asText());
    }

    @Override
    public boolean equals(Object other) {
        return other instanceof Settings settings && values.equals(settings.values);
    }

    @Override
    public int hashCode() {
        return Objects.hash(values);
    }

    @Override
    public String toString() {
        return values.toString();
    }
}
