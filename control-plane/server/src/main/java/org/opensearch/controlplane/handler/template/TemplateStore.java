/*
 * Copyright OpenSearch Software Foundation
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.controlplane.handler.template;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.opensearch.controlplane.metadata.Settings;
import org.opensearch.controlplane.rest.Json;
import org.opensearch.controlplane.rest.NameExpression;
import org.opensearch.controlplane.store.KeyValueStore;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.NavigableMap;
import java.util.Optional;
import java.util.TreeMap;
import java.util.regex.Pattern;

/**
 * Template definitions in the key/value store, one document per template.
 *
 * <p>Settings are stored in the one canonical form — flat dotted keys, string values — rather than as
 * the client sent them, because that is what a real cluster does: {@code {"number_of_shards": 1}} in a
 * template of any of the three kinds comes back as {@code {"index": {"number_of_shards": "1"}}}. See
 * {@link Settings}.
 *
 * <p>Normalising here rather than in {@link PutTemplateHandler} makes it a property of the store, so
 * neither a future write path nor {@link TemplateComposition} can read a definition that skipped it.
 * The nesting a client sees is applied on the way out by {@link #rendered}.
 */
final class TemplateStore {

    private final KeyValueStore store;
    private final TemplateKind kind;

    TemplateStore(KeyValueStore store, TemplateKind kind) {
        this.store = store;
        this.kind = kind;
    }

    String key(String name) {
        return kind.keyPrefix() + name;
    }

    Optional<ObjectNode> get(String name) {
        return store.get(key(name)).map(versioned -> (ObjectNode) parse(versioned.value()));
    }

    void put(String name, ObjectNode definition) {
        store.put(key(name), Json.toBytes(canonical(definition)));
    }

    /** A definition with its settings normalised. Copies rather than mutating the caller's node. */
    private ObjectNode canonical(ObjectNode definition) {
        ObjectNode copy = definition.deepCopy();
        ObjectNode block = kind.blockOf(copy);
        if (block != null && block.has("settings")) {
            block.set("settings", Settings.forIndex(block.get("settings")).toJson(true));
        }
        return copy;
    }

    /**
     * A stored definition rendered for the wire.
     *
     * <p>Nested settings by default. {@code flat_settings=true} leaves them dotted, but only for the
     * kinds that honour it — legacy does, the other two ignore the parameter. See {@link TemplateKind}.
     */
    ObjectNode rendered(ObjectNode definition, boolean flatSettings) {
        ObjectNode block = kind.blockOf(definition);
        if (block == null || block.has("settings") == false) {
            return definition;
        }
        boolean flat = flatSettings && kind.honoursFlatSettings();
        ObjectNode copy = definition.deepCopy();
        kind.blockOf(copy).set("settings", Settings.forIndex(block.get("settings")).toJson(flat));
        return copy;
    }

    boolean delete(String name) {
        return store.delete(key(name));
    }

    NavigableMap<String, ObjectNode> all() {
        NavigableMap<String, ObjectNode> templates = new TreeMap<>();
        store.scanPrefix(kind.keyPrefix()).forEach((key, versioned) ->
            templates.put(key.substring(kind.keyPrefix().length()), (ObjectNode) parse(versioned.value())));
        return templates;
    }

    /**
     * Resolves a name expression: comma-separated, {@code *} wildcards, or {@code null} for everything.
     *
     * <p>Same shape as pipeline ID resolution, deliberately: a client that has learned how one list
     * parameter behaves should not have to relearn it per resource.
     */
    NavigableMap<String, ObjectNode> resolve(String expression) {
        if (expression == null || expression.isBlank() || "*".equals(expression)) {
            return all();
        }
        NavigableMap<String, ObjectNode> matched = new TreeMap<>();
        NavigableMap<String, ObjectNode> everything = null;
        for (String token : expression.split(",")) {
            String name = token.trim();
            if (name.isEmpty()) {
                continue;
            }
            if (name.indexOf('*') >= 0) {
                if (everything == null) {
                    everything = all();
                }
                Pattern pattern = NameExpression.toPattern(name);
                everything.forEach((candidate, definition) -> {
                    if (pattern.matcher(candidate).matches()) {
                        matched.put(candidate, definition);
                    }
                });
            } else {
                get(name).ifPresent(definition -> matched.put(name, definition));
            }
        }
        return matched;
    }

    private static JsonNode parse(byte[] value) {
        try {
            return Json.parse(value);
        } catch (IOException e) {
            throw new UncheckedIOException("corrupt template definition", e);
        }
    }
}
