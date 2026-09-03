/*
 * Copyright OpenSearch Software Foundation
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.controlplane.handler.template;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.opensearch.controlplane.rest.Json;
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
 * <p>Stored verbatim. A real cluster normalises settings on the way in — nesting them under
 * {@code index} and stringifying values, so {@code {"number_of_shards": 1}} comes back as
 * {@code {"index": {"number_of_shards": "1"}}} — and this does not. That divergence is recorded in the
 * README rather than fixed here, because it is not specific to templates: index settings have the same
 * question, and it wants solving once for both.
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
        store.put(key(name), Json.toBytes(definition));
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
                Pattern pattern = globToPattern(name);
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

    private static Pattern globToPattern(String glob) {
        StringBuilder regex = new StringBuilder(glob.length() + 8);
        int literalStart = 0;
        for (int i = 0; i < glob.length(); i++) {
            if (glob.charAt(i) == '*') {
                if (i > literalStart) {
                    regex.append(Pattern.quote(glob.substring(literalStart, i)));
                }
                regex.append(".*");
                literalStart = i + 1;
            }
        }
        if (literalStart < glob.length()) {
            regex.append(Pattern.quote(glob.substring(literalStart)));
        }
        return Pattern.compile(regex.toString());
    }

    private static JsonNode parse(byte[] value) {
        try {
            return Json.parse(value);
        } catch (IOException e) {
            throw new UncheckedIOException("corrupt template definition", e);
        }
    }
}
