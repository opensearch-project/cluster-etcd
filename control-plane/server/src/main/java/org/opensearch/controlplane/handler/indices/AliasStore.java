/*
 * Copyright OpenSearch Software Foundation
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.controlplane.handler.indices;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.opensearch.controlplane.resolver.IndexAbstractions;
import org.opensearch.controlplane.rest.Json;
import org.opensearch.controlplane.store.KeyValueStore;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.NavigableMap;
import java.util.Optional;
import java.util.TreeMap;

/**
 * Alias definitions in the key/value store: one document per alias, at {@code aliases/<name>}.
 *
 * <p>Each document holds the indices the alias points at, with that alias's properties <em>per index</em>
 * — filter, routing, {@code is_write_index}, {@code is_hidden} — because OpenSearch scopes them that
 * way. The same alias can carry a different filter on each of its indices.
 *
 * <pre>{@code
 * aliases/logs -> {"indices": {"logs-v2": {"is_write_index": true}, "logs-v1": {}}}
 * }</pre>
 *
 * <p>The layout is what makes the canonical swap atomic. Moving {@code logs} from {@code logs-v1} to
 * {@code logs-v2} rewrites one document, so a single compare-and-swap covers it and no reader can
 * observe the alias pointing at nothing. Holding aliases inside index metadata would have made the same
 * swap two writes to two documents with a visible gap between them.
 *
 * <p>An alias whose last index is removed is deleted outright rather than left as an empty document, so
 * "exists" means "points at something".
 */
final class AliasStore {

    static final String KEY_PREFIX = IndexAbstractions.ALIAS_KEY_PREFIX;

    /** The member map inside an alias document. */
    static final String INDICES = "indices";

    private final KeyValueStore store;

    AliasStore(KeyValueStore store) {
        this.store = store;
    }

    static String key(String alias) {
        return KEY_PREFIX + alias;
    }

    Optional<ObjectNode> get(String alias) {
        return store.get(key(alias)).map(versioned -> (ObjectNode) parse(versioned.value()));
    }

    /** Every alias, in name order. */
    NavigableMap<String, ObjectNode> all() {
        NavigableMap<String, ObjectNode> aliases = new TreeMap<>();
        store.scanPrefix(KEY_PREFIX).forEach((key, versioned) ->
            aliases.put(key.substring(KEY_PREFIX.length()), (ObjectNode) parse(versioned.value())));
        return aliases;
    }

    /**
     * The aliases naming a given index.
     *
     * <p>This is the direction that still costs a scan. Alias reads are index-keyed even when queried by
     * alias name — {@code GET /_alias/x} answers {@code {"index": {"aliases": {"x": {}}}}} — so the
     * reverse lookup cannot be avoided by choosing a different key layout, only by denormalising, which
     * would reintroduce a second source of truth.
     */
    NavigableMap<String, ObjectNode> forIndex(String index) {
        NavigableMap<String, ObjectNode> matched = new TreeMap<>();
        all().forEach((alias, definition) -> {
            JsonNode properties = definition.path(INDICES).get(index);
            if (properties != null) {
                matched.put(alias, (ObjectNode) properties);
            }
        });
        return matched;
    }

    /** Writes the definition, or deletes it when no indices remain. */
    void put(String alias, ObjectNode definition) {
        if (definition.path(INDICES).isEmpty()) {
            store.delete(key(alias));
            return;
        }
        store.put(key(alias), Json.toBytes(definition));
    }

    boolean delete(String alias) {
        return store.delete(key(alias));
    }

    static ObjectNode emptyDefinition() {
        ObjectNode definition = Json.object();
        definition.putObject(INDICES);
        return definition;
    }

    private static JsonNode parse(byte[] value) {
        try {
            return Json.parse(value);
        } catch (IOException e) {
            throw new UncheckedIOException("corrupt alias definition", e);
        }
    }
}
