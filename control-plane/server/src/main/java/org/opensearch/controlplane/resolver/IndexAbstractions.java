/*
 * Copyright OpenSearch Software Foundation
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.controlplane.resolver;

import com.fasterxml.jackson.databind.JsonNode;
import org.opensearch.controlplane.metadata.Settings;
import org.opensearch.controlplane.rest.Json;
import org.opensearch.controlplane.store.KeyValueStore;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;

/**
 * The name lookup an expression resolves against: every index and alias, keyed by name.
 *
 * <p>OpenSearch keeps this prebuilt in cluster state; here it is assembled from two prefix scans, one
 * per resource kind. Neither scan needs to cross-reference the other, and resolving a single alias by
 * name is a direct key read rather than a scan of every index.
 */
public final class IndexAbstractions {

    /** Key layout for index metadata. Shared so the resolver and the indices handlers agree. */
    public static final String INDEX_KEY_PREFIX = "indices/";

    /**
     * Key layout for alias definitions, one document per alias.
     *
     * <p>Aliases are their own resource rather than fields inside index metadata, which is what makes
     * the canonical swap — move alias {@code x} from index A to index B — a single-key compare-and-swap
     * instead of two writes to two documents. See the README.
     */
    public static final String ALIAS_KEY_PREFIX = "aliases/";

    private final NavigableMap<String, IndexAbstraction> lookup;

    public IndexAbstractions(Map<String, IndexAbstraction> lookup) {
        // Sorted, not just unmodifiable. Both callers build a TreeMap expecting name order, and
        // Map.copyOf discarded it -- which quietly made wildcard expansion order, and so the order of
        // every response built from one, unspecified.
        this.lookup = Collections.unmodifiableNavigableMap(new TreeMap<>(lookup));
    }

    /** Builds the lookup by scanning stored index metadata. */
    public static IndexAbstractions from(KeyValueStore store) {
        Map<String, IndexAbstraction> combined = new TreeMap<>();

        store.scanPrefix(INDEX_KEY_PREFIX).forEach((key, versioned) -> {
            String name = key.substring(INDEX_KEY_PREFIX.length());
            JsonNode metadata = parse(versioned.value());
            // Through Settings rather than a direct path read: values are stored as text, so
            // JsonNode.asBoolean would read the string "true" as false.
            boolean hidden = Settings.forIndex(metadata.path("settings")).getAsBoolean("index.hidden", false);
            boolean open = "open".equals(metadata.path("state").asText("open"));
            combined.put(name, IndexAbstraction.index(name, hidden, open));
        });

        store.scanPrefix(ALIAS_KEY_PREFIX).forEach((key, versioned) -> {
            String alias = key.substring(ALIAS_KEY_PREFIX.length());
            JsonNode definition = parse(versioned.value());

            List<String> members = new ArrayList<>();
            // Hidden only if every member says so, matching how OpenSearch rejects an alias whose
            // is_hidden disagrees across its indices.
            boolean hidden = true;
            for (Map.Entry<String, JsonNode> member : definition.path("indices").properties()) {
                members.add(member.getKey());
                hidden &= member.getValue().path("is_hidden").asBoolean(false);
            }
            if (members.isEmpty()) {
                return;
            }
            // A concrete index of the same name wins; the collision is rejected at write time anyway.
            combined.putIfAbsent(alias, IndexAbstraction.alias(alias, members, hidden));
        });
        return new IndexAbstractions(combined);
    }

    IndexAbstraction get(String name) {
        return lookup.get(name);
    }

    boolean contains(String name) {
        return lookup.containsKey(name);
    }

    /**
     * Every abstraction, keyed by name, in name order.
     *
     * <p>Public because {@code indices.resolve_index} reports on names directly rather than resolving
     * an expression to concrete indices — see that handler for why it cannot use the resolver.
     */
    public NavigableMap<String, IndexAbstraction> all() {
        return lookup;
    }

    public int size() {
        return lookup.size();
    }

    private static JsonNode parse(byte[] value) {
        try {
            return Json.parse(value);
        } catch (IOException e) {
            throw new UncheckedIOException("corrupt index metadata", e);
        }
    }
}
