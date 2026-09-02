/*
 * Copyright OpenSearch Software Foundation
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.controlplane.resolver;

import com.fasterxml.jackson.databind.JsonNode;
import org.opensearch.controlplane.rest.Json;
import org.opensearch.controlplane.store.KeyValueStore;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * The name lookup an expression resolves against: every index and alias, keyed by name.
 *
 * <p>OpenSearch keeps this prebuilt in cluster state. Here it has to be assembled, and because aliases
 * currently live <em>inside</em> index metadata, assembling it means scanning every index. That is the
 * cost of the per-resource-key design showing up on the read side.
 *
 * <p>It gets cheaper once aliases become their own keyed resource, which is already the plan for
 * unrelated reasons — see the README. At that point this becomes two prefix scans that need no
 * cross-referencing, and an alias lookup by name becomes a direct read.
 */
public final class IndexAbstractions {

    /** Key layout for index metadata. Shared so the resolver and the indices handlers agree. */
    public static final String INDEX_KEY_PREFIX = "indices/";

    private final Map<String, IndexAbstraction> lookup;

    public IndexAbstractions(Map<String, IndexAbstraction> lookup) {
        this.lookup = Map.copyOf(lookup);
    }

    /** Builds the lookup by scanning stored index metadata. */
    public static IndexAbstractions from(KeyValueStore store) {
        Map<String, IndexAbstraction> indices = new TreeMap<>();
        Map<String, List<String>> aliasMembers = new TreeMap<>();
        Map<String, Boolean> aliasHidden = new TreeMap<>();

        store.scanPrefix(INDEX_KEY_PREFIX).forEach((key, versioned) -> {
            String name = key.substring(INDEX_KEY_PREFIX.length());
            JsonNode metadata = parse(versioned.value());

            boolean hidden = metadata.path("settings").path("index.hidden").asBoolean(false);
            boolean open = "open".equals(metadata.path("state").asText("open"));
            indices.put(name, IndexAbstraction.index(name, hidden, open));

            metadata.path("aliases").properties().forEach(alias -> {
                aliasMembers.computeIfAbsent(alias.getKey(), k -> new ArrayList<>()).add(name);
                // An alias is hidden only if every definition of it says so, matching how OpenSearch
                // rejects inconsistent is_hidden across an alias's indices.
                boolean thisHidden = alias.getValue().path("is_hidden").asBoolean(false);
                aliasHidden.merge(alias.getKey(), thisHidden, (a, b) -> a && b);
            });
        });

        Map<String, IndexAbstraction> combined = new TreeMap<>(indices);
        aliasMembers.forEach((alias, members) -> {
            // A concrete index of the same name wins; OpenSearch forbids the collision at write time.
            combined.putIfAbsent(alias, IndexAbstraction.alias(alias, members, aliasHidden.get(alias)));
        });
        return new IndexAbstractions(combined);
    }

    IndexAbstraction get(String name) {
        return lookup.get(name);
    }

    boolean contains(String name) {
        return lookup.containsKey(name);
    }

    Map<String, IndexAbstraction> all() {
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
