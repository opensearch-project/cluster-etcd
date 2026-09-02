/*
 * Copyright OpenSearch Software Foundation
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.controlplane.capability;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;

import java.io.IOException;
import java.nio.file.Path;
import java.util.EnumMap;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;

/**
 * Capabilities read from a file the deployer supplies.
 *
 * <p>Expected shape, with one list per {@link ProcessorKind}:
 *
 * <pre>{@code
 * processors:
 *   ingest:               [append, bytes, convert, ...]
 *   search_request:       [filter_query, oversample, ...]
 *   search_response:      [collapse, rename_field, sort]
 *   search_phase_results: []
 * }</pre>
 *
 * <p>Generate it from the cluster this control plane fronts rather than writing it by hand:
 * {@code tools/dump-capabilities.sh http://localhost:9200 > capabilities.yaml}. A hand-written list
 * drifts, and drift here means rejecting definitions that would have worked.
 *
 * <p>An omitted kind is an <em>empty</em> set, not an unconstrained one: a cluster with no plugins
 * genuinely reports {@code phase_results_processors: []}, so "absent" and "none" have to mean the same
 * thing for the file to be able to express that at all. A deployer who wants a kind unchecked should
 * not be declaring capabilities for it.
 */
public final class DeclaredCapabilities implements DataPlaneCapabilities {

    private final Map<ProcessorKind, Set<String>> processors;

    public DeclaredCapabilities(Map<ProcessorKind, Set<String>> processors) {
        EnumMap<ProcessorKind, Set<String>> copy = new EnumMap<>(ProcessorKind.class);
        for (ProcessorKind kind : ProcessorKind.values()) {
            copy.put(kind, Set.copyOf(processors.getOrDefault(kind, Set.of())));
        }
        this.processors = copy;
    }

    public static DeclaredCapabilities load(Path file) throws IOException {
        ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
        JsonNode root = mapper.readTree(file.toFile());
        if (root == null || root.isObject() == false) {
            throw new IOException("capabilities file " + file + " is not a YAML mapping");
        }

        JsonNode processorsNode = root.path("processors");
        if (processorsNode.isObject() == false) {
            throw new IOException(
                "capabilities file " + file + " must have a `processors` mapping; "
                    + "generate one with tools/dump-capabilities.sh"
            );
        }

        Map<ProcessorKind, Set<String>> declared = new EnumMap<>(ProcessorKind.class);
        for (Map.Entry<String, JsonNode> entry : processorsNode.properties()) {
            ProcessorKind kind = ProcessorKind.fromConfigKey(entry.getKey());
            if (entry.getValue().isArray() == false) {
                throw new IOException("processors." + entry.getKey() + " must be a list of type names");
            }
            Set<String> types = new TreeSet<>();
            entry.getValue().forEach(type -> types.add(type.asText()));
            declared.put(kind, types);
        }
        return new DeclaredCapabilities(declared);
    }

    @Override
    public boolean supportsProcessor(ProcessorKind kind, String type) {
        return processors.get(kind).contains(type);
    }

    @Override
    public boolean isEnforcing() {
        return true;
    }

    @Override
    public Set<String> supportedProcessors(ProcessorKind kind) {
        return processors.get(kind);
    }

    @Override
    public String toString() {
        return "enforcing ("
            + processors.entrySet().stream()
                .map(e -> e.getKey().configKey() + "=" + e.getValue().size())
                .collect(Collectors.joining(", "))
            + " processor types declared)";
    }
}
