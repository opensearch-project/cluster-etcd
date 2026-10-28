/*
 * Copyright OpenSearch Software Foundation
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.controlplane.codegen;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import org.yaml.snakeyaml.LoaderOptions;

import java.io.IOException;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

/**
 * The parsed contents of {@code spec/control-plane-scope.yaml}: the decision
 * about which OpenSearch operation groups this service implements.
 */
record Scope(
    SemanticVersion targetVersion,
    String targetVersionRaw,
    Map<String, String> excludedNamespaces,
    Set<String> reviewedNamespaces,
    Set<String> includedOperations,
    Map<String, String> excludedOperations
) {

    static Scope load(Path file) throws IOException {
        JsonNode root = yamlMapper().readTree(file.toFile());

        JsonNode version = root.get("target_opensearch_version");
        if (version == null || version.asText("").isBlank()) {
            throw new IllegalArgumentException(
                "scope file must set target_opensearch_version: it decides both which operations the "
                    + "route table includes and what GET / reports as version.number"
            );
        }
        String targetVersionRaw = version.asText().trim();

        return new Scope(
            SemanticVersion.parse(targetVersionRaw),
            targetVersionRaw,
            readStringMap(root, "excluded_namespaces"),
            readStringSet(root, "reviewed_namespaces"),
            readStringSet(root, "included_operations"),
            readStringMap(root, "excluded_operations")
        );
    }

    static ObjectMapper yamlMapper() {
        LoaderOptions loaderOptions = new LoaderOptions();
        // The bundled OpenSearch spec is ~3 MB, which is right at snakeyaml's
        // default code point limit. Raise it so a growing spec does not
        // mysteriously start failing to parse.
        loaderOptions.setCodePointLimit(64 * 1024 * 1024);
        return new ObjectMapper(YAMLFactory.builder().loaderOptions(loaderOptions).build());
    }

    private static Map<String, String> readStringMap(JsonNode root, String field) {
        Map<String, String> result = new LinkedHashMap<>();
        JsonNode node = root.get(field);
        if (node == null || node.isNull()) {
            return result;
        }
        if (!node.isObject()) {
            throw new IllegalArgumentException("scope field [" + field + "] must be a mapping of name -> reason");
        }
        node.properties().forEach(entry -> result.put(entry.getKey(), entry.getValue().asText("")));
        return result;
    }

    private static Set<String> readStringSet(JsonNode root, String field) {
        Set<String> result = new LinkedHashSet<>();
        JsonNode node = root.get(field);
        if (node == null || node.isNull()) {
            return result;
        }
        if (!node.isArray()) {
            throw new IllegalArgumentException("scope field [" + field + "] must be a list");
        }
        for (JsonNode element : node) {
            String value = element.asText("");
            if (!result.add(value)) {
                throw new IllegalArgumentException("scope field [" + field + "] lists [" + value + "] more than once");
            }
        }
        return result;
    }

    boolean isIncluded(String operationGroup) {
        return includedOperations.contains(operationGroup);
    }
}
