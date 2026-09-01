/*
 * Copyright OpenSearch Software Foundation
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.controlplane.codegen;

import com.fasterxml.jackson.databind.JsonNode;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.stream.Collectors;

/**
 * Reads the bundled OpenSearch OpenAPI spec plus the control plane scope
 * definition and emits a Java route table.
 *
 * <p>The generator refuses to emit anything unless every namespace and every
 * operation group in a reviewed namespace has been explicitly classified. That
 * is the mechanism that turns "OpenSearch added an API" into a build failure
 * rather than a silent gap.
 */
public final class SpecCodegen {

    private static final List<String> HTTP_METHODS =
        List.of("get", "put", "post", "delete", "head", "patch", "options", "trace");

    private static final String TARGET_PACKAGE = "org.opensearch.controlplane.router";

    private SpecCodegen() {}

    public static void main(String[] args) throws Exception {
        Path specFile = null;
        Path scopeFile = null;
        Path outputDir = null;
        Path reportFile = null;

        for (int i = 0; i < args.length - 1; i++) {
            switch (args[i]) {
                case "--spec" -> specFile = Path.of(args[++i]);
                case "--scope" -> scopeFile = Path.of(args[++i]);
                case "--out" -> outputDir = Path.of(args[++i]);
                case "--report" -> reportFile = Path.of(args[++i]);
                default -> { }
            }
        }
        if (specFile == null || scopeFile == null || outputDir == null) {
            System.err.println("usage: SpecCodegen --spec <openapi.yaml> --scope <scope.yaml> --out <dir> [--report <file>]");
            System.exit(2);
            return;
        }

        JsonNode spec = Scope.yamlMapper().readTree(specFile.toFile());
        Scope scope = Scope.load(scopeFile);

        List<SpecRoute> allRoutes = discoverRoutes(spec);
        Set<String> globalQueryParams = discoverGlobalQueryParams(spec);

        validate(scope, allRoutes);

        List<SpecRoute> inScope = allRoutes.stream()
            .filter(route -> scope.isIncluded(route.operationGroup()))
            // Superseded _opendistro-era paths are kept in the spec because the
            // server still answers them, but a new service has no reason to.
            .filter(route -> route.ignorable() == false)
            .sorted()
            .toList();

        Files.createDirectories(outputDir);
        Path packageDir = outputDir.resolve(TARGET_PACKAGE.replace('.', '/'));
        Files.createDirectories(packageDir);

        String specVersion = spec.path("info").path("version").asText("unknown");
        String source = renderRouteTable(inScope, globalQueryParams, specVersion);
        Files.writeString(packageDir.resolve("GeneratedRoutes.java"), source, StandardCharsets.UTF_8);

        if (reportFile != null) {
            Files.createDirectories(reportFile.getParent());
            Files.writeString(reportFile, renderReport(scope, allRoutes, inScope, specVersion), StandardCharsets.UTF_8);
        }

        long groups = inScope.stream().map(SpecRoute::operationGroup).distinct().count();
        System.out.printf(
            Locale.ROOT,
            "generated %d routes across %d operation groups (of %d routes / %d groups in the spec)%n",
            inScope.size(),
            groups,
            allRoutes.size(),
            allRoutes.stream().map(SpecRoute::operationGroup).distinct().count()
        );
    }

    // ------------------------------------------------------------------
    // Spec traversal
    // ------------------------------------------------------------------

    private static List<SpecRoute> discoverRoutes(JsonNode spec) {
        List<SpecRoute> routes = new ArrayList<>();
        JsonNode paths = spec.path("paths");

        for (Map.Entry<String, JsonNode> pathEntry : paths.properties()) {
            String path = pathEntry.getKey();
            JsonNode pathItem = pathEntry.getValue();

            for (String method : HTTP_METHODS) {
                JsonNode operation = pathItem.get(method);
                if (operation == null || operation.isObject() == false) {
                    continue;
                }
                String group = operation.path("x-operation-group").asText(null);
                if (group == null || group.isEmpty()) {
                    throw new IllegalStateException(
                        "operation " + method.toUpperCase(Locale.ROOT) + " " + path + " has no x-operation-group"
                    );
                }
                routes.add(new SpecRoute(
                    method.toUpperCase(Locale.ROOT),
                    path,
                    group,
                    namespaceOf(group),
                    operation.path("deprecated").asBoolean(false),
                    operation.path("x-ignorable").asBoolean(false),
                    operationQueryParams(spec, operation)
                ));
            }
        }
        return routes;
    }

    /** Operation groups are {@code namespace.action}, except {@code _core} where the namespace is elided. */
    private static String namespaceOf(String operationGroup) {
        int dot = operationGroup.indexOf('.');
        return dot < 0 ? "_core" : operationGroup.substring(0, dot);
    }

    /**
     * Query parameter names specific to this operation. The merger flattens the
     * global parameters (pretty, human, error_trace, source, filter_path) onto
     * every operation and tags them {@code x-global}; those are hoisted into a
     * single shared set instead of repeated on every route.
     */
    private static Set<String> operationQueryParams(JsonNode spec, JsonNode operation) {
        Set<String> params = new TreeSet<>();
        for (JsonNode parameter : operation.path("parameters")) {
            JsonNode resolved = resolveParameter(spec, parameter);
            if ("query".equals(resolved.path("in").asText()) && resolved.path("x-global").asBoolean(false) == false) {
                params.add(resolved.path("name").asText());
            }
        }
        return params;
    }

    private static Set<String> discoverGlobalQueryParams(JsonNode spec) {
        Set<String> params = new TreeSet<>();
        for (Map.Entry<String, JsonNode> entry : spec.path("components").path("parameters").properties()) {
            JsonNode parameter = entry.getValue();
            if (parameter.path("x-global").asBoolean(false) && "query".equals(parameter.path("in").asText())) {
                params.add(parameter.path("name").asText());
            }
        }
        return params;
    }

    private static JsonNode resolveParameter(JsonNode spec, JsonNode parameter) {
        JsonNode ref = parameter.get("$ref");
        if (ref == null) {
            return parameter;
        }
        String pointer = ref.asText();
        if (pointer.startsWith("#/") == false) {
            throw new IllegalStateException("unsupported external $ref: " + pointer);
        }
        // JSON Pointer escaping: the merged spec uses names like
        // "indices.get_alias___query.local", which need no unescaping, but be
        // correct about ~ and / regardless.
        JsonNode resolved = spec.at(pointer.substring(1));
        if (resolved.isMissingNode()) {
            throw new IllegalStateException("dangling $ref: " + pointer);
        }
        return resolved;
    }

    // ------------------------------------------------------------------
    // Scope enforcement
    // ------------------------------------------------------------------

    private static void validate(Scope scope, List<SpecRoute> allRoutes) {
        List<String> errors = new ArrayList<>();

        Set<String> specNamespaces = allRoutes.stream()
            .map(SpecRoute::namespace)
            .collect(Collectors.toCollection(TreeSet::new));
        Set<String> specGroups = allRoutes.stream()
            .map(SpecRoute::operationGroup)
            .collect(Collectors.toCollection(TreeSet::new));

        Set<String> classifiedNamespaces = new TreeSet<>(scope.excludedNamespaces().keySet());
        classifiedNamespaces.addAll(scope.reviewedNamespaces());

        for (String namespace : specNamespaces) {
            if (classifiedNamespaces.contains(namespace) == false) {
                errors.add(
                    "namespace [" + namespace + "] exists in the spec but is not classified. Add it to "
                        + "reviewed_namespaces (and classify each of its operations) or to excluded_namespaces with a reason."
                );
            }
        }

        for (String namespace : scope.reviewedNamespaces()) {
            if (scope.excludedNamespaces().containsKey(namespace)) {
                errors.add("namespace [" + namespace + "] is listed as both reviewed and excluded");
            }
            if (specNamespaces.contains(namespace) == false) {
                errors.add("namespace [" + namespace + "] is reviewed but no longer exists in the spec");
            }
        }

        // Within reviewed namespaces every operation group needs a verdict.
        for (String group : specGroups) {
            String namespace = namespaceOf(group);
            if (scope.reviewedNamespaces().contains(namespace) == false) {
                continue;
            }
            boolean included = scope.includedOperations().contains(group);
            boolean excluded = scope.excludedOperations().containsKey(group);
            if (included && excluded) {
                errors.add("operation [" + group + "] is listed as both included and excluded");
            } else if (included == false && excluded == false) {
                errors.add(
                    "operation [" + group + "] exists in the spec but is not classified. Add it to "
                        + "included_operations or to excluded_operations with a reason."
                );
            }
        }

        // Stale entries: the scope file references something the spec no longer has.
        for (String group : scope.includedOperations()) {
            if (specGroups.contains(group) == false) {
                errors.add("included operation [" + group + "] no longer exists in the spec");
            }
        }
        for (String group : scope.excludedOperations().keySet()) {
            if (specGroups.contains(group) == false) {
                errors.add("excluded operation [" + group + "] no longer exists in the spec");
            }
        }

        // An included operation inside a wholesale-excluded namespace is a contradiction.
        for (String group : scope.includedOperations()) {
            String namespace = namespaceOf(group);
            if (scope.excludedNamespaces().containsKey(namespace)) {
                errors.add(
                    "operation [" + group + "] is included but its namespace [" + namespace + "] is excluded wholesale"
                );
            }
        }

        if (errors.isEmpty() == false) {
            throw new IllegalStateException(
                "control-plane-scope.yaml does not fully classify the spec ("
                    + errors.size() + " problem(s)):\n  - " + String.join("\n  - ", errors)
            );
        }
    }

    // ------------------------------------------------------------------
    // Emission
    // ------------------------------------------------------------------

    private static String renderRouteTable(List<SpecRoute> routes, Set<String> globalQueryParams, String specVersion) {
        // Group by namespace so each namespace gets its own method. A single
        // static initialiser holding every route would risk the 64 KB method
        // bytecode limit as the surface grows.
        Map<String, List<SpecRoute>> byNamespace = new TreeMap<>();
        for (SpecRoute route : routes) {
            byNamespace.computeIfAbsent(route.namespace(), key -> new ArrayList<>()).add(route);
        }

        StringBuilder out = new StringBuilder(64 * 1024);
        out.append("""
            /*
             * SPDX-License-Identifier: Apache-2.0
             *
             * GENERATED FILE - DO NOT EDIT.
             *
             * Produced by :codegen from spec/opensearch-openapi.yaml filtered through
             * spec/control-plane-scope.yaml. To change the API surface, edit the scope
             * file and re-run `./gradlew :server:generateRoutes`.
             */
            """);
        out.append("package ").append(TARGET_PACKAGE).append(";\n\n");

        out.append("""
            import java.util.ArrayList;
            import java.util.List;
            import java.util.Set;

            /** The control plane HTTP surface, derived from the OpenSearch API specification. */
            public final class GeneratedRoutes {

            """);

        out.append("    /** info.version of the spec this table was generated from. */\n");
        out.append("    public static final String SPEC_VERSION = \"").append(escape(specVersion)).append("\";\n\n");

        out.append("    /** Query parameters that every OpenSearch operation accepts. */\n");
        out.append("    public static final Set<String> GLOBAL_QUERY_PARAMS = Set.of(\n        ");
        out.append(globalQueryParams.stream().map(p -> "\"" + escape(p) + "\"").collect(Collectors.joining(",\n        ")));
        out.append("\n    );\n\n");

        out.append("    private static final List<Route> ROUTES = buildRoutes();\n\n");
        out.append("    private GeneratedRoutes() {}\n\n");
        out.append("    /** Every route in the control plane surface, in a stable order. */\n");
        out.append("    public static List<Route> routes() {\n        return ROUTES;\n    }\n\n");

        out.append("    private static List<Route> buildRoutes() {\n");
        out.append("        List<Route> routes = new ArrayList<>(").append(routes.size()).append(");\n");
        for (String namespace : byNamespace.keySet()) {
            out.append("        ").append(methodName(namespace)).append("(routes);\n");
        }
        out.append("        return List.copyOf(routes);\n    }\n");

        for (Map.Entry<String, List<SpecRoute>> entry : byNamespace.entrySet()) {
            String namespace = entry.getKey();
            List<SpecRoute> namespaceRoutes = entry.getValue();
            out.append("\n    // ").append(namespace).append(": ").append(namespaceRoutes.size())
               .append(" routes, ")
               .append(namespaceRoutes.stream().map(SpecRoute::operationGroup).distinct().count())
               .append(" operation groups\n");
            out.append("    private static void ").append(methodName(namespace)).append("(List<Route> routes) {\n");
            for (SpecRoute route : namespaceRoutes) {
                out.append("        routes.add(new Route(\"").append(route.method()).append("\", \"")
                   .append(escape(route.path())).append("\", \"").append(route.operationGroup()).append("\", ")
                   .append(route.deprecated()).append(", ");
                if (route.queryParams().isEmpty()) {
                    out.append("Set.of()");
                } else {
                    out.append("Set.of(")
                       .append(route.queryParams().stream()
                           .map(p -> "\"" + escape(p) + "\"")
                           .collect(Collectors.joining(", ")))
                       .append(")");
                }
                out.append("));\n");
            }
            out.append("    }\n");
        }

        out.append("}\n");
        return out.toString();
    }

    private static String methodName(String namespace) {
        StringBuilder camel = new StringBuilder("add");
        for (String part : namespace.split("_")) {
            if (part.isEmpty()) {
                continue;
            }
            camel.append(Character.toUpperCase(part.charAt(0))).append(part.substring(1));
        }
        return camel.append("Routes").toString();
    }

    private static String escape(String value) {
        return value.replace("\\", "\\\\").replace("\"", "\\\"");
    }

    private static String renderReport(Scope scope, List<SpecRoute> allRoutes, List<SpecRoute> inScope, String specVersion) {
        Map<String, List<SpecRoute>> byNamespace = allRoutes.stream()
            .collect(Collectors.groupingBy(SpecRoute::namespace, TreeMap::new, Collectors.toList()));

        StringBuilder out = new StringBuilder();
        out.append("# Control plane scope report\n\n");
        out.append("Generated from spec version `").append(specVersion).append("`.\n\n");
        out.append("| | operation groups | routes |\n|---|---|---|\n");
        out.append("| in scope | ").append(inScope.stream().map(SpecRoute::operationGroup).distinct().count())
           .append(" | ").append(inScope.size()).append(" |\n");
        out.append("| in spec | ").append(allRoutes.stream().map(SpecRoute::operationGroup).distinct().count())
           .append(" | ").append(allRoutes.size()).append(" |\n\n");

        out.append("## Reviewed namespaces\n\n");
        for (String namespace : scope.reviewedNamespaces().stream().sorted().toList()) {
            List<SpecRoute> namespaceRoutes = byNamespace.getOrDefault(namespace, List.of());
            Map<String, Long> groups = namespaceRoutes.stream()
                .collect(Collectors.groupingBy(SpecRoute::operationGroup, TreeMap::new, Collectors.counting()));
            long included = groups.keySet().stream().filter(scope::isIncluded).count();
            out.append("### `").append(namespace).append("` — ").append(included).append('/')
               .append(groups.size()).append(" operation groups in scope\n\n");
            for (Map.Entry<String, Long> group : groups.entrySet()) {
                boolean in = scope.isIncluded(group.getKey());
                out.append(in ? "- **in** " : "- out ").append('`').append(group.getKey()).append('`')
                   .append(" (").append(group.getValue()).append(in ? " routes)" : " routes) — ")
                   .append(in ? "" : scope.excludedOperations().getOrDefault(group.getKey(), ""))
                   .append('\n');
            }
            out.append('\n');
        }

        out.append("## Excluded namespaces\n\n");
        scope.excludedNamespaces().entrySet().stream()
            .sorted(Map.Entry.comparingByKey())
            .forEach(entry -> {
                List<SpecRoute> namespaceRoutes = byNamespace.getOrDefault(entry.getKey(), List.of());
                out.append("- `").append(entry.getKey()).append("` (")
                   .append(namespaceRoutes.stream().map(SpecRoute::operationGroup).distinct().count())
                   .append(" groups, ").append(namespaceRoutes.size()).append(" routes) — ")
                   .append(entry.getValue()).append('\n');
            });

        out.append("\n## Operation groups in scope\n\n");
        Map<String, List<SpecRoute>> byGroup = inScope.stream()
            .collect(Collectors.groupingBy(SpecRoute::operationGroup, TreeMap::new, Collectors.toList()));
        for (Map.Entry<String, List<SpecRoute>> entry : byGroup.entrySet()) {
            out.append("- `").append(entry.getKey()).append("`\n");
            entry.getValue().stream()
                .sorted(Comparator.comparing(SpecRoute::path).thenComparing(SpecRoute::method))
                .forEach(route -> out.append("    - `").append(route.method()).append(' ')
                    .append(route.path()).append('`')
                    .append(route.deprecated() ? " _(deprecated)_" : "").append('\n'));
        }
        return out.toString();
    }
}
