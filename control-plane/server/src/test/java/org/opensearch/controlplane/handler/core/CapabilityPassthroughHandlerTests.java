/*
 * Copyright OpenSearch Software Foundation
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.controlplane.handler.core;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.opensearch.controlplane.TestContexts;
import org.opensearch.controlplane.capability.DataPlaneCapabilities;
import org.opensearch.controlplane.capability.DeclaredCapabilities;
import org.opensearch.controlplane.capability.PermissiveCapabilities;
import org.opensearch.controlplane.handler.ControlPlaneHandlers;
import org.opensearch.controlplane.rest.RestResponse;
import org.opensearch.controlplane.rest.RestStatus;
import org.opensearch.controlplane.router.Router;
import org.opensearch.controlplane.store.InMemoryKeyValueStore;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * {@code GET /_script_context} and {@code GET /_ingest/processor/grok} — the two capability
 * passthroughs that validate nothing.
 *
 * <p>The fixture is deliberately <b>not</b> in alphabetical order at any level, because order is the
 * thing most likely to break here and a sorted fixture cannot catch it. Shapes and orderings are taken
 * from a live 3.9.0 cluster.
 */
class CapabilityPassthroughHandlerTests {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    /**
     * Shaped exactly as {@code tools/dump-capabilities.sh} emits it.
     *
     * <p>{@code zebra} before {@code alpha}, {@code canReturnEarly} after {@code execute}, and grok
     * patterns in load order rather than sorted — all three of which a re-sorting bug would disturb.
     * {@code similarity} reproduces a real signature: five parameters whose order is the signature, with
     * inner-class {@code $} notation and an array type.
     */
    private static final String CAPABILITIES = """
        processors:
          ingest: [append, set]
        script_contexts:
          - name: "zebra"
            methods:
              - name: "execute"
                return_type: "boolean"
                params: []
              - name: "canReturnEarly"
                return_type: "boolean"
                params: []
          - name: "moving-function"
            methods:
              - name: "execute"
                return_type: "double"
                params:
                  - name: "params"
                    type: "java.util.Map"
                  - name: "values"
                    type: "double[]"
          - name: "alpha"
            methods: []
        grok_patterns:
          "S3_REQUEST_LINE": "(?:%{WORD:verb} %{NOTSPACE:request})"
          "BACULA_CAPACITY": "%{INT}{1,3}(,%{INT}{3})*"
          "QUOTED": "\\\\\\"(?:[^\\\\\\"]|\\\\\\\\.)*\\\\\\""
        """;

    private Router routerWith(DataPlaneCapabilities capabilities) {
        return Router.fromGeneratedRoutes(
            ControlPlaneHandlers.build(ClusterIdentity.defaults(), capabilities),
            TestContexts.factory(new InMemoryKeyValueStore())
        );
    }

    private Router declaring(Path dir, String yaml) throws IOException {
        Path file = dir.resolve("capabilities.yaml");
        Files.writeString(file, yaml);
        return routerWith(DeclaredCapabilities.load(file));
    }

    private JsonNode get(Router router, String path) throws Exception {
        RestResponse response = router.dispatch("GET", path, path, Map.of(), Map.of(), new byte[0]);
        assertEquals(RestStatus.OK, response.status(),
            () -> new String(response.body(), StandardCharsets.UTF_8));
        return MAPPER.readTree(new String(response.body(), StandardCharsets.UTF_8));
    }

    private JsonNode get(Router router, String path, Map<String, List<String>> params) throws Exception {
        RestResponse response = router.dispatch("GET", path, path, params, Map.of(), new byte[0]);
        assertEquals(RestStatus.OK, response.status());
        return MAPPER.readTree(new String(response.body(), StandardCharsets.UTF_8));
    }

    private List<String> namesOf(JsonNode array, String field) {
        return array.valueStream().map(node -> node.get(field).asText()).toList();
    }

    // ------------------------------------------------------------------
    // registered only when declared
    // ------------------------------------------------------------------

    @Test
    void bothStayStubbedWhenNothingIsDeclared() {
        Router router = routerWith(new PermissiveCapabilities());

        assertEquals(RestStatus.NOT_IMPLEMENTED,
            router.dispatch("GET", "/_script_context").status());
        assertEquals(RestStatus.NOT_IMPLEMENTED,
            router.dispatch("GET", "/_ingest/processor/grok").status());
    }

    /** One section present and the other absent registers only the one. */
    @Test
    void eachSectionRegistersItsOwnOperation(@TempDir Path dir) throws Exception {
        Router onlyContexts = declaring(dir, """
            processors:
              ingest: [set]
            script_contexts:
              - name: "alpha"
                methods: []
            """);

        assertEquals(RestStatus.OK, onlyContexts.dispatch("GET", "/_script_context").status());
        assertEquals(RestStatus.NOT_IMPLEMENTED,
            onlyContexts.dispatch("GET", "/_ingest/processor/grok").status());
    }

    /** A capabilities file written before these sections existed keeps working. */
    @Test
    void aFileWithNeitherSectionIsStillValid(@TempDir Path dir) throws Exception {
        Router router = declaring(dir, "processors:\n  ingest: [set]\n");

        assertEquals(RestStatus.NOT_IMPLEMENTED, router.dispatch("GET", "/_script_context").status());
        assertEquals(RestStatus.NOT_IMPLEMENTED,
            router.dispatch("GET", "/_ingest/processor/grok").status());
    }

    // ------------------------------------------------------------------
    // script contexts
    // ------------------------------------------------------------------

    /**
     * Contexts come back in declared order. They are sorted by name upstream, so this looks like a sort
     * and is not one — the fixture proves the difference.
     */
    @Test
    void contextsKeepTheirDeclaredOrder(@TempDir Path dir) throws Exception {
        JsonNode contexts = get(declaring(dir, CAPABILITIES), "/_script_context").get("contexts");

        assertEquals(List.of("zebra", "moving-function", "alpha"), namesOf(contexts, "name"));
    }

    /**
     * Methods are <b>not</b> sorted: the server emits {@code execute} first, then the getters sorted
     * among themselves, so a getter starting a-d follows {@code execute} rather than preceding it.
     */
    @Test
    void methodsKeepExecuteFirstRatherThanBeingSorted(@TempDir Path dir) throws Exception {
        JsonNode contexts = get(declaring(dir, CAPABILITIES), "/_script_context").get("contexts");

        assertEquals(List.of("execute", "canReturnEarly"), namesOf(contexts.get(0).get("methods"), "name"));
    }

    /** Parameters are the signature, so their order is data. */
    @Test
    void parametersKeepSignatureOrder(@TempDir Path dir) throws Exception {
        JsonNode contexts = get(declaring(dir, CAPABILITIES), "/_script_context").get("contexts");
        JsonNode params = contexts.get(1).get("methods").get(0).get("params");

        assertEquals(List.of("params", "values"), namesOf(params, "name"));
        assertEquals(List.of("java.util.Map", "double[]"), namesOf(params, "type"));
    }

    /** {@code type} before {@code name} — the reverse of how the capabilities file spells it. */
    @Test
    void parameterFieldsAreTypeThenName(@TempDir Path dir) throws Exception {
        JsonNode contexts = get(declaring(dir, CAPABILITIES), "/_script_context").get("contexts");
        JsonNode parameter = contexts.get(1).get("methods").get(0).get("params").get(0);

        assertEquals(List.of("type", "name"), parameter.propertyStream().map(Map.Entry::getKey).toList());
    }

    @Test
    void methodFieldsAreNameReturnTypeThenParams(@TempDir Path dir) throws Exception {
        JsonNode contexts = get(declaring(dir, CAPABILITIES), "/_script_context").get("contexts");
        JsonNode method = contexts.get(1).get("methods").get(0);

        assertEquals(List.of("name", "return_type", "params"),
            method.propertyStream().map(Map.Entry::getKey).toList());
    }

    /** A context with no methods still carries an empty array, as a real cluster does. */
    @Test
    void aContextWithNoMethodsReportsAnEmptyArray(@TempDir Path dir) throws Exception {
        JsonNode contexts = get(declaring(dir, CAPABILITIES), "/_script_context").get("contexts");

        assertEquals(0, contexts.get(2).get("methods").size());
    }

    // ------------------------------------------------------------------
    // grok patterns
    // ------------------------------------------------------------------

    /** The default is load order, which is why the capabilities file must not be sorted. */
    @Test
    void grokPatternsDefaultToLoadOrder(@TempDir Path dir) throws Exception {
        JsonNode patterns = get(declaring(dir, CAPABILITIES), "/_ingest/processor/grok").get("patterns");

        assertEquals(List.of("S3_REQUEST_LINE", "BACULA_CAPACITY", "QUOTED"),
            patterns.propertyStream().map(Map.Entry::getKey).toList());
    }

    /** {@code s}, not {@code sort} or {@code sorted}: the parameter really is one letter. */
    @Test
    void grokPatternsSortWhenSIsTrue(@TempDir Path dir) throws Exception {
        JsonNode patterns = get(declaring(dir, CAPABILITIES), "/_ingest/processor/grok",
            Map.of("s", List.of("true"))).get("patterns");

        assertEquals(List.of("BACULA_CAPACITY", "QUOTED", "S3_REQUEST_LINE"),
            patterns.propertyStream().map(Map.Entry::getKey).toList());
    }

    /** A differently-spelled parameter does not sort, here or on a real cluster. */
    @Test
    void grokPatternsIgnoreAParameterNamedSort(@TempDir Path dir) throws Exception {
        JsonNode patterns = get(declaring(dir, CAPABILITIES), "/_ingest/processor/grok",
            Map.of("sort", List.of("true"))).get("patterns");

        assertEquals("S3_REQUEST_LINE", patterns.propertyStream().map(Map.Entry::getKey).findFirst().orElseThrow());
    }

    /**
     * The pattern values survive the YAML round trip intact — the reason the dump script emits them as
     * double-quoted scalars rather than bare text.
     */
    @Test
    void grokPatternValuesSurviveTheRoundTrip(@TempDir Path dir) throws Exception {
        JsonNode patterns = get(declaring(dir, CAPABILITIES), "/_ingest/processor/grok").get("patterns");

        assertEquals("(?:%{WORD:verb} %{NOTSPACE:request})", patterns.get("S3_REQUEST_LINE").textValue());
        assertEquals("%{INT}{1,3}(,%{INT}{3})*", patterns.get("BACULA_CAPACITY").textValue());
        assertEquals("\\\"(?:[^\\\"]|\\\\.)*\\\"", patterns.get("QUOTED").textValue());
    }
}
