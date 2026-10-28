/*
 * Copyright OpenSearch Software Foundation
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.controlplane.handler.cluster;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.opensearch.controlplane.TestContexts;
import org.opensearch.controlplane.capability.DeclaredSettings;
import org.opensearch.controlplane.capability.PermissiveCapabilities;
import org.opensearch.controlplane.capability.PermissiveSettings;
import org.opensearch.controlplane.capability.SettingsRegistry;
import org.opensearch.controlplane.handler.ControlPlaneHandlers;
import org.opensearch.controlplane.handler.core.ClusterIdentity;
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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * {@code GET} and {@code PUT /_cluster/settings}, verified against OpenSearch 3.9.0.
 *
 * <p>The control plane keeps the two scopes separate and does not interpret either; which one wins is
 * the data plane's question, resolved there by merging with transient taking precedence.
 */
class ClusterSettingsHandlerTests {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private static final String REGISTRY = """
        version: "3.9.0"
        cluster:
          - {key: "cluster.max_shards_per_node",       kind: exact, properties: [Dynamic, NodeScope]}
          - {key: "cluster.blocks.read_only",          kind: exact, properties: [Dynamic, NodeScope]}
          - {key: "cluster.name",                      kind: exact, properties: [NodeScope]}
          - {key: "cluster.remote_store.state.enabled", kind: exact, properties: [NodeScope, Final]}
          - {key: "cluster.remote.*.seeds",            kind: affix, properties: [Dynamic, NodeScope]}
        """;

    private InMemoryKeyValueStore store;
    private Router permissive;

    @BeforeEach
    void setUp() {
        store = new InMemoryKeyValueStore();
        permissive = routerWith(new PermissiveSettings());
    }

    private Router routerWith(SettingsRegistry settings) {
        return Router.fromGeneratedRoutes(
            ControlPlaneHandlers.build(ClusterIdentity.defaults(), new PermissiveCapabilities(), settings),
            TestContexts.factory(store)
        );
    }

    private Router enforcing(Path dir) throws IOException {
        Path file = dir.resolve("settings_registry.yaml");
        Files.writeString(file, REGISTRY);
        return routerWith(DeclaredSettings.load(file).orElseThrow());
    }

    private RestResponse put(Router router, String body) {
        return put(router, Map.of(), body);
    }

    private RestResponse put(Router router, Map<String, List<String>> params, String body) {
        return router.dispatch("PUT", "/_cluster/settings", "/_cluster/settings", params, Map.of(),
            body.getBytes(StandardCharsets.UTF_8));
    }

    private JsonNode get(Router router) throws Exception {
        return get(router, Map.of());
    }

    private JsonNode get(Router router, Map<String, List<String>> params) throws Exception {
        RestResponse response = router.dispatch("GET", "/_cluster/settings", "/_cluster/settings",
            params, Map.of(), new byte[0]);
        assertEquals(RestStatus.OK, response.status());
        return MAPPER.readTree(new String(response.body(), StandardCharsets.UTF_8));
    }

    private JsonNode body(RestResponse response) throws Exception {
        return MAPPER.readTree(new String(response.body(), StandardCharsets.UTF_8));
    }

    private String reasonOf(RestResponse response) throws Exception {
        return body(response).get("error").get("reason").asText();
    }

    // ------------------------------------------------------------------
    // reading
    // ------------------------------------------------------------------

    /** Both scopes are always present, empty rather than absent. */
    @Test
    void anUntouchedClusterAnswersTwoEmptyBlocks() throws Exception {
        JsonNode settings = get(permissive);

        assertEquals(List.of("persistent", "transient"),
            settings.propertyStream().map(Map.Entry::getKey).toList());
        assertEquals(0, settings.get("persistent").size());
        assertEquals(0, settings.get("transient").size());
    }

    @Test
    void readsBackWhatWasSetInEachScope() throws Exception {
        put(permissive, "{\"persistent\":{\"cluster.max_shards_per_node\":1500}}");
        put(permissive, "{\"transient\":{\"cluster.blocks.read_only\":false}}");

        JsonNode settings = get(permissive);
        assertEquals("1500",
            settings.get("persistent").get("cluster").get("max_shards_per_node").textValue());
        assertEquals("false",
            settings.get("transient").get("cluster").get("blocks").get("read_only").textValue());
    }

    /** The scopes do not leak into one another; keeping them apart is the whole job here. */
    @Test
    void theScopesStaySeparate() throws Exception {
        put(permissive, "{\"persistent\":{\"cluster.max_shards_per_node\":1500}}");

        JsonNode settings = get(permissive);
        assertTrue(settings.get("persistent").has("cluster"));
        assertEquals(0, settings.get("transient").size());
    }

    @Test
    void flatSettingsAppliesToBothBlocks() throws Exception {
        put(permissive, "{\"persistent\":{\"cluster.max_shards_per_node\":1500}}");

        JsonNode settings = get(permissive, Map.of("flat_settings", List.of("true")));
        assertEquals("1500", settings.get("persistent").get("cluster.max_shards_per_node").textValue());
        assertFalse(settings.get("persistent").has("cluster"));
    }

    /** Accepted in any of the shapes a settings document may be written in. */
    @Test
    void acceptsNestedAndDottedKeys() throws Exception {
        put(permissive, "{\"persistent\":{\"cluster\":{\"max_shards_per_node\":1500}}}");

        assertEquals("1500", get(permissive, Map.of("flat_settings", List.of("true")))
            .get("persistent").get("cluster.max_shards_per_node").textValue());
    }

    // ------------------------------------------------------------------
    // the response echoes the request, not the state
    // ------------------------------------------------------------------

    @Test
    void theResponseEchoesOnlyWhatThisRequestAssigned() throws Exception {
        put(permissive, "{\"persistent\":{\"cluster.max_shards_per_node\":1500}}");

        RestResponse response = put(permissive, "{\"persistent\":{\"cluster.blocks.read_only\":false}}");
        JsonNode echoed = body(response).get("persistent");

        assertTrue(body(response).get("acknowledged").asBoolean());
        assertTrue(echoed.get("cluster").has("blocks"), "what this request set");
        assertFalse(echoed.get("cluster").has("max_shards_per_node"),
            "and not what was already set, which is still stored");
        assertEquals("1500", get(permissive, Map.of("flat_settings", List.of("true")))
            .get("persistent").get("cluster.max_shards_per_node").textValue());
    }

    /** A removal assigns nothing, so it echoes an empty block. */
    @Test
    void aNullRemovesTheSettingAndEchoesNothing() throws Exception {
        put(permissive, "{\"persistent\":{\"cluster.max_shards_per_node\":1500}}");

        RestResponse response = put(permissive, "{\"persistent\":{\"cluster.max_shards_per_node\":null}}");
        assertEquals(0, body(response).get("persistent").size());
        assertEquals(0, get(permissive).get("persistent").size(), "gone, not stored as null");
    }

    // ------------------------------------------------------------------
    // rejections
    // ------------------------------------------------------------------

    /** An empty body, and a body naming neither scope, are the same error. */
    @Test
    void rejectsARequestThatSetsNothing() throws Exception {
        for (String body : List.of("{}", "{\"bogus\":{\"a\":1}}")) {
            RestResponse response = put(permissive, body);
            assertEquals(RestStatus.BAD_REQUEST, response.status(), body);
            assertEquals("action_request_validation_exception",
                body(response).get("error").get("type").asText());
            assertEquals("Validation Failed: 1: no settings to update;", reasonOf(response), body);
        }
    }

    @Test
    void withoutARegistryNothingIsValidated() throws Exception {
        assertEquals(RestStatus.OK, put(permissive, "{\"persistent\":{\"cluster.totally_bogus\":1}}").status());
        assertEquals(RestStatus.OK, put(permissive, "{\"persistent\":{\"cluster.name\":\"x\"}}").status());
    }

    /** Different wording from the index path, and the scope word is part of the message. */
    @Test
    void rejectsAnUnknownSettingNamingTheScope(@TempDir Path dir) throws Exception {
        Router router = enforcing(dir);

        assertEquals("persistent setting [cluster.totally_bogus], not recognized",
            reasonOf(put(router, "{\"persistent\":{\"cluster.totally_bogus\":1}}")));
        assertEquals("transient setting [cluster.totally_bogus], not recognized",
            reasonOf(put(router, "{\"transient\":{\"cluster.totally_bogus\":1}}")));
    }

    @Test
    void rejectsAStaticSetting(@TempDir Path dir) throws Exception {
        Router router = enforcing(dir);

        RestResponse response = put(router, "{\"persistent\":{\"cluster.name\":\"x\"}}");
        assertEquals(RestStatus.BAD_REQUEST, response.status());
        assertEquals("settings_exception", body(response).get("error").get("type").asText());
        assertEquals("persistent setting [cluster.name], not dynamically updateable", reasonOf(response));
    }

    @Test
    void acceptsASettingMatchedByAnAffix(@TempDir Path dir) throws Exception {
        Router router = enforcing(dir);

        assertEquals(RestStatus.OK,
            put(router, "{\"persistent\":{\"cluster.remote.other.seeds\":\"127.0.0.1:9300\"}}").status());
    }

    /** A null is validated like a value, and reported in the same words. */
    @Test
    void aNullIsValidated(@TempDir Path dir) throws Exception {
        Router router = enforcing(dir);

        assertEquals("persistent setting [cluster.totally_bogus], not recognized",
            reasonOf(put(router, "{\"persistent\":{\"cluster.totally_bogus\":null}}")));
        assertEquals("persistent setting [cluster.name], not dynamically updateable",
            reasonOf(put(router, "{\"persistent\":{\"cluster.name\":null}}")));
    }

    @Test
    void rejectsAFinalSetting(@TempDir Path dir) throws Exception {
        RestResponse response = put(enforcing(dir), "{\"persistent\":{\"cluster.remote_store.state.enabled\":true}}");
        assertEquals(RestStatus.BAD_REQUEST, response.status());
        assertEquals("final persistent setting [cluster.remote_store.state.enabled], not updateable",
            reasonOf(response));
    }

    /** Upstream applies transient first, then the first failing key in sorted order, whatever its kind. */
    @Test
    void reportsTransientFirstThenTheFirstKeyInOrder(@TempDir Path dir) throws Exception {
        Router router = enforcing(dir);

        assertEquals("transient setting [cluster.name], not dynamically updateable", reasonOf(put(router,
            "{\"persistent\":{\"cluster.bogus\":1},\"transient\":{\"cluster.name\":\"x\"}}")));
        assertEquals("persistent setting [cluster.name], not dynamically updateable", reasonOf(put(router,
            "{\"persistent\":{\"cluster.name\":\"x\",\"cluster.zzz\":1}}")));
    }

    /** A wildcard removal is not validated, and takes only the dynamic settings it matches. */
    @Test
    void aWildcardRemovesOnlyDynamicSettings(@TempDir Path dir) throws Exception {
        store.put("tenant/default/cluster/settings",
            "{\"persistent\":{\"cluster.max_shards_per_node\":\"5\",\"cluster.name\":\"x\"},\"transient\":{}}"
                .getBytes(StandardCharsets.UTF_8));
        Router router = enforcing(dir);

        assertEquals(RestStatus.OK, put(router, "{\"persistent\":{\"cluster.*\":null}}").status());
        JsonNode persistent = get(router, Map.of("flat_settings", List.of("true"))).get("persistent");
        assertFalse(persistent.has("cluster.max_shards_per_node"));
        assertEquals("x", persistent.get("cluster.name").textValue());
    }

    @Test
    void aRejectionWritesNothing(@TempDir Path dir) throws Exception {
        Router router = enforcing(dir);
        put(router, "{\"persistent\":{\"cluster.max_shards_per_node\":1500}}");

        RestResponse response = put(router,
            "{\"persistent\":{\"cluster.max_shards_per_node\":2000},\"transient\":{\"cluster.name\":\"x\"}}");
        assertEquals(RestStatus.BAD_REQUEST, response.status());
        assertEquals("1500", get(router, Map.of("flat_settings", List.of("true")))
            .get("persistent").get("cluster.max_shards_per_node").textValue(), "unchanged");
    }

    @Test
    void bothGroupsAreRegistered() {
        var registry = ControlPlaneHandlers.build();

        assertTrue(registry.implemented().contains("cluster.get_settings"));
        assertTrue(registry.implemented().contains("cluster.put_settings"));
    }
}
