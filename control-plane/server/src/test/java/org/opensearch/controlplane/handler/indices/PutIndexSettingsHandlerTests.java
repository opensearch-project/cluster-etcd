/*
 * Copyright OpenSearch Software Foundation
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.controlplane.handler.indices;

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
import org.opensearch.controlplane.handler.HandlerRegistry;
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
 * {@code PUT /{index}/_settings}, with and without a settings registry.
 *
 * <p>Both modes matter and are tested: without a registry nothing is validated, which is the shipping
 * default; with one, the rejections have to match a real cluster's. Every message here was verified
 * against OpenSearch 3.9.0.
 */
class PutIndexSettingsHandlerTests {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    /** A slice of a real registry: dynamic, static, and not-user-settable, plus a prefix. */
    private static final String REGISTRY = """
        version: "3.9.0"
        index:
          - {key: "index.number_of_shards",   kind: exact,  properties: [Final, IndexScope]}
          - {key: "index.number_of_replicas", kind: exact,  properties: [Dynamic, IndexScope]}
          - {key: "index.refresh_interval",   kind: exact,  properties: [Dynamic, IndexScope]}
          - {key: "index.number_of_search_replicas", kind: exact, properties: [Dynamic, IndexScope]}
          - {key: "index.codec",              kind: exact,  properties: [IndexScope]}
          - {key: "index.analysis.",          kind: prefix, properties: [IndexScope]}
          - {key: "index.uuid",               kind: exact,  properties: [IndexScope, PrivateIndex]}
        """;

    private InMemoryKeyValueStore store;
    private Router permissive;

    @BeforeEach
    void setUp() {
        store = new InMemoryKeyValueStore();
        permissive = routerWith(new PermissiveSettings());
    }

    private Router routerWith(SettingsRegistry settings) {
        HandlerRegistry registry = ControlPlaneHandlers.build(
            ClusterIdentity.defaults(), new PermissiveCapabilities(), settings);
        return Router.fromGeneratedRoutes(registry, TestContexts.factory(store));
    }

    private Router enforcing(Path dir) throws IOException {
        Path file = dir.resolve("settings_registry.yaml");
        Files.writeString(file, REGISTRY);
        return routerWith(DeclaredSettings.load(file).orElseThrow());
    }

    private void create(Router router, String index, String body) {
        String path = "/" + index;
        assertEquals(RestStatus.OK, router.dispatch("PUT", path, path, Map.of(), Map.of(),
            (body == null ? "" : body).getBytes(StandardCharsets.UTF_8)).status(), index);
    }

    private RestResponse put(Router router, String path, String body) {
        return router.dispatch("PUT", path, path, Map.of(), Map.of(), body.getBytes(StandardCharsets.UTF_8));
    }

    private RestResponse put(Router router, String path, Map<String, List<String>> params, String body) {
        return router.dispatch("PUT", path, path, params, Map.of(), body.getBytes(StandardCharsets.UTF_8));
    }

    private JsonNode settings(String index) throws Exception {
        return MAPPER.readTree(store.getAsString("tenant/default/indices/" + index).orElseThrow())
            .get("settings");
    }

    private String reasonOf(RestResponse response) throws Exception {
        return MAPPER.readTree(new String(response.body(), StandardCharsets.UTF_8))
            .get("error").get("reason").asText();
    }

    // ------------------------------------------------------------------
    // applying settings
    // ------------------------------------------------------------------

    @Test
    void appliesASettingAndAcknowledges() throws Exception {
        create(permissive, "my-index", "{\"settings\":{\"number_of_replicas\":1}}");

        RestResponse response = put(permissive, "/my-index/_settings", "{\"index\":{\"number_of_replicas\":2}}");
        assertEquals(RestStatus.OK, response.status());
        assertTrue(MAPPER.readTree(new String(response.body(), StandardCharsets.UTF_8))
            .get("acknowledged").asBoolean());
        assertEquals("2", settings("my-index").get("index.number_of_replicas").textValue());
    }

    /** The body is a settings document, so the same three accepted shapes apply. */
    @Test
    void acceptsTheUnprefixedAndDottedForms() throws Exception {
        create(permissive, "a", null);
        create(permissive, "b", null);

        put(permissive, "/a/_settings", "{\"number_of_replicas\":4}");
        put(permissive, "/b/_settings", "{\"index.number_of_replicas\":5}");

        assertEquals("4", settings("a").get("index.number_of_replicas").textValue());
        assertEquals("5", settings("b").get("index.number_of_replicas").textValue());
    }

    /** Verified: a null removes the setting entirely, so it reverts to its default. */
    @Test
    void aNullRemovesTheSetting() throws Exception {
        create(permissive, "my-index", "{\"settings\":{\"refresh_interval\":\"5s\"}}");
        assertTrue(settings("my-index").has("index.refresh_interval"));

        assertEquals(RestStatus.OK,
            put(permissive, "/my-index/_settings", "{\"index\":{\"refresh_interval\":null}}").status());
        assertFalse(settings("my-index").has("index.refresh_interval"), "gone, not stored as null");
    }

    /** Verified in both directions: an existing key is left alone, an absent one is set. */
    @Test
    void preserveExistingSkipsKeysTheIndexAlreadyHas() throws Exception {
        create(permissive, "my-index", "{\"settings\":{\"number_of_replicas\":3}}");

        Map<String, List<String>> preserve = Map.of("preserve_existing", List.of("true"));
        put(permissive, "/my-index/_settings", preserve, "{\"index\":{\"number_of_replicas\":9}}");
        assertEquals("3", settings("my-index").get("index.number_of_replicas").textValue(), "kept");

        put(permissive, "/my-index/_settings", preserve, "{\"index\":{\"refresh_interval\":\"7s\"}}");
        assertEquals("7s", settings("my-index").get("index.refresh_interval").textValue(), "set");
    }

    @Test
    void appliesToEveryResolvedIndexAndRegistersATask() throws Exception {
        create(permissive, "idx-a", null);
        create(permissive, "idx-b", null);

        assertEquals(RestStatus.OK, put(permissive, "/idx-*/_settings", "{\"number_of_replicas\":6}").status());
        assertEquals("6", settings("idx-a").get("index.number_of_replicas").textValue());
        assertEquals("6", settings("idx-b").get("index.number_of_replicas").textValue());

        boolean found = store.scanPrefix("tenant/default/" + IndexManagementTasks.KEY_PREFIX).values().stream()
            .map(v -> new String(v.value(), StandardCharsets.UTF_8))
            .anyMatch(task -> task.contains(IndexManagementTasks.TYPE_UPDATE_SETTINGS));
        assertTrue(found, "the data plane has to learn the settings changed");
    }

    // ------------------------------------------------------------------
    // without a registry: the shipping default
    // ------------------------------------------------------------------

    @Test
    void withoutARegistryNothingIsValidated() throws Exception {
        create(permissive, "my-index", null);

        assertEquals(RestStatus.OK,
            put(permissive, "/my-index/_settings", "{\"index\":{\"totally_bogus\":1}}").status(),
            "unknown settings are accepted");
        assertEquals(RestStatus.OK,
            put(permissive, "/my-index/_settings", "{\"index\":{\"number_of_shards\":4}}").status(),
            "and so are static ones");
    }

    // ------------------------------------------------------------------
    // with a registry
    // ------------------------------------------------------------------

    @Test
    void rejectsAnUnknownSetting(@TempDir Path dir) throws Exception {
        Router router = enforcing(dir);
        create(router, "my-index", null);

        RestResponse response = put(router, "/my-index/_settings", "{\"index\":{\"totally_bogus\":1}}");
        assertEquals(RestStatus.BAD_REQUEST, response.status());
        assertEquals(
            "unknown setting [index.totally_bogus] please check that any required plugins are installed, "
                + "or check the breaking changes documentation for removed settings",
            reasonOf(response)
        );
    }

    /**
     * A near miss gets the suggestion instead of the generic advice, as a real cluster does — and with
     * more than one candidate it becomes {@code any of [...]}, ordered by descending similarity.
     *
     * <p>The plural branch is the one worth pinning. Lucene normalises the edit distance by the
     * <b>longer</b> of the two strings; dividing by the shorter instead scores every candidate too low
     * and silently truncates the list, which is exactly what a first cut of this did — it suggested one
     * setting where a live cluster suggested two.
     */
    @Test
    void suggestsNearbySettingsForATypo(@TempDir Path dir) throws Exception {
        Router router = enforcing(dir);
        create(router, "my-index", null);

        RestResponse response = put(router, "/my-index/_settings", "{\"index\":{\"number_of_replica\":2}}");
        assertEquals(RestStatus.BAD_REQUEST, response.status());
        assertEquals(
            "unknown setting [index.number_of_replica] did you mean any of "
                + "[index.number_of_replicas, index.number_of_search_replicas]?",
            reasonOf(response)
        );
    }

    /** A single candidate uses the singular form, without the "any of". */
    @Test
    void asingleCandidateUsesTheSingularForm(@TempDir Path dir) throws Exception {
        Router router = enforcing(dir);
        create(router, "my-index", null);

        RestResponse response = put(router, "/my-index/_settings", "{\"index\":{\"refresh_intervl\":\"5s\"}}");
        assertEquals("unknown setting [index.refresh_intervl] did you mean [index.refresh_interval]?",
            reasonOf(response));
    }

    /** Byte-exact, doubled brackets and all: two lists interpolated into one format string. */
    @Test
    void rejectsAStaticSettingOnAnOpenIndex(@TempDir Path dir) throws Exception {
        Router router = enforcing(dir);
        create(router, "my-index", null);
        String uuid = MAPPER.readTree(store.getAsString("tenant/default/indices/my-index").orElseThrow())
            .get("uuid").asText();

        RestResponse response = put(router, "/my-index/_settings", "{\"index\":{\"number_of_shards\":4}}");
        assertEquals(RestStatus.BAD_REQUEST, response.status());
        assertEquals(
            "Can't update non dynamic settings [[index.number_of_shards]] for open indices "
                + "[[my-index/" + uuid + "]]",
            reasonOf(response)
        );
    }

    /** No Dynamic property at all is static too, not just an explicit Final. */
    @Test
    void aSettingWithNoDynamicPropertyIsStatic(@TempDir Path dir) throws Exception {
        Router router = enforcing(dir);
        create(router, "my-index", null);

        RestResponse response = put(router, "/my-index/_settings", "{\"index\":{\"codec\":\"best_compression\"}}");
        assertEquals(RestStatus.BAD_REQUEST, response.status());
        assertTrue(reasonOf(response).contains("[[index.codec]]"));
    }

    /**
     * A private setting has its own message here, distinct from both the unknown-setting message and
     * from what {@code indices.create} says for the same condition — {@code AbstractScopedSettings}
     * reaches it rather than {@code MetadataCreateIndexService}.
     *
     * <p>Pinned because an earlier version of this test asserted it reported as <em>unknown</em>, on the
     * strength of a probe using {@code index.provided_name} — which is absent from the registry rather
     * than private, so it was really exercising the unknown path.
     */
    @Test
    void aPrivateSettingHasItsOwnMessage(@TempDir Path dir) throws Exception {
        Router router = enforcing(dir);
        create(router, "my-index", null);

        RestResponse response = put(router, "/my-index/_settings", "{\"index\":{\"uuid\":\"x\"}}");
        assertEquals(RestStatus.BAD_REQUEST, response.status());
        assertEquals("settings_exception", MAPPER.readTree(new String(response.body(), StandardCharsets.UTF_8))
            .get("error").get("type").asText());
        assertEquals("can not update private setting [index.uuid]; this setting is managed by OpenSearch",
            reasonOf(response));
    }

    @Test
    void acceptsASettingCoveredByAPrefix(@TempDir Path dir) throws Exception {
        Router router = enforcing(dir);
        create(router, "my-index", null);

        // Static, so it is rejected for being static rather than for being unknown -- which is the
        // point: the prefix matched.
        RestResponse response = put(router, "/my-index/_settings",
            "{\"index\":{\"analysis\":{\"analyzer\":{\"my\":{\"type\":\"custom\"}}}}}");
        assertTrue(reasonOf(response).contains("non dynamic"), reasonOf(response));
    }

    /** Removing a setting is not setting it, so a null is never checked against the registry. */
    @Test
    void aNullIsNotValidated(@TempDir Path dir) throws Exception {
        Router router = enforcing(dir);
        create(router, "my-index", null);

        assertEquals(RestStatus.OK,
            put(router, "/my-index/_settings", "{\"index\":{\"totally_bogus\":null}}").status());
    }

    /** All or nothing: a rejection must leave every index untouched. */
    @Test
    void aRejectionWritesNothing(@TempDir Path dir) throws Exception {
        Router router = enforcing(dir);
        create(router, "idx-a", null);
        create(router, "idx-b", null);

        RestResponse response = put(router, "/idx-*/_settings",
            "{\"index\":{\"number_of_replicas\":8,\"number_of_shards\":4}}");
        assertEquals(RestStatus.BAD_REQUEST, response.status());
        for (String index : List.of("idx-a", "idx-b")) {
            assertEquals("1", settings(index).get("index.number_of_replicas").textValue(), index + " untouched");
        }
    }

    @Test
    void rejectsAMissingIndexAndAMalformedBody(@TempDir Path dir) throws Exception {
        Router router = enforcing(dir);
        create(router, "my-index", null);

        assertEquals(RestStatus.NOT_FOUND, put(router, "/nope/_settings", "{\"index\":{}}").status());
        assertEquals(RestStatus.BAD_REQUEST, put(router, "/my-index/_settings", "{not json").status());
        assertEquals(RestStatus.BAD_REQUEST,
            router.dispatch("PUT", "/my-index/_settings", "/my-index/_settings",
                Map.of(), Map.of(), new byte[0]).status());
    }
}
