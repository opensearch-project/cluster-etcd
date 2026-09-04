/*
 * Copyright OpenSearch Software Foundation
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.controlplane.handler.indices;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
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
 * {@code indices.close} and {@code indices.open}, and the {@code put_settings} branch they unlock.
 *
 * <p>The last of those is the reason these exist: a static setting may be changed on a closed index and
 * not on an open one, so before close there was a branch no request could reach. Everything verified
 * against OpenSearch 3.9.0.
 */
class IndexStateHandlerTests {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    /** One of each updateability, which is what this whole area turns on. */
    private static final String REGISTRY = """
        version: "3.9.0"
        index:
          - {key: "index.number_of_shards",   kind: exact, properties: [Final, IndexScope]}
          - {key: "index.number_of_replicas", kind: exact, properties: [Dynamic, IndexScope]}
          - {key: "index.codec",              kind: exact, properties: [NodeScope, IndexScope]}
          - {key: "index.verified_before_close", kind: exact, properties: [IndexScope, PrivateIndex]}
        """;

    private final InMemoryKeyValueStore store = new InMemoryKeyValueStore();

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

    private void create(Router router, String index) {
        String path = "/" + index;
        assertEquals(RestStatus.OK,
            router.dispatch("PUT", path, path, Map.of(), Map.of(), new byte[0]).status(), index);
    }

    private RestResponse post(Router router, String path) {
        return router.dispatch("POST", path, path, Map.of(), Map.of(), new byte[0]);
    }

    private RestResponse put(Router router, String path, String body) {
        return router.dispatch("PUT", path, path, Map.of(), Map.of(), body.getBytes(StandardCharsets.UTF_8));
    }

    private JsonNode body(RestResponse response) throws Exception {
        return MAPPER.readTree(new String(response.body(), StandardCharsets.UTF_8));
    }

    private JsonNode metadata(String index) throws Exception {
        return MAPPER.readTree(store.getAsString("tenant/default/indices/" + index).orElseThrow());
    }

    private String reasonOf(RestResponse response) throws Exception {
        return body(response).get("error").get("reason").asText();
    }

    // ------------------------------------------------------------------
    // close and open
    // ------------------------------------------------------------------

    @Test
    void closeReportsTheIndicesItClosed() throws Exception {
        Router router = routerWith(new PermissiveSettings());
        create(router, "my-index");

        RestResponse response = post(router, "/my-index/_close");
        assertEquals(RestStatus.OK, response.status());
        assertEquals(List.of("acknowledged", "shards_acknowledged", "indices"),
            body(response).propertyStream().map(Map.Entry::getKey).toList());
        assertTrue(body(response).get("shards_acknowledged").asBoolean());
        assertTrue(body(response).get("indices").get("my-index").get("closed").asBoolean());
        assertEquals("close", metadata("my-index").get("state").asText());
    }

    /** Open has no indices block at all — the shapes are not symmetric. */
    @Test
    void openHasNoIndicesBlock() throws Exception {
        Router router = routerWith(new PermissiveSettings());
        create(router, "my-index");
        post(router, "/my-index/_close");

        RestResponse response = post(router, "/my-index/_open");
        assertEquals(List.of("acknowledged", "shards_acknowledged"),
            body(response).propertyStream().map(Map.Entry::getKey).toList());
        assertEquals("open", metadata("my-index").get("state").asText());
    }

    /**
     * Closing an already-closed index is idempotent, and says so by reporting nothing: an empty
     * {@code indices} block and {@code shards_acknowledged: false}. Verified.
     */
    @Test
    void closingAnAlreadyClosedIndexChangesAndReportsNothing() throws Exception {
        Router router = routerWith(new PermissiveSettings());
        create(router, "my-index");
        post(router, "/my-index/_close");

        RestResponse response = post(router, "/my-index/_close");
        assertEquals(RestStatus.OK, response.status());
        assertFalse(body(response).get("shards_acknowledged").asBoolean());
        assertEquals(0, body(response).get("indices").size());
    }

    /** Closing adds the marker a real cluster adds; opening removes it rather than setting it false. */
    @Test
    void closingAddsVerifiedBeforeCloseAndOpeningRemovesIt() throws Exception {
        Router router = routerWith(new PermissiveSettings());
        create(router, "my-index");

        post(router, "/my-index/_close");
        assertEquals("true",
            metadata("my-index").get("settings").get("index.verified_before_close").textValue());

        post(router, "/my-index/_open");
        assertFalse(metadata("my-index").get("settings").has("index.verified_before_close"),
            "removed, not set to false");
    }

    /**
     * Wildcard expansion is asymmetric: close reaches open indices, open reaches closed ones. So each
     * operation touches exactly what it would change.
     */
    @Test
    void wildcardExpansionIsAsymmetric() throws Exception {
        Router router = routerWith(new PermissiveSettings());
        create(router, "idx-a");
        create(router, "idx-b");

        RestResponse closed = post(router, "/idx-*/_close");
        assertEquals(2, body(closed).get("indices").size(), "both were open, so both close");

        // Both are closed now, so a second close matches nothing at all.
        assertEquals(0, body(post(router, "/idx-*/_close")).get("indices").size());

        // But open reaches them, because it expands to closed indices.
        assertEquals(RestStatus.OK, post(router, "/idx-*/_open").status());
        for (String index : List.of("idx-a", "idx-b")) {
            assertEquals("open", metadata(index).get("state").asText(), index);
        }
    }

    @Test
    void aMissingIndexIsNotFoundAndAWildcardMatchingNothingIsFine() throws Exception {
        Router router = routerWith(new PermissiveSettings());
        create(router, "my-index");

        assertEquals(RestStatus.NOT_FOUND, post(router, "/nope/_close").status());
        assertEquals(RestStatus.NOT_FOUND, post(router, "/nope/_open").status());

        RestResponse response = post(router, "/zzz-*/_close");
        assertEquals(RestStatus.OK, response.status());
        assertEquals(0, response.body().length > 0 ? body(response).get("indices").size() : 0);
    }

    @Test
    void aClosedIndexReportsClosedFromResolveIndex() throws Exception {
        Router router = routerWith(new PermissiveSettings());
        create(router, "my-index");
        post(router, "/my-index/_close");

        JsonNode entry = MAPPER.readTree(new String(
            router.dispatch("GET", "/_resolve/index/my-index").body(), StandardCharsets.UTF_8))
            .get("indices").get(0);
        assertEquals("closed", entry.get("attributes").get(0).asText());
    }

    // ------------------------------------------------------------------
    // the put_settings branch this unlocks
    // ------------------------------------------------------------------

    /** The whole point: a static setting is legal on a closed index and not on an open one. */
    @Test
    void aStaticSettingIsAcceptedOnlyWhenClosed(@TempDir Path dir) throws Exception {
        Router router = enforcing(dir);
        create(router, "my-index");

        RestResponse whileOpen = put(router, "/my-index/_settings", "{\"index\":{\"codec\":\"best_compression\"}}");
        assertEquals(RestStatus.BAD_REQUEST, whileOpen.status());
        assertTrue(reasonOf(whileOpen).contains("[[index.codec]]"), reasonOf(whileOpen));

        post(router, "/my-index/_close");
        assertEquals(RestStatus.OK,
            put(router, "/my-index/_settings", "{\"index\":{\"codec\":\"best_compression\"}}").status());
        assertEquals("best_compression", metadata("my-index").get("settings").get("index.codec").textValue());
    }

    /** A final setting is refused even when closed — and with its own wording. */
    @Test
    void aFinalSettingIsRefusedEvenWhenClosed(@TempDir Path dir) throws Exception {
        Router router = enforcing(dir);
        create(router, "my-index");
        post(router, "/my-index/_close");

        RestResponse response = put(router, "/my-index/_settings", "{\"index\":{\"number_of_shards\":4}}");
        assertEquals(RestStatus.BAD_REQUEST, response.status());
        assertEquals("settings_exception", body(response).get("error").get("type").asText());
        assertEquals("final my-index setting [index.number_of_shards], not updateable", reasonOf(response));
    }

    /** While open, final reports as "non dynamic" like any other — the pre-filter runs first. */
    @Test
    void aFinalSettingReportsAsNonDynamicWhileOpen(@TempDir Path dir) throws Exception {
        Router router = enforcing(dir);
        create(router, "my-index");

        RestResponse response = put(router, "/my-index/_settings", "{\"index\":{\"number_of_shards\":4}}");
        assertEquals("illegal_argument_exception", body(response).get("error").get("type").asText());
        assertTrue(reasonOf(response).startsWith("Can't update non dynamic settings [[index.number_of_shards]]"),
            reasonOf(response));
    }

    /** A dynamic setting works in either state. */
    @Test
    void aDynamicSettingWorksInEitherState(@TempDir Path dir) throws Exception {
        Router router = enforcing(dir);
        create(router, "my-index");

        assertEquals(RestStatus.OK,
            put(router, "/my-index/_settings", "{\"index\":{\"number_of_replicas\":2}}").status());
        post(router, "/my-index/_close");
        assertEquals(RestStatus.OK,
            put(router, "/my-index/_settings", "{\"index\":{\"number_of_replicas\":3}}").status());
    }

    /** One open index among closed ones is enough to trigger the open-index rejection. */
    @Test
    void oneOpenIndexIsEnoughToRefuseAStaticSetting(@TempDir Path dir) throws Exception {
        Router router = enforcing(dir);
        create(router, "idx-a");
        create(router, "idx-b");
        post(router, "/idx-a/_close");

        RestResponse response = put(router, "/idx-a,idx-b/_settings", "{\"index\":{\"codec\":\"best_compression\"}}");
        assertEquals(RestStatus.BAD_REQUEST, response.status());
        assertTrue(reasonOf(response).contains("idx-b/"), "names the open one only: " + reasonOf(response));
        assertFalse(reasonOf(response).contains("idx-a/"), reasonOf(response));
    }

    @Test
    void bothGroupsAreRegistered() {
        var registry = ControlPlaneHandlers.build();

        assertTrue(registry.implemented().contains("indices.close"));
        assertTrue(registry.implemented().contains("indices.open"));
    }
}
