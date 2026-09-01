/*
 * Copyright OpenSearch Software Foundation
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.controlplane.handler.core;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.opensearch.controlplane.TestContexts;
import org.opensearch.controlplane.handler.ControlPlaneHandlers;
import org.opensearch.controlplane.handler.HandlerRegistry;
import org.opensearch.controlplane.rest.RestResponse;
import org.opensearch.controlplane.rest.RestStatus;
import org.opensearch.controlplane.router.Router;
import org.opensearch.controlplane.store.InMemoryKeyValueStore;
import org.opensearch.controlplane.tenant.HeaderTenantResolver;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * {@code GET /} and {@code HEAD /}.
 *
 * <p>The field set asserted here is not cosmetic: the spec marks {@code name}, {@code cluster_name},
 * {@code cluster_uuid}, {@code version} and {@code tagline} required, marks nine sub-fields of
 * {@code version} required, and the spec repo's test harness refuses to run any story until it can
 * read {@code version.number}.
 */
class InfoHandlerTests {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private InMemoryKeyValueStore store;
    private Router router;

    @BeforeEach
    void setUp() {
        store = new InMemoryKeyValueStore();
        router = Router.fromGeneratedRoutes(ControlPlaneHandlers.build(), TestContexts.factory(store));
    }

    private JsonNode info() throws Exception {
        RestResponse response = router.dispatch("GET", "/");
        assertEquals(RestStatus.OK, response.status());
        return MAPPER.readTree(new String(response.body(), StandardCharsets.UTF_8));
    }

    @Test
    void reportsEveryFieldTheSpecRequires() throws Exception {
        JsonNode body = info();

        for (String field : List.of("name", "cluster_name", "cluster_uuid", "version", "tagline")) {
            assertTrue(body.has(field), "missing required field " + field);
        }
        for (String field : List.of(
            "distribution", "number", "build_type", "build_hash", "build_date",
            "build_snapshot", "lucene_version",
            "minimum_wire_compatibility_version", "minimum_index_compatibility_version"
        )) {
            assertTrue(body.get("version").has(field), "missing required version field " + field);
        }
    }

    @Test
    void reportsTheOpenSearchDistributionAndTagline() throws Exception {
        JsonNode body = info();

        // Language clients branch on distribution to decide whether they are talking to OpenSearch.
        assertEquals("opensearch", body.get("version").get("distribution").asText());
        assertEquals("The OpenSearch Project: https://opensearch.org/", body.get("tagline").asText());
    }

    @Test
    void versionNumberIsWhatTheHarnessNeeds() throws Exception {
        // The tester reads exactly this to decide which stories apply.
        assertEquals(ClusterIdentity.DEFAULT_VERSION, info().get("version").get("number").asText());
    }

    @Test
    void identityIsConfigurable() throws Exception {
        Router configured = Router.fromGeneratedRoutes(
            ControlPlaneHandlers.build(ClusterIdentity.defaults().withClusterName("acme").withVersion("2.19.0")),
            TestContexts.factory(store)
        );
        JsonNode body = MAPPER.readTree(
            new String(configured.dispatch("GET", "/").body(), StandardCharsets.UTF_8)
        );

        assertEquals("acme", body.get("cluster_name").asText());
        assertEquals("2.19.0", body.get("version").get("number").asText());
    }

    @Test
    void buildHashIsUnknownRatherThanFabricatedWhenNotInjected() throws Exception {
        // No build.properties on the test classpath, so honesty over plausibility.
        assertEquals("unknown", info().get("version").get("build_hash").asText());
    }

    // ------------------------------------------------------------------
    // cluster_uuid
    // ------------------------------------------------------------------

    @Test
    void clusterUuidIsStableAcrossRequests() throws Exception {
        String first = info().get("cluster_uuid").asText();
        String second = info().get("cluster_uuid").asText();

        // Clients treat a change as "different cluster" and may discard cached state.
        assertEquals(first, second);
    }

    @Test
    void clusterUuidIsPersistedSoItSurvivesARestart() throws Exception {
        String before = info().get("cluster_uuid").asText();

        // A fresh router over the same store stands in for a restart.
        Router restarted = Router.fromGeneratedRoutes(ControlPlaneHandlers.build(), TestContexts.factory(store));
        JsonNode body = MAPPER.readTree(new String(restarted.dispatch("GET", "/").body(), StandardCharsets.UTF_8));

        assertEquals(before, body.get("cluster_uuid").asText());
    }

    @Test
    void clusterUuidUsesOpenSearchsFormat() throws Exception {
        // 128 bits, base64url, unpadded: 22 characters, as in G9zz_G-6QJub-r6T13kkOw
        String uuid = info().get("cluster_uuid").asText();

        assertEquals(22, uuid.length(), uuid);
        assertTrue(uuid.matches("[A-Za-z0-9_-]{22}"), uuid);
    }

    @Test
    void clusterUuidIsPerTenant() throws Exception {
        Router multiTenant = Router.fromGeneratedRoutes(
            ControlPlaneHandlers.build(),
            TestContexts.factory(store, new HeaderTenantResolver())
        );
        String header = HeaderTenantResolver.DEFAULT_HEADER;

        JsonNode acme = MAPPER.readTree(new String(
            multiTenant.dispatch("GET", "/", "/", Map.of(), Map.of(header, "acme"), new byte[0]).body(),
            StandardCharsets.UTF_8));
        JsonNode globex = MAPPER.readTree(new String(
            multiTenant.dispatch("GET", "/", "/", Map.of(), Map.of(header, "globex"), new byte[0]).body(),
            StandardCharsets.UTF_8));

        // Each tenant sees a coherent virtual cluster of its own.
        assertNotEquals(acme.get("cluster_uuid").asText(), globex.get("cluster_uuid").asText());
        assertTrue(store.get("tenant/acme/" + ClusterUuid.KEY).isPresent());
        assertTrue(store.get("tenant/globex/" + ClusterUuid.KEY).isPresent());
    }

    // ------------------------------------------------------------------
    // ping
    // ------------------------------------------------------------------

    @Test
    void pingReturns200WithNoBody() {
        RestResponse response = router.dispatch("HEAD", "/");

        assertEquals(RestStatus.OK, response.status());
        assertEquals(0, response.body().length);
    }

    @Test
    void infoAndPingAreRegistered() {
        HandlerRegistry registry = ControlPlaneHandlers.build();

        assertTrue(registry.implemented().contains("info"));
        assertTrue(registry.implemented().contains("ping"));
        assertFalse(registry.notImplemented().contains("info"));
    }
}
