/*
 * Copyright OpenSearch Software Foundation
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.controlplane.handler.indices;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.opensearch.controlplane.TestContexts;
import org.opensearch.controlplane.capability.DataPlaneCapabilities;
import org.opensearch.controlplane.capability.DeclaredCapabilities;
import org.opensearch.controlplane.capability.NodeVersion;
import org.opensearch.controlplane.capability.PermissiveCapabilities;
import org.opensearch.controlplane.handler.ControlPlaneHandlers;
import org.opensearch.controlplane.handler.core.ClusterIdentity;
import org.opensearch.controlplane.rest.RestResponse;
import org.opensearch.controlplane.rest.RestStatus;
import org.opensearch.controlplane.router.Router;
import org.opensearch.controlplane.store.InMemoryKeyValueStore;
import org.opensearch.controlplane.tenant.SingleTenantResolver;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

/**
 * {@code index.version.created}: fixed at creation to the oldest data-node version the capabilities declare, held
 * as {@code version_created}, rendered in upstream's encoding, and never taken from a client. The data plane's
 * finding 2 in {@code FINDINGS_FROM_THE_DATA_PLANE.md}.
 */
class VersionCreatedTests {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final String TENANT = SingleTenantResolver.DEFAULT_TENANT.keyPrefix();

    private InMemoryKeyValueStore store;

    @BeforeEach
    void setUp() {
        store = new InMemoryKeyValueStore();
    }

    private Router router(DataPlaneCapabilities capabilities) {
        return Router.fromGeneratedRoutes(
            ControlPlaneHandlers.build(ClusterIdentity.defaults(), capabilities), TestContexts.factory(store));
    }

    private static DataPlaneCapabilities dataNodesFrom(String version) {
        return new DeclaredCapabilities(Map.of(), null, null, null, NodeVersion.parse(version));
    }

    private static RestResponse send(Router router, String method, String path, String body) {
        return router.dispatch(method, path, path, Map.of("flat_settings", List.of("true")), Map.of(),
            body == null ? new byte[0] : body.getBytes(StandardCharsets.UTF_8));
    }

    private JsonNode document(String index) throws Exception {
        return MAPPER.readTree(store.get(TENANT + "indices/" + index).orElseThrow().value());
    }

    private static JsonNode settingsOf(Router router, String index) throws Exception {
        return MAPPER.readTree(send(router, "GET", "/" + index + "/_settings", null).body()).get(index).get("settings");
    }

    private static String reason(RestResponse response) throws Exception {
        return MAPPER.readTree(response.body()).get("error").get("reason").asText();
    }

    @Test
    void anIndexRecordsTheMinimumAndReportsItInUpstreamsEncoding() throws Exception {
        Router router = router(dataNodesFrom("3.10.0"));
        assertEquals(RestStatus.OK, send(router, "PUT", "/logs", null).status());

        assertEquals("3.10.0", document("logs").get("version_created").asText(), "held as a version string");
        assertFalse(document("logs").path("settings").has("index.version.created"), "and not in the stored settings");
        assertEquals("137317827", settingsOf(router, "logs").get("index.version.created").asText());
    }

    @Test
    void withNoMinimumNothingIsWritten() throws Exception {
        Router router = router(new PermissiveCapabilities());
        send(router, "PUT", "/logs", null);

        assertFalse(document("logs").has("version_created"));
        assertFalse(settingsOf(router, "logs").has("index.version.created"));
    }

    /** Fixed at creation: a later minimum does not move an index created under an earlier one. */
    @Test
    void anUpgradeLaterDoesNotMoveIt() throws Exception {
        send(router(dataNodesFrom("3.9.0")), "PUT", "/old", null);
        Router upgraded = router(dataNodesFrom("3.10.0"));
        send(upgraded, "PUT", "/new", null);
        send(upgraded, "PUT", "/old/_settings", "{\"index\":{\"number_of_replicas\":2}}");

        assertEquals("3.9.0", document("old").get("version_created").asText(), "put_settings left it alone");
        assertEquals("3.10.0", document("new").get("version_created").asText());
        assertEquals(Integer.toString(NodeVersion.parse("3.9.0").encodedId()),
            settingsOf(upgraded, "old").get("index.version.created").asText());
    }

    @Test
    void aDataStreamsBackingIndexIsStampedToo() throws Exception {
        Router router = router(dataNodesFrom("3.10.0"));
        send(router, "PUT", "/_index_template/ds", "{\"index_patterns\":[\"logs*\"],\"data_stream\":{}}");
        assertEquals(RestStatus.OK, send(router, "PUT", "/_data_stream/logs", null).status());

        assertEquals("3.10.0", document(".ds-logs-000001").get("version_created").asText());
    }

    @Test
    void clusterStateReportsItWithTheOtherSettings() throws Exception {
        Router router = router(dataNodesFrom("3.10.0"));
        send(router, "PUT", "/logs", null);

        JsonNode settings = MAPPER.readTree(send(router, "GET", "/_cluster/state/metadata/logs", null).body())
            .get("metadata").get("indices").get("logs").get("settings");
        assertEquals("137317827", settings.get("index.version.created").asText());
    }

    // ------------------------------------------------------------------ never from a client

    @Test
    void aCreateMayNotSupplyIt() throws Exception {
        Router router = router(dataNodesFrom("3.10.0"));

        RestResponse response = send(router, "PUT", "/logs", "{\"settings\":{\"index.version.created\":\"1\"}}");
        assertEquals(RestStatus.BAD_REQUEST, response.status());
        assertEquals("Validation Failed: 1: private index setting [index.version.created] can not be set explicitly;",
            reason(response));
    }

    @Test
    void aTemplateMayNotSupplyIt() throws Exception {
        Router router = router(dataNodesFrom("3.10.0"));

        RestResponse response = send(router, "PUT", "/_index_template/t",
            "{\"index_patterns\":[\"x-*\"],\"template\":{\"settings\":{\"index.version.created\":\"1\"}}}");
        assertEquals(RestStatus.BAD_REQUEST, response.status());
        assertEquals("index_template [t] invalid, cause [Validation Failed: 1: private index setting "
            + "[index.version.created] can not be set explicitly;]", reason(response));
    }

    @Test
    void putSettingsMayNotChangeIt() throws Exception {
        Router router = router(dataNodesFrom("3.10.0"));
        send(router, "PUT", "/logs", null);

        RestResponse response = send(router, "PUT", "/logs/_settings", "{\"index\":{\"version\":{\"created\":\"1\"}}}");
        assertEquals(RestStatus.BAD_REQUEST, response.status());
        assertEquals("can not update private setting [index.version.created]; this setting is managed by OpenSearch",
            reason(response));
        assertEquals("3.10.0", document("logs").get("version_created").asText());
    }
}
