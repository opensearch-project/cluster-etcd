/*
 * Copyright OpenSearch Software Foundation
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.controlplane.handler.indices;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.opensearch.controlplane.TestContexts;
import org.opensearch.controlplane.capability.PermissiveCapabilities;
import org.opensearch.controlplane.capability.PermissiveSettings;
import org.opensearch.controlplane.handler.ControlPlaneHandlers;
import org.opensearch.controlplane.handler.core.ClusterIdentity;
import org.opensearch.controlplane.rest.RestResponse;
import org.opensearch.controlplane.rest.RestStatus;
import org.opensearch.controlplane.router.Router;
import org.opensearch.controlplane.store.InMemoryKeyValueStore;

import java.nio.charset.StandardCharsets;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * A pull-based index whose ingestion settings a node would refuse is refused at create, before anything is written.
 * Without this it was acknowledged, placed, and then failed on every node it was placed on.
 */
class IngestionSettingsOnCreateTests {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final InMemoryKeyValueStore store = new InMemoryKeyValueStore();

    private final Router router = Router.fromGeneratedRoutes(
        ControlPlaneHandlers.build(ClusterIdentity.defaults(), new PermissiveCapabilities(), new PermissiveSettings()),
        TestContexts.factory(store)
    );

    private RestResponse dispatch(String method, String path, String body) {
        return router.dispatch(method, path, path, Map.of(), Map.of(),
            body == null ? new byte[0] : body.getBytes(StandardCharsets.UTF_8));
    }

    private JsonNode error(RestResponse response) throws Exception {
        return MAPPER.readTree(new String(response.body(), StandardCharsets.UTF_8)).get("error");
    }

    private static String kafkaIndex(String ingestionExtra) {
        return "{\"settings\":{\"index.number_of_shards\":\"1\",\"index.replication.type\":\"SEGMENT\","
            + "\"ingestion_source\":{\"type\":\"kafka\"," + ingestionExtra
            + "\"param\":{\"topic\":\"t\",\"bootstrap_servers\":\"b:9092\"}}}}";
    }

    @Test
    void aBadResetStateIsRefusedAndNothingIsWritten() throws Exception {
        RestResponse created = dispatch("PUT", "/bad-reset", kafkaIndex("\"pointer.init.reset\":\"beginning\","));

        assertEquals(RestStatus.BAD_REQUEST, created.status());
        assertEquals("illegal_argument_exception", error(created).get("type").asText());
        assertEquals("Invalid value for index.ingestion_source.pointer.init.reset [beginning]",
            error(created).get("reason").asText());
        assertEquals(RestStatus.NOT_FOUND, dispatch("GET", "/bad-reset", null).status(), "nothing was stored");
    }

    @Test
    void aKafkaIndexThatNamesNoReplicationTypeNeedsAllActive() throws Exception {
        RestResponse created = dispatch("PUT", "/bad-docrep",
            "{\"settings\":{\"ingestion_source\":{\"type\":\"kafka\",\"param\":{\"topic\":\"t\"}}}}");

        assertEquals(RestStatus.BAD_REQUEST, created.status());
        assertEquals("Replication type DOCUMENT is not supported in pull-based ingestion when"
            + " index.ingestion_source.all_active is not enabled", error(created).get("reason").asText());
    }

    @Test
    void aValidKafkaIndexIsStillCreated() {
        assertEquals(RestStatus.OK, dispatch("PUT", "/movies", kafkaIndex("\"pointer.init.reset\":\"earliest\",")).status());
        assertEquals(RestStatus.OK, dispatch("GET", "/movies", null).status());
    }

    @Test
    void aBadErrorStrategyIsRefusedOnUpdate() throws Exception {
        assertEquals(RestStatus.OK, dispatch("PUT", "/movies", kafkaIndex("")).status());

        RestResponse updated = dispatch("PUT", "/movies/_settings",
            "{\"index.ingestion_source.error_strategy\":\"skip\"}");

        assertEquals(RestStatus.BAD_REQUEST, updated.status());
        assertEquals("Invalid ingestion errorStrategy: skip", error(updated).get("reason").asText());
    }
}
