/*
 * Copyright OpenSearch Software Foundation
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.controlplane.handler.cluster;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.opensearch.controlplane.TestContexts;
import org.opensearch.controlplane.handler.ControlPlaneHandlers;
import org.opensearch.controlplane.handler.HandlerRegistry;
import org.opensearch.controlplane.handler.core.ClusterIdentity;
import org.opensearch.controlplane.rest.RestResponse;
import org.opensearch.controlplane.rest.RestStatus;
import org.opensearch.controlplane.router.Router;
import org.opensearch.controlplane.store.InMemoryKeyValueStore;
import org.opensearch.controlplane.tenant.HeaderTenantResolver;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * {@code cluster.health} from the supervisor's projections. The projections are written straight into the store,
 * in the shapes {@code supervisor/SUPERVISOR_KEYS.md} §1 documents, as the supervisor writes them.
 */
class ClusterHealthHandlerTests {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final String TENANT = "tenant/default/";

    private InMemoryKeyValueStore store;
    private Router router;

    @BeforeEach
    void setUp() {
        store = new InMemoryKeyValueStore();
        router = routerFor(TestContexts.factory(store));
    }

    /** The full registry, with health polling every 20 ms so that waiting tests finish quickly. */
    private static Router routerFor(org.opensearch.controlplane.handler.OperationContextFactory contexts) {
        HandlerRegistry registry = ControlPlaneHandlers.build()
            .register(new ClusterHealthHandler(ClusterIdentity.defaults(), Duration.ofMillis(20)));
        return Router.fromGeneratedRoutes(registry, contexts);
    }

    private RestResponse health(String path) {
        int query = path.indexOf('?');
        String rawPath = query < 0 ? path : path.substring(0, query);
        Map<String, List<String>> params = new java.util.LinkedHashMap<>();
        if (query >= 0) {
            for (String pair : path.substring(query + 1).split("&")) {
                String[] parts = pair.split("=", 2);
                params.put(parts[0], List.of(parts.length > 1 ? parts[1] : ""));
            }
        }
        return router.dispatch("GET", rawPath, path, params, Map.of(), new byte[0]);
    }

    private static JsonNode body(RestResponse response) throws Exception {
        return MAPPER.readTree(response.body());
    }

    private String create(String index, String settings) throws Exception {
        String path = "/" + index;
        router.dispatch("PUT", path, path, Map.of(), Map.of(),
            ("{\"settings\":" + settings + "}").getBytes(StandardCharsets.UTF_8));
        return MAPPER.readTree(store.get(TENANT + "indices/" + index).orElseThrow().value()).get("uuid").asText();
    }

    private void clusterStatus(long observedAt, long heartbeatMs, int nodes) {
        store.put(TENANT + "status/cluster", ("{\"status\":\"green\",\"number_of_nodes\":" + nodes
            + ",\"number_of_data_nodes\":" + nodes + ",\"active_shards\":0,\"active_primary_shards\":0,"
            + "\"unassigned_shards\":0,\"initializing_shards\":0,\"indices_by_status\":{},\"observed_at\":" + observedAt
            + ",\"observed_pass\":1,\"heartbeat_ms\":" + heartbeatMs + "}").getBytes(StandardCharsets.UTF_8));
    }

    private void freshCluster(int nodes) {
        clusterStatus(System.currentTimeMillis(), 10_000, nodes);
    }

    /** An index projection; each shard is a list of "ROLE:STATE" copies. */
    private void indexStatus(String name, String uuid, String status, int replicas, List<List<String>> shards) {
        StringBuilder placements = new StringBuilder("{");
        int active = 0;
        int primaries = 0;
        int initializing = 0;
        int unassigned = 0;
        for (int shard = 0; shard < shards.size(); shard++) {
            if (shard > 0) {
                placements.append(',');
            }
            placements.append('"').append(shard).append("\":[");
            List<String> copies = shards.get(shard);
            for (int i = 0; i < copies.size(); i++) {
                String[] copy = copies.get(i).split(":");
                placements.append(i > 0 ? "," : "").append("{\"node_id\":\"n").append(i).append("\",\"role\":\"")
                    .append(copy[0]).append("\",\"state\":\"").append(copy[1]).append("\"}");
                if (copy[1].equals("STARTED")) {
                    active++;
                    if (copy[0].equals("PRIMARY")) {
                        primaries++;
                    }
                } else if (copy[1].equals("INITIALIZING")) {
                    initializing++;
                }
            }
            unassigned += Math.max(0, 1 + replicas - copies.size());
            placements.append(']');
        }
        placements.append('}');
        store.put(TENANT + "status/indices/" + uuid, ("{\"index\":\"" + name + "\",\"index_uuid\":\"" + uuid
            + "\",\"status\":\"" + status + "\",\"number_of_shards\":" + shards.size() + ",\"number_of_replicas\":"
            + replicas + ",\"number_of_search_replicas\":0,\"active_shards\":" + active + ",\"active_primary_shards\":"
            + primaries + ",\"initializing_shards\":" + initializing + ",\"unassigned_shards\":" + unassigned
            + ",\"shards\":" + placements + ",\"observed_at\":" + System.currentTimeMillis() + ",\"observed_pass\":1}")
            .getBytes(StandardCharsets.UTF_8));
    }

    // ------------------------------------------------------------------ the three the data plane asked for

    /** The trap: the projection lists only observed copies, so a recomputation would call this green. */
    @Test
    void aShardMissingAReplicaIsYellowFromTheProjection() throws Exception {
        String uuid = create("logs", "{\"number_of_shards\":1,\"number_of_replicas\":1}");
        indexStatus("logs", uuid, "yellow", 1, List.of(List.of("PRIMARY:STARTED")));
        freshCluster(2);

        JsonNode health = body(health("/_cluster/health?level=shards"));
        assertEquals("yellow", health.get("status").asText());
        assertEquals(1, health.get("unassigned_shards").asInt());
        JsonNode shard = health.get("indices").get("logs").get("shards").get("0");
        assertEquals("yellow", shard.get("status").asText());
        assertEquals(1, shard.get("unassigned_shards").asInt());
        assertTrue(shard.get("primary_active").asBoolean());
    }

    /** Each tenant sees its own indices, by its own names, and nobody else's. */
    @Test
    void levelIndicesListsOnlyThisTenantsIndicesByTheirOwnNames() throws Exception {
        router = routerFor(TestContexts.factory(store, new HeaderTenantResolver()));
        for (String tenant : List.of("acme", "globex")) {
            String index = tenant + "-logs";
            router.dispatch("PUT", "/" + index, "/" + index, Map.of(), Map.of("x-tenant-id", tenant), new byte[0]);
            String uuid = MAPPER.readTree(store.get("tenant/" + tenant + "/indices/" + index).orElseThrow().value())
                .get("uuid").asText();
            store.put("tenant/" + tenant + "/status/indices/" + uuid, ("{\"index\":\"" + index + "\",\"index_uuid\":\""
                + uuid + "\",\"status\":\"green\",\"number_of_shards\":1,\"number_of_replicas\":0,"
                + "\"active_shards\":1,\"active_primary_shards\":1,\"initializing_shards\":0,\"unassigned_shards\":0,"
                + "\"shards\":{\"0\":[{\"node_id\":\"n\",\"role\":\"PRIMARY\",\"state\":\"STARTED\"}]}}")
                .getBytes(StandardCharsets.UTF_8));
            store.put("tenant/" + tenant + "/status/cluster", ("{\"status\":\"green\",\"number_of_nodes\":1,"
                + "\"number_of_data_nodes\":1,\"observed_at\":" + System.currentTimeMillis() + ",\"heartbeat_ms\":10000}")
                .getBytes(StandardCharsets.UTF_8));
        }

        RestResponse response = router.dispatch("GET", "/_cluster/health", "/_cluster/health",
            Map.of("level", List.of("indices")), Map.of("x-tenant-id", "acme"), new byte[0]);
        List<String> names = new ArrayList<>();
        body(response).get("indices").fieldNames().forEachRemaining(names::add);
        assertEquals(List.of("acme-logs"), names);
    }

    // ------------------------------------------------------------------ composition

    @Test
    void anIndexNotYetProjectedIsRedWithEveryCopyUnassigned() throws Exception {
        create("fresh", "{\"number_of_shards\":2,\"number_of_replicas\":1}");
        freshCluster(1);

        JsonNode health = body(health("/_cluster/health?level=indices"));
        assertEquals("red", health.get("status").asText());
        assertEquals(4, health.get("unassigned_shards").asInt());
        assertEquals("red", health.get("indices").get("fresh").get("status").asText());
    }

    /** The supervisor removes a gone index's projection a pass later; until then it must not answer. */
    @Test
    void aProjectionForAnIndexThatNoLongerExistsIsIgnored() throws Exception {
        indexStatus("deleted", "gone-uuid", "red", 0, List.of(List.of()));
        freshCluster(1);

        JsonNode health = body(health("/_cluster/health?level=indices"));
        assertEquals("green", health.get("status").asText());
        assertEquals(0, health.get("indices").size());
    }

    /** Filtered: status and counts over the named indices; the percentage over every index, as upstream. */
    @Test
    void filteredHealthCoversTheNamedIndicesOnly() throws Exception {
        String good = create("good", "{\"number_of_shards\":1,\"number_of_replicas\":0}");
        String bad = create("bad", "{\"number_of_shards\":1,\"number_of_replicas\":1}");
        indexStatus("good", good, "green", 0, List.of(List.of("PRIMARY:STARTED")));
        indexStatus("bad", bad, "yellow", 1, List.of(List.of("PRIMARY:STARTED")));
        freshCluster(1);

        JsonNode health = body(health("/_cluster/health/good"));
        assertEquals("green", health.get("status").asText());
        assertEquals(1, health.get("active_shards").asInt());
        assertEquals(100.0, health.get("active_shards_percent_as_number").asDouble());

        JsonNode unfiltered = body(health("/_cluster/health"));
        assertEquals("yellow", unfiltered.get("status").asText());
        assertEquals(2.0 / 3 * 100, unfiltered.get("active_shards_percent_as_number").asDouble(), 1e-9);
    }

    /** Upstream's field order, every required field present. */
    @Test
    void theBodyHasUpstreamsFieldsInUpstreamsOrder() throws Exception {
        freshCluster(3);

        List<String> fields = new ArrayList<>();
        body(health("/_cluster/health")).fieldNames().forEachRemaining(fields::add);
        assertEquals(List.of("cluster_name", "status", "timed_out", "number_of_nodes", "number_of_data_nodes",
            "discovered_master", "discovered_cluster_manager", "active_primary_shards", "active_shards",
            "relocating_shards", "initializing_shards", "unassigned_shards", "delayed_unassigned_shards",
            "number_of_pending_tasks", "number_of_in_flight_fetch", "task_max_waiting_in_queue_millis",
            "active_shards_percent_as_number"), fields);
    }

    @Test
    void pendingTasksAreThisServicesOwn() throws Exception {
        create("logs", "{}");
        freshCluster(1);

        JsonNode health = body(health("/_cluster/health"));
        assertEquals(1, health.get("number_of_pending_tasks").asInt(), "the create-index task");
        assertEquals(1, health.get("number_of_nodes").asInt(), "from status/cluster");
    }

    // ------------------------------------------------------------------ waiting

    @Test
    void withNoConditionItAnswersAtOnce() throws Exception {
        freshCluster(1);

        RestResponse response = health("/_cluster/health");
        assertEquals(RestStatus.OK, response.status());
        assertFalse(body(response).get("timed_out").asBoolean());
    }

    @Test
    void waitForStatusWaitsForTheProjectionToChange() throws Exception {
        String uuid = create("logs", "{\"number_of_shards\":1,\"number_of_replicas\":0}");
        freshCluster(1);
        Thread supervisor = Thread.ofVirtual().start(() -> {
            try {
                Thread.sleep(150);
            } catch (InterruptedException e) {
                return;
            }
            indexStatus("logs", uuid, "green", 0, List.of(List.of("PRIMARY:STARTED")));
        });

        RestResponse response = health("/_cluster/health?wait_for_status=green&timeout=5s");
        supervisor.join();
        assertEquals(RestStatus.OK, response.status());
        assertEquals("green", body(response).get("status").asText());
        assertFalse(body(response).get("timed_out").asBoolean());
    }

    @Test
    void aConditionNotMetByTheTimeoutAnswers408WithTheFullBody() throws Exception {
        create("logs", "{}");
        freshCluster(1);

        RestResponse response = health("/_cluster/health?wait_for_status=green&timeout=100ms");
        assertEquals(RestStatus.REQUEST_TIMEOUT, response.status());
        assertTrue(body(response).get("timed_out").asBoolean());
        assertEquals("red", body(response).get("status").asText());
    }

    /** Upstream waits for a named index to exist, then reports red and timed out. */
    @Test
    void aMissingIndexWaitsAndAnswersRed() throws Exception {
        freshCluster(1);

        RestResponse response = health("/_cluster/health/nonesuch?timeout=0s");
        assertEquals(RestStatus.REQUEST_TIMEOUT, response.status());
        assertEquals("red", body(response).get("status").asText());
        assertTrue(body(response).get("timed_out").asBoolean());
    }

    @Test
    void waitForNodesUnderstandsUpstreamsOperators() throws Exception {
        freshCluster(3);

        for (String condition : List.of("3", ">=3", "ge(3)", "<=3", "le(3)", ">2", "gt(2)", "<4", "lt(4)")) {
            assertEquals(RestStatus.OK, health("/_cluster/health?timeout=0s&wait_for_nodes=" + condition).status(), condition);
        }
        assertEquals(RestStatus.REQUEST_TIMEOUT, health("/_cluster/health?timeout=0s&wait_for_nodes=>3").status());
    }

    // ------------------------------------------------------------------ no supervisor

    @Test
    void noProjectionAtAllIsNoClusterManager() throws Exception {
        RestResponse response = health("/_cluster/health?cluster_manager_timeout=50ms");
        assertEquals(RestStatus.SERVICE_UNAVAILABLE, response.status());
        JsonNode error = body(response).get("error");
        assertEquals("cluster_manager_not_discovered_exception", error.get("type").asText());
        assertTrue(error.get("reason").isNull(), "upstream's exception has no message");
    }

    /** Older than its heartbeat plus a failover: nothing is managing the tenant. */
    @Test
    void aStaleProjectionIsNoClusterManager() throws Exception {
        clusterStatus(System.currentTimeMillis() - 60_000, 10_000, 3);

        assertEquals(RestStatus.SERVICE_UNAVAILABLE, health("/_cluster/health?timeout=50ms").status());
    }

    /** The threshold follows the published heartbeat rather than a hardcoded one. */
    @Test
    void theStalenessThresholdFollowsHeartbeatMs() throws Exception {
        clusterStatus(System.currentTimeMillis() - 50_000, 20_000, 3);

        assertEquals(RestStatus.OK, health("/_cluster/health?timeout=50ms").status(), "50 s is within 20 s + 35 s");
    }

    @Test
    void localAnswersTheLastFiguresRedWithNoClusterManager() throws Exception {
        clusterStatus(System.currentTimeMillis() - 60_000, 10_000, 3);

        RestResponse response = health("/_cluster/health?local=true");
        assertEquals(RestStatus.OK, response.status());
        JsonNode health = body(response);
        assertEquals("red", health.get("status").asText());
        assertFalse(health.get("discovered_cluster_manager").asBoolean());
        assertEquals(3, health.get("number_of_nodes").asInt());
    }

    // ------------------------------------------------------------------ rejections, in upstream's words

    @Test
    void rejectsWhatUpstreamRejects() throws Exception {
        freshCluster(1);

        assertEquals("No enum constant org.opensearch.cluster.health.ClusterHealthStatus.BLUE",
            reason(health("/_cluster/health?wait_for_status=blue")));
        assertEquals("failed to parse setting [timeout] with value [5] as a time value: unit is missing or unrecognized",
            reason(health("/_cluster/health?timeout=5")));
        assertEquals("failed to parse [1.5s], fractional time values are not supported",
            reason(health("/_cluster/health?timeout=1.5s")));
        assertEquals("wait_for_relocating_shards has been removed, use wait_for_no_relocating_shards [true/false] instead",
            reason(health("/_cluster/health?wait_for_relocating_shards=0")));
        assertEquals("Please only use one of the request parameters [master_timeout, cluster_manager_timeout].",
            reason(health("/_cluster/health?master_timeout=1s&cluster_manager_timeout=1s")));
        assertEquals("cannot parse ActiveShardCount[some]", reason(health("/_cluster/health?wait_for_active_shards=some")));
        assertEquals("No enum constant org.opensearch.common.Priority.SOON", reason(health("/_cluster/health?wait_for_events=soon")));
        assertEquals("Validation Failed: 1: level=awareness_attributes is required with awareness_attribute parameter;",
            reason(health("/_cluster/health?awareness_attribute=zone")));
    }

    private static String reason(RestResponse response) throws Exception {
        assertEquals(RestStatus.BAD_REQUEST, response.status());
        return body(response).get("error").get("reason").asText();
    }
}
