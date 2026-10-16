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
import org.opensearch.controlplane.handler.OperationContextFactory;
import org.opensearch.controlplane.handler.core.ClusterIdentity;
import org.opensearch.controlplane.rest.RestResponse;
import org.opensearch.controlplane.rest.RestStatus;
import org.opensearch.controlplane.router.Router;
import org.opensearch.controlplane.store.InMemoryKeyValueStore;
import org.opensearch.controlplane.tenant.HeaderTenantResolver;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * {@code cluster.state} composed from this service's documents and the supervisor's projections. The shapes
 * and field orders asserted here were read off a node's serialisation of {@code ClusterState}
 * (3.10.0-SNAPSHOT); the projections are written straight into the store in the form
 * {@code supervisor/SUPERVISOR_KEYS.md} §1 documents.
 */
class ClusterStateHandlerTests {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final String TENANT = "tenant/default/";

    private InMemoryKeyValueStore store;
    private Router router;

    @BeforeEach
    void setUp() {
        store = new InMemoryKeyValueStore();
        router = routerFor(TestContexts.factory(store));
    }

    /** The full registry, polling every 20 ms so that waiting tests finish quickly. */
    private static Router routerFor(OperationContextFactory contexts) {
        HandlerRegistry registry = ControlPlaneHandlers.build()
            .register(new ClusterStateHandler(ClusterIdentity.defaults(), Duration.ofMillis(20)));
        return Router.fromGeneratedRoutes(registry, contexts);
    }

    private RestResponse get(String path) {
        return get(router, path, Map.of());
    }

    private static RestResponse get(Router router, String path, Map<String, String> headers) {
        int query = path.indexOf('?');
        String rawPath = query < 0 ? path : path.substring(0, query);
        Map<String, List<String>> params = new LinkedHashMap<>();
        if (query >= 0) {
            for (String pair : path.substring(query + 1).split("&")) {
                String[] parts = pair.split("=", 2);
                params.put(parts[0], List.of(parts.length > 1 ? parts[1] : ""));
            }
        }
        return router.dispatch("GET", rawPath, path, params, headers, new byte[0]);
    }

    private JsonNode state(String path) throws Exception {
        RestResponse response = get(path);
        assertEquals(RestStatus.OK, response.status(), () -> new String(response.body(), StandardCharsets.UTF_8));
        return MAPPER.readTree(response.body());
    }

    private static List<String> keys(JsonNode object) {
        List<String> keys = new ArrayList<>();
        object.fieldNames().forEachRemaining(keys::add);
        return keys;
    }

    private String create(String index, String body) throws Exception {
        String path = "/" + index;
        RestResponse response = router.dispatch("PUT", path, path, Map.of(), Map.of(), body.getBytes(StandardCharsets.UTF_8));
        assertEquals(RestStatus.OK, response.status(), () -> new String(response.body(), StandardCharsets.UTF_8));
        return MAPPER.readTree(store.get(TENANT + "indices/" + index).orElseThrow().value()).get("uuid").asText();
    }

    private void freshCluster() {
        store.put(TENANT + "status/cluster", ("{\"status\":\"green\",\"number_of_nodes\":1,\"observed_at\":"
            + System.currentTimeMillis() + ",\"heartbeat_ms\":10000}").replace("\\\\\"", "\"").getBytes(StandardCharsets.UTF_8));
    }

    private void staleCluster() {
        store.put(TENANT + "status/cluster", ("{\"status\":\"green\",\"number_of_nodes\":1,\"observed_at\":"
            + (System.currentTimeMillis() - 120_000) + ",\"heartbeat_ms\":10000}").replace("\\\\\"", "\"").getBytes(StandardCharsets.UTF_8));
    }

    private void node(String tenantPrefix, String id, String name, String address, int port, String roles, String attributes) {
        store.put(tenantPrefix + "status/nodes/" + id, ("{\"node_id\":\"" + id + "\",\"ephemeral_id\":\"eph-" + id
            + "\",\"name\":\"" + name + "\",\"roles\":" + roles + ",\"address\":\"" + address + "\",\"port\":" + port
            + ",\"version\":\"3.10.0\",\"attributes\":" + attributes + ",\"liveness\":\"live\",\"observed_at\":1}")
            .getBytes(StandardCharsets.UTF_8));
    }

    /** A projection; each copy is "ROLE:STATE:node[:allocation_id]". */
    private void projection(String name, String uuid, int replicas, int searchReplicas, List<List<String>> shards) {
        StringBuilder placements = new StringBuilder("{");
        for (int shard = 0; shard < shards.size(); shard++) {
            placements.append(shard > 0 ? "," : "").append('"').append(shard).append("\":[");
            List<String> copies = shards.get(shard);
            for (int i = 0; i < copies.size(); i++) {
                String[] copy = copies.get(i).split(":");
                placements.append(i > 0 ? "," : "").append("{\"node_id\":\"").append(copy[2]).append("\",\"role\":\"")
                    .append(copy[0]).append("\",\"state\":\"").append(copy[1]).append('"');
                if (copy.length > 3) {
                    placements.append(",\"allocation_id\":\"").append(copy[3]).append('"');
                    if (copy[0].equals("PRIMARY")) {
                        placements.append(",\"in_sync\":[\"").append(copy[3]).append("\"]");
                    }
                }
                placements.append('}');
            }
            placements.append(']');
        }
        placements.append('}');
        store.put(TENANT + "status/indices/" + uuid, ("{\"index\":\"" + name + "\",\"index_uuid\":\"" + uuid
            + "\",\"status\":\"green\",\"number_of_shards\":" + shards.size() + ",\"number_of_replicas\":" + replicas
            + ",\"number_of_search_replicas\":" + searchReplicas + ",\"active_shards\":0,\"active_primary_shards\":0,"
            + "\"initializing_shards\":0,\"unassigned_shards\":0,\"shards\":" + placements + ",\"observed_at\":1}")
            .getBytes(StandardCharsets.UTF_8));
    }

    // ------------------------------------------------------------------ the response, section by section

    @Test
    void everySectionComesOutInUpstreamsOrder() throws Exception {
        create("logs", "{}");
        freshCluster();

        assertEquals(List.of("cluster_name", "cluster_uuid", "version", "state_uuid", "master_node", "cluster_manager_node",
            "blocks", "nodes", "metadata", "routing_table", "routing_nodes"), keys(state("/_cluster/state")));
    }

    /** No elected cluster manager exists, and upstream writes that as null rather than leaving the field out. */
    @Test
    void thereIsNoClusterManagerAndSaysSo() throws Exception {
        JsonNode state = state("/_cluster/state/master_node,cluster_manager_node");

        assertTrue(state.has("master_node") && state.get("master_node").isNull());
        assertTrue(state.has("cluster_manager_node") && state.get("cluster_manager_node").isNull());
    }

    @Test
    void metricsSelectSectionsAndUnknownOnesAreIgnored() throws Exception {
        create("logs", "{}");
        freshCluster();

        assertEquals(List.of("cluster_name", "cluster_uuid", "metadata"), keys(state("/_cluster/state/metadata")));
        assertEquals(List.of("cluster_name", "cluster_uuid", "version", "state_uuid", "blocks"),
            keys(state("/_cluster/state/blocks,version")));
        assertEquals(List.of("cluster_name", "cluster_uuid"), keys(state("/_cluster/state/nonesuch")),
            "an unknown metric selects nothing");
        assertEquals(11, keys(state("/_cluster/state/version,_all")).size(), "_all anywhere means all");
    }

    @Test
    void theNodesAreTheTenantsOwnWithUpstreamsFieldsOnly() throws Exception {
        freshCluster();
        node(TENANT, "n1", "data-1", "10.0.0.1", 9300, "[\"data\"]", "{\"zone\":\"z1\",\"rack\":\"r7\"}");

        JsonNode node = state("/_cluster/state/nodes").get("nodes").get("n1");
        assertEquals(List.of("name", "ephemeral_id", "transport_address", "attributes"), keys(node),
            "no roles, version or host: a cluster state does not carry them");
        assertEquals("data-1", node.get("name").asText());
        assertEquals("eph-n1", node.get("ephemeral_id").asText());
        assertEquals("10.0.0.1:9300", node.get("transport_address").asText());
        assertEquals("z1", node.get("attributes").get("zone").asText());
    }

    /** The data plane asked for this one: a tenant in a shared pool does not see its neighbours' machines. */
    @Test
    void anotherTenantsNodesAreNotListed() throws Exception {
        router = routerFor(TestContexts.factory(store, new HeaderTenantResolver()));
        for (String tenant : List.of("acme", "globex")) {
            store.put("tenant/" + tenant + "/status/cluster", ("{\"number_of_nodes\":1,\"observed_at\":"
                + System.currentTimeMillis() + ",\"heartbeat_ms\":10000}").replace("\\\\\"", "\"").getBytes(StandardCharsets.UTF_8));
            node("tenant/" + tenant + "/", tenant + "-node", tenant + "-data", "10.0.0.1", 9300, "[\"data\"]", "{}");
        }

        RestResponse response = get(router, "/_cluster/state/nodes", Map.of("x-tenant-id", "acme"));
        assertEquals(List.of("acme-node"), keys(MAPPER.readTree(response.body()).get("nodes")));
    }

    // ------------------------------------------------------------------ metadata

    @Test
    void anIndexIsRenderedInUpstreamsFieldOrder() throws Exception {
        create("logs", "{\"settings\":{\"number_of_shards\":2,\"number_of_replicas\":0},"
            + "\"mappings\":{\"properties\":{\"host\":{\"type\":\"keyword\"}}},\"aliases\":{\"all-logs\":{}}}");

        JsonNode index = state("/_cluster/state/metadata").get("metadata").get("indices").get("logs");
        assertEquals(List.of("state", "settings", "mappings", "aliases", "rollover_info", "system"), keys(index));
        assertEquals("open", index.get("state").asText());
        assertEquals("2", index.get("settings").get("index").get("number_of_shards").asText());
        assertEquals("keyword", index.get("mappings").get("_doc").get("properties").get("host").get("type").asText(),
            "mappings are keyed by type, always _doc");
        assertEquals("[\"all-logs\"]", index.get("aliases").toString(), "alias names only, not their definitions");
        assertEquals(0, index.get("rollover_info").size());
        assertFalse(index.get("system").asBoolean());
    }

    @Test
    void anIndexWithNoMappingHasAnEmptyMappingsObject() throws Exception {
        create("logs", "{}");

        JsonNode mappings = state("/_cluster/state/metadata").get("metadata").get("indices").get("logs").get("mappings");
        assertTrue(mappings.isObject() && mappings.isEmpty());
    }

    @Test
    void flatSettingsFlattensTheIndexSettings() throws Exception {
        create("logs", "{\"settings\":{\"number_of_shards\":2}}");

        JsonNode settings = state("/_cluster/state/metadata?flat_settings=true")
            .get("metadata").get("indices").get("logs").get("settings");
        assertEquals("2", settings.get("index.number_of_shards").asText());
    }

    @Test
    void theMetadataHeaderAndAnEmptyCoordination() throws Exception {
        JsonNode metadata = state("/_cluster/state/metadata").get("metadata");

        assertEquals(List.of("cluster_uuid", "cluster_uuid_committed", "cluster_coordination", "templates", "indices"), keys(metadata));
        assertTrue(metadata.get("cluster_uuid_committed").asBoolean());
        assertEquals(state("/_cluster/state/metadata").get("cluster_uuid").asText(), metadata.get("cluster_uuid").asText());
        assertEquals(List.of("term", "last_committed_config", "last_accepted_config", "voting_config_exclusions"),
            keys(metadata.get("cluster_coordination")));
        assertEquals(0, metadata.get("cluster_coordination").get("term").asInt());
    }

    @Test
    void persistentSettingsAreNotPartOfTheApiContext() throws Exception {
        router.dispatch("PUT", "/_cluster/settings", "/_cluster/settings", Map.of(), Map.of(),
            "{\"persistent\":{\"cluster.max_shards_per_node\":1500}}".getBytes(StandardCharsets.UTF_8));
        freshCluster();

        assertFalse(state("/_cluster/state").get("metadata").has("settings"));
    }

    /** Typed, as the API writes templates: mappings keyed by type, settings nested. */
    @Test
    void legacyTemplatesAreRenderedTyped() throws Exception {
        router.dispatch("PUT", "/_template/t", "/_template/t", Map.of(), Map.of(),
            ("{\"index_patterns\":[\"x-*\"],\"order\":3,\"settings\":{\"number_of_shards\":2},"
                + "\"mappings\":{\"properties\":{\"f\":{\"type\":\"text\"}}},\"aliases\":{\"a\":{}}}").getBytes(StandardCharsets.UTF_8));

        JsonNode template = state("/_cluster/state/metadata").get("metadata").get("templates").get("t");
        assertEquals(List.of("order", "index_patterns", "settings", "mappings", "aliases"), keys(template));
        assertEquals(3, template.get("order").asInt());
        assertEquals("2", template.get("settings").get("index").get("number_of_shards").asText());
        assertEquals("text", template.get("mappings").get("_doc").get("properties").get("f").get("type").asText());
        assertTrue(template.get("aliases").has("a"));
    }

    // ------------------------------------------------------------------ the index expression

    /**
     * Upstream builds a fresh Metadata for a filtered request: only the matching indices, no templates, and a
     * uuid that reads uncommitted. Reproduced rather than papered over.
     */
    @Test
    void anIndexExpressionNarrowsMetadataTheWayUpstreamDoes() throws Exception {
        create("logs-1", "{}");
        create("logs-2", "{}");
        create("other", "{}");
        router.dispatch("PUT", "/_template/t", "/_template/t", Map.of(), Map.of(),
            "{\"index_patterns\":[\"x-*\"]}".getBytes(StandardCharsets.UTF_8));

        JsonNode metadata = state("/_cluster/state/metadata/logs-*").get("metadata");
        assertEquals(List.of("logs-1", "logs-2"), keys(metadata.get("indices")));
        assertFalse(metadata.get("cluster_uuid_committed").asBoolean());
        assertEquals(0, metadata.get("templates").size());
        assertEquals(1, state("/_cluster/state/metadata").get("metadata").get("templates").size(), "unfiltered still has them");
    }

    @Test
    void aMissingIndexIsSkippedAndNothingMatchingIsEmpty() throws Exception {
        create("logs", "{}");
        freshCluster();

        assertEquals(List.of("logs"), keys(state("/_cluster/state/metadata/logs,nonesuch").get("metadata").get("indices")));
        assertEquals(0, state("/_cluster/state/metadata/nonesuch").get("metadata").get("indices").size());
        assertEquals(0, state("/_cluster/state/routing_table/nonesuch").get("routing_table").get("indices").size());
    }

    /** A wildcard expands to open indices only; naming a closed index still finds it. */
    @Test
    void wildcardsSkipClosedIndicesButANameDoesNot() throws Exception {
        create("open-one", "{}");
        create("closed-one", "{}");
        router.dispatch("POST", "/closed-one/_close");

        assertEquals(List.of("open-one"), keys(state("/_cluster/state/metadata/*-one").get("metadata").get("indices")));
        assertEquals(List.of("closed-one"), keys(state("/_cluster/state/metadata/closed-one").get("metadata").get("indices")));
    }

    // ------------------------------------------------------------------ routing

    @Test
    void routingTableListsObservedCopiesThenTheUnassignedOnes() throws Exception {
        String uuid = create("logs", "{\"settings\":{\"number_of_shards\":1,\"number_of_replicas\":2}}");
        projection("logs", uuid, 2, 0, List.of(List.of("PRIMARY:STARTED:n1:alloc-1", "REPLICA:INITIALIZING:n2:alloc-2")));
        freshCluster();

        JsonNode copies = state("/_cluster/state/routing_table").get("routing_table").get("indices").get("logs").get("shards").get("0");
        assertEquals(3, copies.size(), "two observed and one wanted and placed nowhere");

        JsonNode primary = copies.get(0);
        assertEquals(List.of("state", "primary", "searchOnly", "node", "relocating_node", "shard", "index", "allocation_id"), keys(primary));
        assertEquals("STARTED", primary.get("state").asText());
        assertTrue(primary.get("primary").asBoolean());
        assertEquals("n1", primary.get("node").asText());
        assertTrue(primary.get("relocating_node").isNull());
        assertEquals("alloc-1", primary.get("allocation_id").get("id").asText());

        assertEquals("INITIALIZING", copies.get(1).get("state").asText());

        JsonNode unassigned = copies.get(2);
        assertEquals(List.of("state", "primary", "searchOnly", "node", "relocating_node", "shard", "index"), keys(unassigned),
            "no allocation id, and no unassigned_info: why and when are not projected");
        assertEquals("UNASSIGNED", unassigned.get("state").asText());
        assertFalse(unassigned.get("primary").asBoolean());
        assertTrue(unassigned.has("node") && unassigned.get("node").isNull(), "an explicit null, not an omission");
    }

    @Test
    void aMissingPrimaryAndAMissingSearchReplicaAreBothUnassigned() throws Exception {
        String uuid = create("logs", "{\"settings\":{\"number_of_shards\":1,\"number_of_replicas\":0,\"number_of_search_replicas\":1}}");
        projection("logs", uuid, 0, 1, List.of(List.of()));
        freshCluster();

        JsonNode copies = state("/_cluster/state/routing_table").get("routing_table").get("indices").get("logs").get("shards").get("0");
        assertEquals(2, copies.size());
        assertTrue(copies.get(0).get("primary").asBoolean() && copies.get(0).get("searchOnly").asBoolean() == false);
        assertTrue(copies.get(1).get("searchOnly").asBoolean() && copies.get(1).get("primary").asBoolean() == false);
    }

    /** An index no pass has projected yet: every copy it asks for is unassigned. */
    @Test
    void anUnprojectedIndexIsEntirelyUnassigned() throws Exception {
        create("fresh", "{\"settings\":{\"number_of_shards\":2,\"number_of_replicas\":1}}");
        freshCluster();

        JsonNode shards = state("/_cluster/state/routing_table").get("routing_table").get("indices").get("fresh").get("shards");
        assertEquals(List.of("0", "1"), keys(shards));
        shards.forEach(copies -> {
            assertEquals(2, copies.size());
            copies.forEach(copy -> assertEquals("UNASSIGNED", copy.get("state").asText()));
        });
    }

    /** A copy the node reports failed serves nothing, so it is unassigned, and holds no node. */
    @Test
    void aFailedCopyIsUnassigned() throws Exception {
        String uuid = create("logs", "{\"settings\":{\"number_of_shards\":1,\"number_of_replicas\":0}}");
        projection("logs", uuid, 0, 0, List.of(List.of("PRIMARY:FAILED:n1:alloc-1")));
        freshCluster();

        JsonNode copy = state("/_cluster/state/routing_table").get("routing_table").get("indices").get("logs").get("shards").get("0").get(0);
        assertEquals("UNASSIGNED", copy.get("state").asText());
        assertTrue(copy.get("node").isNull());
        assertFalse(copy.has("allocation_id"));
        assertTrue(copy.get("primary").asBoolean());
    }

    @Test
    void aClosedIndexHasNoRoutingTable() throws Exception {
        create("shut", "{}");
        router.dispatch("POST", "/shut/_close");
        freshCluster();

        assertEquals(0, state("/_cluster/state/routing_table").get("routing_table").get("indices").size());
    }

    @Test
    void routingNodesGroupsCopiesByNodeAndListsEveryDataNode() throws Exception {
        String uuid = create("logs", "{\"settings\":{\"number_of_shards\":1,\"number_of_replicas\":1}}");
        projection("logs", uuid, 1, 0, List.of(List.of("PRIMARY:STARTED:n1:alloc-1")));
        freshCluster();
        node(TENANT, "n1", "data-1", "10.0.0.1", 9300, "[\"data\"]", "{}");
        node(TENANT, "n2", "data-2", "10.0.0.2", 9300, "[\"data\"]", "{}");
        node(TENANT, "c1", "coord-1", "10.0.0.3", 9300, "[\"coordinator\"]", "{}");

        JsonNode routingNodes = state("/_cluster/state").get("routing_nodes");
        assertEquals(List.of("unassigned", "nodes"), keys(routingNodes));
        assertEquals(1, routingNodes.get("unassigned").size(), "the replica");
        assertEquals(List.of("n1", "n2"), keys(routingNodes.get("nodes")), "data nodes only, the idle one with no copies");
        assertEquals(1, routingNodes.get("nodes").get("n1").size());
        assertEquals(0, routingNodes.get("nodes").get("n2").size());
    }

    /** Without the nodes themselves in the response, only nodes that hold something appear. */
    @Test
    void routingNodesAloneListsOnlyNodesHoldingCopies() throws Exception {
        String uuid = create("logs", "{\"settings\":{\"number_of_shards\":1,\"number_of_replicas\":0}}");
        projection("logs", uuid, 0, 0, List.of(List.of("PRIMARY:STARTED:n1:alloc-1")));
        freshCluster();
        node(TENANT, "n1", "data-1", "10.0.0.1", 9300, "[\"data\"]", "{}");
        node(TENANT, "n2", "data-2", "10.0.0.2", 9300, "[\"data\"]", "{}");

        assertEquals(List.of("n1"), keys(state("/_cluster/state/routing_nodes").get("routing_nodes").get("nodes")));
    }

    @Test
    void inSyncAllocationsComeFromTheDocumentReplicationPrimary() throws Exception {
        String uuid = create("logs", "{\"settings\":{\"number_of_shards\":1,\"number_of_replicas\":0}}");
        projection("logs", uuid, 0, 0, List.of(List.of("PRIMARY:STARTED:n1:alloc-1")));
        create("plain", "{}");
        freshCluster();

        JsonNode indices = state("/_cluster/state/metadata").get("metadata").get("indices");
        assertEquals("[\"alloc-1\"]", indices.get("logs").get("in_sync_allocations").get("0").toString());
        assertFalse(indices.get("plain").has("in_sync_allocations"), "absent rather than empty: nothing claims no copy is in sync");
    }

    // ------------------------------------------------------------------ blocks

    @Test
    void noBlocksIsAnEmptyObject() throws Exception {
        create("logs", "{}");
        freshCluster();

        JsonNode blocks = state("/_cluster/state/blocks").get("blocks");
        assertTrue(blocks.isObject() && blocks.isEmpty());
    }

    @Test
    void aClosedIndexAndTheBlockSettingsMakeIndexBlocks() throws Exception {
        create("shut", "{}");
        router.dispatch("POST", "/shut/_close");
        create("frozen", "{\"settings\":{\"blocks.read_only\":true,\"blocks.metadata\":true}}");
        create("quiet", "{}");
        freshCluster();

        JsonNode blocks = state("/_cluster/state/blocks").get("blocks");
        assertEquals(List.of("indices"), keys(blocks));
        assertEquals(List.of("frozen", "shut"), keys(blocks.get("indices")), "only indices that have a block");

        JsonNode closed = blocks.get("indices").get("shut").get("4");
        assertEquals(List.of("description", "retryable", "levels"), keys(closed));
        assertEquals("index closed", closed.get("description").asText());
        assertEquals("[\"read\",\"write\"]", closed.get("levels").toString());

        JsonNode frozen = blocks.get("indices").get("frozen");
        assertEquals(List.of("5", "9"), keys(frozen));
        assertEquals("index read-only (api)", frozen.get("5").get("description").asText());
        assertEquals("[\"write\",\"metadata_write\"]", frozen.get("5").get("levels").toString());
        assertEquals("[\"metadata_read\",\"metadata_write\"]", frozen.get("9").get("levels").toString(), "enum order, as upstream writes them");
    }

    /** Index blocks are not filtered by the index expression: upstream copies them whole. */
    @Test
    void blocksIgnoreTheIndexExpression() throws Exception {
        create("shut", "{}");
        router.dispatch("POST", "/shut/_close");
        create("other", "{}");

        assertEquals(List.of("shut"), keys(state("/_cluster/state/blocks/other").get("blocks").get("indices")));
    }

    // ------------------------------------------------------------------ version and state_uuid

    @Test
    void aWriteMovesBothAndAnUnchangedStoreMovesNeither() throws Exception {
        create("logs", "{}");
        JsonNode before = state("/_cluster/state/version");
        JsonNode again = state("/_cluster/state/version");
        assertEquals(before.get("version").asLong(), again.get("version").asLong());
        assertEquals(before.get("state_uuid").asText(), again.get("state_uuid").asText());

        create("more", "{}");
        JsonNode after = state("/_cluster/state/version");
        assertTrue(after.get("version").asLong() > before.get("version").asLong());
        assertNotEquals(before.get("state_uuid").asText(), after.get("state_uuid").asText());
    }

    /** The version can fall when the newest document is removed, but the state_uuid always changes. */
    @Test
    void aRemovalChangesTheStateUuid() throws Exception {
        create("keep", "{}");
        create("gone", "{}");
        String before = state("/_cluster/state/version").get("state_uuid").asText();

        router.dispatch("DELETE", "/gone");
        assertNotEquals(before, state("/_cluster/state/version").get("state_uuid").asText());
    }

    /** status/cluster is rewritten every heartbeat whether or not anything changed. */
    @Test
    void theSupervisorsHeartbeatDoesNotMoveTheVersion() throws Exception {
        create("logs", "{}");
        freshCluster();
        JsonNode before = state("/_cluster/state/version");

        freshCluster();
        JsonNode after = state("/_cluster/state/version");
        assertEquals(before.get("version").asLong(), after.get("version").asLong());
        assertEquals(before.get("state_uuid").asText(), after.get("state_uuid").asText());
    }

    @Test
    void aPlacementChangeMovesTheStateUuidButNotTheMetadataVersion() throws Exception {
        String uuid = create("logs", "{\"settings\":{\"number_of_shards\":1,\"number_of_replicas\":0}}");
        freshCluster();
        String before = state("/_cluster/state/version").get("state_uuid").asText();

        projection("logs", uuid, 0, 0, List.of(List.of("PRIMARY:STARTED:n1:alloc-1")));
        assertNotEquals(before, state("/_cluster/state/version").get("state_uuid").asText());
    }

    // ------------------------------------------------------------------ waiting for a metadata version

    @Test
    void waitingForAVersionAlreadyReachedAnswersAtOnce() throws Exception {
        create("logs", "{}");
        long version = state("/_cluster/state/version").get("version").asLong();

        JsonNode state = state("/_cluster/state/metadata?wait_for_metadata_version=1&wait_for_timeout=50ms");
        assertEquals(List.of("wait_for_timed_out", "cluster_name", "cluster_uuid", "metadata"), keys(state));
        assertFalse(state.get("wait_for_timed_out").asBoolean());
        assertTrue(version >= 1);
    }

    @Test
    void waitForMetadataVersionWaitsForAWrite() throws Exception {
        create("logs", "{}");
        long next = state("/_cluster/state/version").get("version").asLong() + 1;
        Thread writer = Thread.ofVirtual().start(() -> {
            try {
                Thread.sleep(150);
                create("later", "{}");
            } catch (Exception e) {
                throw new AssertionError(e);
            }
        });

        JsonNode state = state("/_cluster/state/metadata?wait_for_metadata_version=" + next + "&wait_for_timeout=5s");
        writer.join();
        assertFalse(state.get("wait_for_timed_out").asBoolean());
        assertTrue(state.get("metadata").get("indices").has("later"));
    }

    /** Upstream builds this response with no state to describe; see the class note on what it does then. */
    @Test
    void aWaitThatRunsOutSaysSo() throws Exception {
        create("logs", "{}");
        freshCluster();

        JsonNode state = state("/_cluster/state?wait_for_metadata_version=999999999999&wait_for_timeout=100ms");
        assertEquals(List.of("wait_for_timed_out", "cluster_name", "cluster_uuid"), keys(state));
        assertTrue(state.get("wait_for_timed_out").asBoolean());
    }

    // ------------------------------------------------------------------ no supervisor

    /** What the tenant's own metadata says needs no supervisor, and is the commonest use of this API. */
    @Test
    void metadataIsServedWithNoSupervisor() throws Exception {
        create("logs", "{}");

        assertTrue(state("/_cluster/state/metadata/logs").get("metadata").get("indices").has("logs"));
    }

    @Test
    void routingAndNodesWaitForASupervisorThenAre503() throws Exception {
        create("logs", "{}");

        for (String metric : List.of("nodes", "routing_table", "routing_nodes", "_all")) {
            RestResponse response = get("/_cluster/state/" + metric + "?cluster_manager_timeout=50ms");
            assertEquals(RestStatus.SERVICE_UNAVAILABLE, response.status(), metric);
            JsonNode error = MAPPER.readTree(response.body()).get("error");
            assertEquals("cluster_manager_not_discovered_exception", error.get("type").asText());
            assertTrue(error.get("reason").isNull());
        }
    }

    @Test
    void aStaleSupervisorIsNoSupervisor() throws Exception {
        create("logs", "{}");
        staleCluster();

        assertEquals(RestStatus.SERVICE_UNAVAILABLE, get("/_cluster/state/nodes?cluster_manager_timeout=50ms").status());
    }

    @Test
    void localAnswersTheLastDocumentsWithTheNoClusterManagerBlock() throws Exception {
        create("logs", "{}");
        staleCluster();
        node(TENANT, "n1", "data-1", "10.0.0.1", 9300, "[\"data\"]", "{}");

        JsonNode state = state("/_cluster/state?local=true");
        assertTrue(state.get("nodes").has("n1"));
        JsonNode block = state.get("blocks").get("global").get("2");
        assertEquals("no cluster-manager", block.get("description").asText());
        assertTrue(block.get("retryable").asBoolean());
        assertEquals("[\"write\",\"metadata_write\"]", block.get("levels").toString());
    }

    @Test
    void aHealthySupervisorHasNoGlobalBlock() throws Exception {
        create("logs", "{}");
        freshCluster();

        assertFalse(state("/_cluster/state/blocks").get("blocks").has("global"));
    }

    // ------------------------------------------------------------------ rejections

    @Test
    void rejectsWhatUpstreamRejects() throws Exception {
        assertEquals("provided waitForMetadataVersion should be >= 1, but instead is [0]",
            reason(get("/_cluster/state?wait_for_metadata_version=0")));
        assertEquals("Failed to parse long parameter [wait_for_metadata_version] with value [soon]",
            reason(get("/_cluster/state?wait_for_metadata_version=soon")));
        assertEquals("Please only use one of the request parameters [master_timeout, cluster_manager_timeout].",
            reason(get("/_cluster/state?master_timeout=1s&cluster_manager_timeout=1s")));
        assertEquals("failed to parse setting [wait_for_timeout] with value [5] as a time value: unit is missing or unrecognized",
            reason(get("/_cluster/state?wait_for_timeout=5")));
    }

    private static String reason(RestResponse response) throws Exception {
        assertEquals(RestStatus.BAD_REQUEST, response.status());
        return MAPPER.readTree(response.body()).get("error").get("reason").asText();
    }
}
