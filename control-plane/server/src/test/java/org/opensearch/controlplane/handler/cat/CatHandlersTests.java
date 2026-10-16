/*
 * Copyright OpenSearch Software Foundation
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.controlplane.handler.cat;

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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The four tables, each over the supervisor's projections written straight into the store in the form
 * {@code supervisor/SUPERVISOR_KEYS.md} documents. The layout of each was compared with a node's own
 * {@code _cat} output.
 */
class CatHandlersTests {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final String TENANT = "tenant/default/";

    private InMemoryKeyValueStore store;
    private Router router;

    @BeforeEach
    void setUp() {
        store = new InMemoryKeyValueStore();
        router = routerFor(TestContexts.factory(store));
    }

    /** The full registry with the four cat handlers polling every 20 ms, so that waiting tests are quick. */
    private static Router routerFor(OperationContextFactory contexts) {
        Duration poll = Duration.ofMillis(20);
        HandlerRegistry registry = ControlPlaneHandlers.build()
            .register(new CatHealthHandler(ClusterIdentity.defaults(), poll))
            .register(new CatIndicesHandler(poll))
            .register(new CatShardsHandler(poll))
            .register(new CatNodesHandler(poll));
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

    private String table(String path) {
        RestResponse response = get(path);
        assertEquals(RestStatus.OK, response.status(), () -> new String(response.body(), StandardCharsets.UTF_8));
        assertEquals("text/plain; charset=UTF-8", response.contentType());
        return new String(response.body(), StandardCharsets.UTF_8);
    }

    private String create(String index, String body) throws Exception {
        String path = "/" + index;
        RestResponse response = router.dispatch("PUT", path, path, Map.of(), Map.of(), body.getBytes(StandardCharsets.UTF_8));
        assertEquals(RestStatus.OK, response.status(), () -> new String(response.body(), StandardCharsets.UTF_8));
        return MAPPER.readTree(store.get(TENANT + "indices/" + index).orElseThrow().value()).get("uuid").asText();
    }

    private void supervisor(long observedAt, int nodes, int dataNodes) {
        store.put(TENANT + "status/cluster", ("{\"number_of_nodes\":" + nodes + ",\"number_of_data_nodes\":" + dataNodes
            + ",\"observed_at\":" + observedAt + ",\"heartbeat_ms\":10000}").getBytes(StandardCharsets.UTF_8));
    }

    private void freshCluster() {
        supervisor(System.currentTimeMillis(), 3, 3);
    }

    private void node(String prefix, String id, String name, String address, int port, String roles) {
        store.put(prefix + "status/nodes/" + id, ("{\"node_id\":\"" + id + "\",\"ephemeral_id\":\"eph-" + id + "\",\"name\":\""
            + name + "\",\"roles\":" + roles + ",\"address\":\"" + address + "\",\"port\":" + port
            + ",\"version\":\"3.10.0\",\"attributes\":{},\"liveness\":\"live\",\"observed_at\":1}")
            .getBytes(StandardCharsets.UTF_8));
    }

    /** A projection with the status the supervisor would compute; each copy is "ROLE:STATE:node". */
    private void projection(String name, String uuid, String status, int replicas, List<List<String>> shards) {
        StringBuilder placements = new StringBuilder("{");
        int active = 0;
        int primaries = 0;
        int initializing = 0;
        int unassigned = 0;
        for (int shard = 0; shard < shards.size(); shard++) {
            placements.append(shard > 0 ? "," : "").append('"').append(shard).append("\":[");
            List<String> copies = shards.get(shard);
            for (int i = 0; i < copies.size(); i++) {
                String[] copy = copies.get(i).split(":");
                placements.append(i > 0 ? "," : "").append("{\"node_id\":\"").append(copy[2]).append("\",\"role\":\"")
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
            + "\",\"status\":\"" + status + "\",\"number_of_shards\":" + shards.size() + ",\"number_of_replicas\":" + replicas
            + ",\"number_of_search_replicas\":0,\"active_shards\":" + active + ",\"active_primary_shards\":" + primaries
            + ",\"initializing_shards\":" + initializing + ",\"unassigned_shards\":" + unassigned
            + ",\"shards\":" + placements + ",\"observed_at\":1}").getBytes(StandardCharsets.UTF_8));
    }

    /** The same table with runs of spaces collapsed, for tests about cells rather than padding. */
    private String cells(String path) {
        return table(path).replaceAll(" +", " ");
    }

    private static String reason(RestResponse response) throws Exception {
        return MAPPER.readTree(response.body()).get("error").get("reason").asText();
    }

    // ------------------------------------------------------------------ _cat/health

    @Test
    void healthIsOneRowOfTheFiguresClusterHealthReports() throws Exception {
        String uuid = create("logs", "{\"settings\":{\"number_of_shards\":1,\"number_of_replicas\":1}}");
        projection("logs", uuid, "yellow", 1, List.of(List.of("PRIMARY:STARTED:n1")));
        supervisor(System.currentTimeMillis(), 2, 2);

        String[] lines = cells("/_cat/health?v&ts=false").split("\n");
        assertEquals(2, lines.length);
        assertEquals("cluster status node.total node.data discovered_cluster_manager shards pri relo init unassign "
            + "pending_tasks max_task_wait_time active_shards_percent", lines[0]);
        // yellow: a copy is missing. One of the two wanted is active, and the create-index task is still pending.
        String[] cells = lines[1].split(" ");
        assertEquals(List.of("opensearch", "yellow", "2", "2", "true", "1", "1", "0", "0", "1", "1"), List.of(cells).subList(0, 11));
        assertTrue(cells[11].matches("\\d+(\\.\\d)?(ms|s|m|h|d|micros|nanos)|-"), "a time value, or - for none: " + cells[11]);
        assertEquals("50.0%", cells[12]);
    }

    @Test
    void healthStartsWithEpochAndTimestampUnlessTsIsFalse() {
        freshCluster();

        String header = table("/_cat/health?v").split("\n")[0];
        assertTrue(header.startsWith("epoch      timestamp cluster"), header);
        assertTrue(table("/_cat/health?v&ts=false").startsWith("cluster"));
        String row = table("/_cat/health").split("\n")[0];
        assertTrue(row.matches("\\d{10} \\d\\d:\\d\\d:\\d\\d opensearch green .*"), row);
    }

    @Test
    void healthColumnsCanBeChosenByAliasAndReadAsJson() throws Exception {
        freshCluster();

        JsonNode json = MAPPER.readTree(get("/_cat/health?format=json&h=st,nt,dcm").body());
        assertEquals("[{\"st\":\"green\",\"nt\":\"3\",\"dcm\":\"true\"}]", json.toString());
    }

    // ------------------------------------------------------------------ _cat/indices

    @Test
    void indicesPrintsOneRowPerIndexWithTheStatisticsAsX() throws Exception {
        String uuid = create("logs", "{\"settings\":{\"number_of_shards\":2,\"number_of_replicas\":1}}");
        projection("logs", uuid, "yellow", 1, List.of(List.of("PRIMARY:STARTED:n1", "REPLICA:STARTED:n2"), List.of("PRIMARY:STARTED:n2")));
        create("more", "{}");
        freshCluster();

        assertEquals(
            "health status index uuid" + " ".repeat(18) + " pri rep docs.count docs.deleted store.size pri.store.size\n"
                + "yellow open   logs  " + uuid + "   2   1          x            x          x              x\n"
                + "red    open   more  " + MAPPER.readTree(store.get(TENANT + "indices/more").orElseThrow().value()).get("uuid").asText()
                + "   1   1          x            x          x              x\n",
            table("/_cat/indices?v"));
    }

    @Test
    void aClosedIndexIsStatusCloseWithBlankStatistics() throws Exception {
        create("shut", "{\"settings\":{\"number_of_shards\":1,\"number_of_replicas\":0}}");
        router.dispatch("POST", "/shut/_close");
        freshCluster();

        // The blank statistics cells leave their separators behind, so the line ends in two spaces.
        assertEquals("green close shut 1 0  \n", table("/_cat/indices?h=health,status,index,pri,rep,docs.count,store.size"),
            "health stays, the statistics are blank");
    }

    @Test
    void indicesAreSortedByNameAndHiddenOnesLeftOutUnlessAsked() throws Exception {
        create("b-index", "{}");
        create("a-index", "{}");
        create("hidden-one", "{\"settings\":{\"hidden\":true}}");
        freshCluster();

        assertEquals("a-index\nb-index\n", table("/_cat/indices?h=index"));
        assertEquals("a-index\nb-index\nhidden-one\n", table("/_cat/indices?h=index&expand_wildcards=all"));
        assertEquals("hidden-one\n", table("/_cat/indices/hidden-one?h=index"), "naming it finds it");
    }

    @Test
    void anExpressionNarrowsAndAMissingNameIs404() throws Exception {
        create("logs-1", "{}");
        create("logs-2", "{}");
        create("other", "{}");
        freshCluster();

        assertEquals("logs-1\nlogs-2\n", table("/_cat/indices/logs-*?h=index"));
        assertEquals("", table("/_cat/indices/nothing-*?h=index"), "a wildcard matching nothing is an empty table");
        RestResponse missing = get("/_cat/indices/nonesuch");
        assertEquals(RestStatus.NOT_FOUND, missing.status());
        assertEquals("no such index [nonesuch]", reason(missing));
    }

    @Test
    void healthFiltersRowsAndRejectsAnUnknownStatus() throws Exception {
        String uuid = create("fine", "{\"settings\":{\"number_of_shards\":1,\"number_of_replicas\":0}}");
        projection("fine", uuid, "green", 0, List.of(List.of("PRIMARY:STARTED:n1")));
        create("fresh", "{}");
        freshCluster();

        assertEquals("fresh\n", table("/_cat/indices?h=index&health=red"));
        assertEquals("fresh\n", table("/_cat/indices?h=index&health=RED"), "case-insensitive");
        RestResponse bad = get("/_cat/indices?health=blue");
        assertEquals(RestStatus.BAD_REQUEST, bad.status());
        assertEquals("unknown cluster health status [blue]", reason(bad));
    }

    @Test
    void creationDateIsAvailableAsMillisAndAsAString() throws Exception {
        create("logs", "{}");
        freshCluster();

        String[] cells = table("/_cat/indices?h=cd,cds").trim().split(" +");
        assertTrue(cells[0].matches("\\d{13}"), cells[0]);
        assertTrue(cells[1].matches("\\d{4}-\\d\\d-\\d\\dT\\d\\d:\\d\\d:\\d\\d\\.\\d{3}Z"), cells[1]);
    }

    @Test
    void indicesCanBeSortedByAColumnAndByAnUnshownOne() throws Exception {
        create("small", "{\"settings\":{\"number_of_shards\":1}}");
        create("big", "{\"settings\":{\"number_of_shards\":5}}");
        freshCluster();

        assertEquals("big\nsmall\n", table("/_cat/indices?h=index&s=pri:desc"), "numerically: 5 before 1");
        assertEquals("small\nbig\n", table("/_cat/indices?h=index&s=pri"));
    }

    // ------------------------------------------------------------------ _cat/shards

    @Test
    void shardsListsEveryCopyIncludingTheUnassignedOnes() throws Exception {
        String uuid = create("logs", "{\"settings\":{\"number_of_shards\":1,\"number_of_replicas\":2}}");
        projection("logs", uuid, "yellow", 2, List.of(List.of("REPLICA:STARTED:n2", "PRIMARY:STARTED:n1")));
        freshCluster();
        node(TENANT, "n1", "data-1", "10.0.0.1", 9300, "[\"data\"]");
        node(TENANT, "n2", "data-2", "10.0.0.2", 9300, "[\"data\"]");

        assertEquals(
            "index shard prirep state      docs store ip       node\n"
                + "logs  0     p      STARTED       x     x 10.0.0.1 data-1\n"
                + "logs  0     r      STARTED       x     x 10.0.0.2 data-2\n"
                + "logs  0     r      UNASSIGNED" + " ".repeat(21) + "\n",
            table("/_cat/shards?v"));
    }

    @Test
    void shardsCanShowTheFullNodeIdAndAreFilteredByIndex() throws Exception {
        String uuid = create("logs", "{\"settings\":{\"number_of_shards\":1,\"number_of_replicas\":0}}");
        projection("logs", uuid, "green", 0, List.of(List.of("PRIMARY:STARTED:n1")));
        create("other", "{\"settings\":{\"number_of_replicas\":0}}");
        freshCluster();
        node(TENANT, "n1", "data-1", "10.0.0.1", 9300, "[\"data\"]");

        assertEquals("logs 0 n1 data-1\n", cells("/_cat/shards/logs?h=index,shard,id,node"));
        assertEquals("", table("/_cat/shards/nonesuch?h=index"), "lenient: a missing name is skipped, not a 404");
    }

    @Test
    void aSearchReplicaIsPrirepS() throws Exception {
        String uuid = create("logs", "{\"settings\":{\"number_of_shards\":1,\"number_of_replicas\":0,\"number_of_search_replicas\":1}}");
        projection("logs", uuid, "green", 0, List.of(List.of("PRIMARY:STARTED:n1", "SEARCH_REPLICA:STARTED:n2")));
        freshCluster();

        assertEquals("p\ns\n", table("/_cat/shards?h=prirep"));
    }

    @Test
    void aClosedIndexHasNoShards() throws Exception {
        create("shut", "{}");
        router.dispatch("POST", "/shut/_close");
        freshCluster();

        assertEquals("", table("/_cat/shards"));
    }

    // ------------------------------------------------------------------ _cat/nodes

    @Test
    void nodesPrintsTheTenantsOwnNodesSortedByName() {
        freshCluster();
        node(TENANT, "zz-node-id", "beta", "10.0.0.2", 9300, "[\"data\"]");
        node(TENANT, "aa-node-id", "alpha", "10.0.0.1", 9300, "[\"data\",\"coordinator\"]");
        node(TENANT, "cc-node-id", "gamma", "10.0.0.3", 9300, "[\"coordinator\"]");

        assertEquals(
            "ip heap.percent ram.percent cpu load_1m load_5m load_15m node.role node.roles cluster_manager name\n"
                + "10.0.0.1 x x x x x x d coordinator,data x alpha\n"
                + "10.0.0.2 x x x x x x d data x beta\n"
                + "10.0.0.3 x x x x x x - coordinator x gamma\n",
            cells("/_cat/nodes?v"));
    }

    @Test
    void nodeIdIsFourCharactersUnlessFullIdIsAsked() {
        freshCluster();
        node(TENANT, "pv1sDMfZRxSTMiW5VrbN9w", "data-1", "10.0.0.1", 9300, "[\"data\"]");

        assertEquals("pv1s\n", table("/_cat/nodes?h=id"));
        assertEquals("pv1sDMfZRxSTMiW5VrbN9w\n", table("/_cat/nodes?h=id&full_id=true"));
        assertEquals("9300 3.10.0\n", cells("/_cat/nodes?h=port,version"));
    }

    @Test
    void noClusterManagerIsElectedAndTheTableSaysSoWithX() {
        freshCluster();
        node(TENANT, "n1", "data-1", "10.0.0.1", 9300, "[\"data\"]");

        assertEquals("x\n", table("/_cat/nodes?h=master"), "alias m, cm and master all name cluster_manager");
    }

    @Test
    void anotherTenantsNodesAreNotListed() throws Exception {
        router = routerFor(TestContexts.factory(store, new HeaderTenantResolver()));
        for (String tenant : List.of("acme", "globex")) {
            store.put("tenant/" + tenant + "/status/cluster", ("{\"observed_at\":" + System.currentTimeMillis()
                + ",\"heartbeat_ms\":10000}").getBytes(StandardCharsets.UTF_8));
            node("tenant/" + tenant + "/", tenant + "-id", tenant + "-node", "10.0.0.1", 9300, "[\"data\"]");
        }

        RestResponse response = get(router, "/_cat/nodes?h=name", Map.of("x-tenant-id", "acme"));
        assertEquals("acme-node\n", new String(response.body(), StandardCharsets.UTF_8));
    }

    // ------------------------------------------------------------------ all four

    @Test
    void helpNeedsNoSupervisor() {
        for (String api : List.of("health", "indices", "shards", "nodes")) {
            RestResponse response = get("/_cat/" + api + "?help");
            assertEquals(RestStatus.OK, response.status(), api);
            assertTrue(new String(response.body(), StandardCharsets.UTF_8).contains(" | "), api);
        }
    }

    @Test
    void withNoSupervisorEachWaitsThenIs503() throws Exception {
        create("logs", "{}");

        for (String api : List.of("health", "indices", "shards", "nodes")) {
            RestResponse response = get("/_cat/" + api + "?cluster_manager_timeout=50ms");
            assertEquals(RestStatus.SERVICE_UNAVAILABLE, response.status(), api);
            JsonNode error = MAPPER.readTree(response.body()).get("error");
            assertEquals("cluster_manager_not_discovered_exception", error.get("type").asText(), api);
            assertTrue(error.get("reason").isNull());
        }
    }

    @Test
    void aStaleSupervisorIsNoSupervisorAndLocalTakesTheLastDocuments() throws Exception {
        create("logs", "{}");
        supervisor(System.currentTimeMillis() - 120_000, 3, 3);
        node(TENANT, "n1", "data-1", "10.0.0.1", 9300, "[\"data\"]");

        assertEquals(RestStatus.SERVICE_UNAVAILABLE, get("/_cat/nodes?cluster_manager_timeout=50ms").status());
        assertEquals("data-1\n", table("/_cat/nodes?h=name&local=true"));
        assertTrue(table("/_cat/health?h=st,dcm&local=true").startsWith("red false"), "red, and no cluster manager discovered");
    }

    @Test
    void anUnknownSortKeyIs400WhereUpstreamsIs500() throws Exception {
        freshCluster();

        RestResponse response = get("/_cat/indices?s=nonesuch");
        assertEquals(RestStatus.BAD_REQUEST, response.status());
        assertEquals("Unable to sort by unknown sort key `nonesuch`", reason(response));
    }

    @Test
    void rejectsBothSpellingsOfTheManagerTimeout() throws Exception {
        assertEquals("Please only use one of the request parameters [master_timeout, cluster_manager_timeout].",
            reason(get("/_cat/shards?master_timeout=1s&cluster_manager_timeout=1s")));
    }
}
