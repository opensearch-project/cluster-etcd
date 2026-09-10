package org.opensearch.supervisor.core;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.opensearch.controlplane.store.InMemoryKeyValueStore;
import org.opensearch.supervisor.fakenode.FakeNode;
import org.opensearch.supervisor.model.Json;
import org.opensearch.supervisor.model.Keys;
import org.opensearch.supervisor.model.NodeId;
import org.opensearch.supervisor.model.PoolId;
import org.opensearch.supervisor.model.TenantId;
import org.opensearch.supervisor.model.index.IndexConfig;
import org.opensearch.supervisor.model.node.NodeRole;
import org.opensearch.supervisor.model.node.Registration;
import org.opensearch.supervisor.model.plan.Planner;
import org.opensearch.supervisor.model.pool.PoolBinding;
import org.opensearch.supervisor.model.pool.PoolDefinition;
import org.opensearch.supervisor.model.pool.PoolKind;
import org.opensearch.supervisor.model.status.AwarenessStatus;
import org.opensearch.supervisor.model.status.ClusterStatus;
import org.opensearch.supervisor.model.status.HealthStatus;
import org.opensearch.supervisor.model.status.IndexStatus;
import org.opensearch.supervisor.model.status.NodeStatus;
import org.opensearch.supervisor.model.status.Replication;

import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The projections in the store, where the control plane reads them.
 *
 * <p>Each of these unblocks an operation that answers {@code 501} today, so the test that matters is not that
 * a document was written but that what it says is what {@code cluster.health} would need to answer.
 */
class StatusProjectionTests {

    private static final PoolId POOL = PoolId.of("shared-a");
    private static final TenantId ACME = TenantId.of("acme");
    private static final NodeId NODE = NodeId.of("nodeAAAAAAAAAAAAAAAAAA");
    private static final NodeId NODE_B = NodeId.of("nodeBBBBBBBBBBBBBBBBBB");
    private static final String UUID = "uzKxLWZu4nPSrTknEcDpoA";
    private static final Duration TTL = Duration.ofSeconds(15);

    private InMemoryKeyValueStore store;
    private MutableClock clock;

    private static final class MutableClock extends Clock {
        private Instant now = Instant.ofEpochMilli(1_000_000);

        @Override
        public ZoneId getZone() {
            return ZoneOffset.UTC;
        }

        @Override
        public Clock withZone(ZoneId zone) {
            return this;
        }

        @Override
        public Instant instant() {
            return now;
        }

        void advance(Duration by) {
            now = now.plus(by);
        }
    }

    @BeforeEach
    void setUp() {
        store = new InMemoryKeyValueStore();
        clock = new MutableClock();
        store.put(Keys.poolDefinition(POOL), new PoolDefinition(POOL, PoolKind.SHARED, false, null, null, java.util.List.of()).toBytes());
        store.put(Keys.poolBinding(ACME), PoolBinding.to(POOL).toBytes());
        store.put(Keys.tenantRegistration(ACME), "{\"tenant\":\"acme\"}".getBytes(StandardCharsets.UTF_8));
    }

    private FakeNode node(NodeId id, String zone) {
        return new FakeNode(store, new Registration(
            id, "placeholder", id.value(), POOL, EnumSet.of(NodeRole.DATA),
            "10.0.0.1", 9300, "3.7.0", Map.of("zone", zone), 5000, 0L
        ), clock, "eph-" + id.value());
    }

    private Supervisor supervisor() {
        return new Supervisor(store, clock, "s1", Set.of(), 1.0, TTL, Balancer.Config.defaults(),
            new Planner.Policy(200, 5, 2), false);
    }

    private void createIndex(String name, String uuid, int shards, int replicas) {
        store.put(Keys.indicesPrefix(ACME) + name, IndexConfig.toDocument(
            name, uuid, true, IndexConfig.settingsOf(shards, replicas, Replication.SEGMENT)
        ));
    }

    private void run(Supervisor supervisor, List<FakeNode> nodes, int passes) {
        for (int pass = 0; pass < passes; pass++) {
            supervisor.tick();
            nodes.forEach(FakeNode::tick);
            clock.advance(Duration.ofSeconds(2));
        }
    }

    private IndexStatus indexStatus() {
        return IndexStatus.from(store.get(Keys.indexStatus(ACME, UUID)).orElseThrow().value());
    }

    private ClusterStatus clusterStatus() {
        return ClusterStatus.from(store.get(Keys.clusterStatus(ACME)).orElseThrow().value());
    }

    // ---------------------------------------------------------------- what the control plane gets

    @Test
    void aConvergedIndexIsProjectedAsGreenWithItsPlacement() {
        FakeNode data = node(NODE, "z1");
        data.start();
        createIndex("my-index", UUID, 2, 0);

        run(supervisor(), List.of(data), 8);

        IndexStatus status = indexStatus();
        assertEquals(HealthStatus.GREEN, status.status());
        assertEquals("my-index", status.index());
        assertEquals(2, status.numberOfShards());
        assertEquals(2, status.activeShards());
        assertEquals(2, status.activePrimaryShards());
        assertEquals(0, status.unassignedShards());
        assertEquals(NODE, status.servingPrimary(0).orElseThrow().nodeId());
        assertEquals(Replication.SEGMENT, status.replication());
    }

    @Test
    void anIndexWithNoShardsAnywhereIsProjectedAsRed() {
        // Nothing is placeable, so this is the state an operator most needs cluster.health to be honest about.
        createIndex("my-index", UUID, 1, 0);

        run(supervisor(), List.of(), 5);

        assertEquals(HealthStatus.RED, indexStatus().status());
        assertEquals(HealthStatus.RED, clusterStatus().status());
    }

    @Test
    void theClusterDocumentIsWhatClusterHealthNeeds() throws Exception {
        FakeNode data = node(NODE, "z1");
        data.start();
        createIndex("my-index", UUID, 2, 1);

        run(supervisor(), List.of(data), 8);

        var document = Json.mapper().readTree(store.get(Keys.clusterStatus(ACME)).orElseThrow().value());
        assertEquals("yellow", document.get("status").asText(), "replicas are not placed yet");
        assertEquals(1, document.get("number_of_nodes").asInt());
        assertEquals(1, document.get("number_of_data_nodes").asInt());
        assertEquals(2, document.get("active_primary_shards").asInt());
        assertEquals(2, document.get("unassigned_shards").asInt());
        assertEquals(1, document.get("indices_by_status").get("yellow").asInt());
    }

    @Test
    void thePerIndexDocumentCarriesTheCountsOnTheWire() throws Exception {
        // Derived rather than stored, but the control plane reads them straight out of the JSON, so their
        // absence would be a silent contract break.
        FakeNode data = node(NODE, "z1");
        data.start();
        createIndex("my-index", UUID, 1, 0);

        run(supervisor(), List.of(data), 8);

        var document = Json.mapper().readTree(store.get(Keys.indexStatus(ACME, UUID)).orElseThrow().value());
        assertTrue(document.has("active_shards"));
        assertTrue(document.has("active_primary_shards"));
        assertTrue(document.has("unassigned_shards"));
        assertTrue(document.has("initializing_shards"));
        assertEquals(1, document.get("active_shards").asInt());
    }

    @Test
    void nodesAreProjectedWithTheAddressAPeerWouldNeed() {
        FakeNode data = node(NODE, "z1");
        data.start();
        createIndex("my-index", UUID, 1, 0);

        run(supervisor(), List.of(data), 8);

        NodeStatus status = NodeStatus.from(store.get(Keys.nodeStatus(ACME, NODE)).orElseThrow().value());
        assertEquals("10.0.0.1", status.address());
        assertEquals(9300, status.port());
        assertEquals(Set.of(NodeRole.DATA), status.roles());
        assertTrue(status.isReachable());
    }

    @Test
    void awarenessValuesAreProjectedForTheControlPlaneToValidateAgainst() {
        // R5.4, and the half of weighted-routing validation the control plane cannot do today.
        FakeNode a = node(NODE, "z1");
        FakeNode b = node(NODE_B, "z2");
        a.start();
        b.start();
        createIndex("my-index", UUID, 2, 0);

        run(supervisor(), List.of(a, b), 10);

        AwarenessStatus awareness = AwarenessStatus.from(store.get(Keys.awarenessStatus(ACME)).orElseThrow().value());
        assertEquals(List.of("z1", "z2"), awareness.attributes().get("zone"));
    }

    // ---------------------------------------------------------------- write behaviour

    @Test
    void aConvergedPoolStopsWritingProjections() {
        // Rewriting a per-index document every pass for ten thousand unchanged indices would make the
        // supervisor the store's dominant writer, ahead of the nodes it is watching.
        FakeNode data = node(NODE, "z1");
        data.start();
        createIndex("my-index", UUID, 1, 0);
        Supervisor supervisor = supervisor();

        run(supervisor, List.of(data), 10);
        long indexRevision = store.get(Keys.indexStatus(ACME, UUID)).orElseThrow().revision();
        long clusterRevision = store.get(Keys.clusterStatus(ACME)).orElseThrow().revision();

        run(supervisor, List.of(data), 10);

        assertEquals(indexRevision, store.get(Keys.indexStatus(ACME, UUID)).orElseThrow().revision());
        assertEquals(clusterRevision, store.get(Keys.clusterStatus(ACME)).orElseThrow().revision());
    }

    @Test
    void aChangeInHealthIsWrittenPromptly() {
        FakeNode data = node(NODE, "z1");
        data.start();
        createIndex("my-index", UUID, 1, 0);
        Supervisor supervisor = supervisor();
        run(supervisor, List.of(data), 8);
        assertEquals(HealthStatus.GREEN, indexStatus().status());

        // The node goes away and stays away long enough to be given up for dead.
        for (int pass = 0; pass < 40; pass++) {
            supervisor.tick();
            clock.advance(Duration.ofSeconds(5));
        }

        assertEquals(HealthStatus.RED, indexStatus().status(), "health must follow reality, not lag it");
    }

    @Test
    void aDeletedIndexsHealthDocumentIsRemovedRatherThanLeftBehind() {
        // The control plane reads this subtree by prefix scan, so a stale document is not untidy -- it is an
        // index that answers cluster.health after it has been deleted.
        FakeNode data = node(NODE, "z1");
        data.start();
        createIndex("my-index", UUID, 1, 0);
        Supervisor supervisor = supervisor();
        run(supervisor, List.of(data), 8);
        assertTrue(store.get(Keys.indexStatus(ACME, UUID)).isPresent());

        store.delete(Keys.indicesPrefix(ACME) + "my-index");
        run(supervisor, List.of(data), 4);

        assertTrue(store.get(Keys.indexStatus(ACME, UUID)).isEmpty());
        assertTrue(store.scanPrefix(Keys.indexStatusPrefix(ACME)).isEmpty());
    }

    @Test
    void aReadOnlySupervisorProjectsNothing() {
        // One document serves health, cluster state, coordinator routing and docrep peer resolution, so health
        // cannot be published without publishing routing that coordinators will act on. Read-only therefore
        // means projecting none of it -- the cost of that four-readers-one-document decision.
        FakeNode data = node(NODE, "z1");
        data.start();
        createIndex("my-index", UUID, 1, 0);

        Supervisor readOnly = new Supervisor(store, clock, "s1", Set.of(), 1.0, TTL,
            Balancer.Config.defaults(), Planner.Policy.defaults(), true);
        run(readOnly, List.of(data), 8);

        assertTrue(store.scanPrefix(Keys.statusPrefix(ACME)).isEmpty());
        assertFalse(readOnly.lastDiffOf(POOL).isEmpty(), "but it still says what is wrong");
    }

    @Test
    void projectionsAreScopedToTheTenantsThisPoolServes() {
        TenantId other = TenantId.of("globex");
        store.put(Keys.tenantRegistration(other), "{\"tenant\":\"globex\"}".getBytes(StandardCharsets.UTF_8));
        store.put(Keys.poolBinding(other), PoolBinding.to(PoolId.of("elsewhere")).toBytes());
        store.put(Keys.indicesPrefix(other) + "theirs", IndexConfig.toDocument(
            "theirs", "globexUuidAAAAAAAAAAA", true, IndexConfig.settingsOf(1, 0, Replication.SEGMENT)
        ));
        createIndex("my-index", UUID, 1, 0);

        FakeNode data = node(NODE, "z1");
        data.start();
        run(supervisor(), List.of(data), 8);

        assertFalse(store.scanPrefix(Keys.statusPrefix(ACME)).isEmpty());
        assertTrue(store.scanPrefix(Keys.statusPrefix(other)).isEmpty(),
            "another pool's tenant is another supervisor's to project");
    }

    @Test
    void projectionsLiveInsideTheTenantPrefixWhereTheControlPlaneCanReachThem() {
        // The whole reason R5.5 is kept one level down rather than literally: the control plane's handlers see
        // a store rooted at tenant/<id>/, so a projection outside it would be unreachable to its only reader.
        FakeNode data = node(NODE, "z1");
        data.start();
        createIndex("my-index", UUID, 1, 0);
        run(supervisor(), List.of(data), 8);

        Set<String> keys = store.scanPrefix(Keys.statusPrefix(ACME)).keySet();
        assertFalse(keys.isEmpty());
        keys.forEach(key -> assertTrue(key.startsWith(Keys.tenantPrefix(ACME)), key));
    }
}
