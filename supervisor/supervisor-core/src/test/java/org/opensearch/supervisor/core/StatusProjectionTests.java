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
import org.opensearch.supervisor.model.node.Assignment;
import org.opensearch.supervisor.model.node.NodeJson;
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
import static org.junit.jupiter.api.Assertions.assertNotEquals;
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
        // Declares `zone` as its awareness attribute, which is what a pool wanting zone-weighted routing does.
        // status/awareness offers only the declared attributes, so a fixture declaring none would be modelling a
        // pool with no valid routing dimensions at all.
        store.put(Keys.poolDefinition(POOL), new PoolDefinition(
            POOL, PoolKind.SHARED, false, null, null, java.util.List.of("zone"), false).toBytes());
        store.put(Keys.poolBinding(ACME), PoolBinding.to(POOL).toBytes());
        store.put(Keys.tenantRegistration(ACME), "{\"tenant\":\"acme\"}".getBytes(StandardCharsets.UTF_8));
    }

    private FakeNode node(NodeId id, String zone) {
        return new FakeNode(store, new Registration(
            id, "placeholder", id.value(), POOL, EnumSet.of(NodeRole.DATA),
            // One port per node. One process owns a socket, so two nodes sharing address:port means one of them
            // is a corpse -- and the projection now publishes only the freshest of them, which quietly dropped a
            // node from this fixture until each got its own.
            "10.0.0.1", portOf(id), "3.7.0",
            Map.of("zone", zone,
                org.opensearch.supervisor.model.allocate.RemoteStoreDecider.SEGMENT_REPOSITORY_ATTRIBUTE,
                "segments"),
            5000, 0L
        ), clock, "eph-" + id.value());
    }

    /** One port per node: one process owns a socket, so a shared endpoint means one of them is a corpse. */
    private static int portOf(NodeId id) {
        return 9300 + Math.abs(id.value().hashCode() % 100);
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
        assertEquals(portOf(NODE), status.port(), "whatever port the fixture gave it, the projection carries it");
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

    @Test
    void anAttributeThePoolDoesNotSpreadAcrossIsNotOfferedAsARoutingDimension() {
        // It offered every attribute a node carried, so `remote_store.segment.repository` appeared beside `zone` as
        // though a client could weight traffic by which bucket the segments are in. Two costs: a weighted-routing
        // request that is accepted and does nothing, because the allocator spreads across the declared attributes
        // and no others; and in a shared pool, telling one tenant the repository name the pool writes to.
        FakeNode a = node(NODE, "z1");
        a.start();
        createIndex("my-index", UUID, 1, 0);

        run(supervisor(), List.of(a), 10);

        AwarenessStatus awareness = AwarenessStatus.from(store.get(Keys.awarenessStatus(ACME)).orElseThrow().value());
        assertEquals(List.of("zone"), List.copyOf(awareness.attributes().keySet()),
            "only the pool's declared awareness attributes belong here");
    }

    // ---------------------------------------------------------------- write behaviour

    @Test
    void aConvergedPoolStopsWritingProjectionsOtherThanTheClusterHeartbeat() {
        // Rewriting a per-index document every pass for ten thousand unchanged indices would make the
        // supervisor the store's dominant writer, ahead of the nodes it is watching. A minute of quiet passes
        // writes the index and node documents not at all, and the one small cluster document once per heartbeat.
        FakeNode data = node(NODE, "z1");
        data.start();
        createIndex("my-index", UUID, 1, 0);
        Supervisor supervisor = supervisor();

        run(supervisor, List.of(data), 10);
        long indexRevision = store.get(Keys.indexStatus(ACME, UUID)).orElseThrow().revision();
        long nodeRevision = store.get(Keys.nodeStatus(ACME, NODE)).orElseThrow().revision();
        List<Long> clusterWrites = new java.util.ArrayList<>();
        for (int pass = 0; pass < 30; pass++) {
            long before = store.get(Keys.clusterStatus(ACME)).orElseThrow().revision();
            run(supervisor, List.of(data), 1);
            if (store.get(Keys.clusterStatus(ACME)).orElseThrow().revision() != before) {
                clusterWrites.add(clusterStatus().observedAt());
            }
        }

        assertEquals(indexRevision, store.get(Keys.indexStatus(ACME, UUID)).orElseThrow().revision());
        assertEquals(nodeRevision, store.get(Keys.nodeStatus(ACME, NODE)).orElseThrow().revision());
        assertEquals(6, clusterWrites.size(), "60 seconds of 2-second passes is six 10-second heartbeats: " + clusterWrites);
    }

    @Test
    void anUnchangedClusterDocumentIsRewrittenOnceTheHeartbeatHasPassedAndNotBefore() {
        // Control plane finding 1. Written only on change, observed_at stopped moving on a quiet tenant, so a
        // healthy supervisor with nothing to say looked exactly like no supervisor at all.
        FakeNode data = node(NODE, "z1");
        data.start();
        createIndex("my-index", UUID, 1, 0);
        Supervisor supervisor = supervisor();
        run(supervisor, List.of(data), 10);

        // Line up on a write, so the heartbeat's start is known. Bounded: with no heartbeat there is never another.
        long revision = store.get(Keys.clusterStatus(ACME)).orElseThrow().revision();
        for (int pass = 0; pass < 10 && store.get(Keys.clusterStatus(ACME)).orElseThrow().revision() == revision; pass++) {
            run(supervisor, List.of(data), 1);
        }
        assertNotEquals(revision, store.get(Keys.clusterStatus(ACME)).orElseThrow().revision(),
            "20 seconds of quiet passes and status/cluster was never rewritten");
        ClusterStatus written = clusterStatus();
        long writtenRevision = store.get(Keys.clusterStatus(ACME)).orElseThrow().revision();

        run(supervisor, List.of(data), 4);
        assertEquals(writtenRevision, store.get(Keys.clusterStatus(ACME)).orElseThrow().revision(),
            "8 seconds on, nothing has changed and the heartbeat has not passed");

        run(supervisor, List.of(data), 1);
        ClusterStatus heartbeat = clusterStatus();
        assertEquals(written.observedAt() + 10_000, heartbeat.observedAt(), "10 seconds on, it is written again");
        assertTrue(heartbeat.observedPass() > written.observedPass());
        assertEquals(10_000, heartbeat.heartbeatMs(), "and it says how often a reader may expect it");
        assertEquals(written.withHeartbeatMs(0), new ClusterStatus(heartbeat.status(), heartbeat.numberOfNodes(),
            heartbeat.numberOfDataNodes(), heartbeat.activeShards(), heartbeat.activePrimaryShards(),
            heartbeat.unassignedShards(), heartbeat.initializingShards(), heartbeat.indicesByStatus(),
            written.observedAt(), written.observedPass()), "with the same content");
    }

    @Test
    void theClusterDocumentIsWrittenAfterEveryOtherDocumentOfItsTenant() {
        // The control plane reads a fresh status/cluster as the tenant's freshness, so it must vouch for the whole
        // pass: every index and node document written, and every gone one removed, before it.
        FakeNode data = node(NODE, "z1");
        data.start();
        createIndex("my-index", UUID, 1, 0);
        run(supervisor(), List.of(data), 10);

        List<String> order = new java.util.ArrayList<>();
        org.opensearch.controlplane.store.KeyValueStore recording = new org.opensearch.controlplane.store.KeyValueStore() {
            @Override
            public java.util.Optional<Versioned> get(String key) {
                return store.get(key);
            }

            @Override
            public java.util.NavigableMap<String, Versioned> scanPrefix(String prefix) {
                return store.scanPrefix(prefix);
            }

            @Override
            public long put(String key, byte[] value) {
                order.add("put " + key);
                return store.put(key, value);
            }

            @Override
            public long put(String key, byte[] value, long expectedRevision) {
                order.add("put " + key);
                return store.put(key, value, expectedRevision);
            }

            @Override
            public boolean delete(String key) {
                order.add("delete " + key);
                return store.delete(key);
            }

            @Override
            public boolean delete(String key, long expectedRevision) {
                order.add("delete " + key);
                return store.delete(key, expectedRevision);
            }
        };
        store.put(Keys.indexStatus(ACME, "goneGoneGoneGoneGoneGo"), "{}".getBytes(StandardCharsets.UTF_8));
        var observation = new Observer(store, clock, new LivenessTracker(clock.instant()))
            .observe(POOL, Pools.read(store), Set.of(ACME));

        // A fresh projector writes everything, which is what a new lease holder's first pass does.
        new StatusProjector(recording).project(observation);

        assertEquals("put " + Keys.clusterStatus(ACME), order.get(order.size() - 1), order.toString());
        assertTrue(order.contains("put " + Keys.indexStatus(ACME, UUID)), order.toString());
        assertTrue(order.contains("put " + Keys.nodeStatus(ACME, NODE)), order.toString());
        assertTrue(order.contains("delete " + Keys.indexStatus(ACME, "goneGoneGoneGoneGoneGo")), order.toString());
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

    @Test
    void aNodesProjectionCarriesTheEphemeralIdItRegisteredWith() {
        // Without it a reader can locate a peer and still be refused by the transport: DiscoveryNode.equals
        // compares only the ephemeral id, and the connection validator rejects a handshake whose remote node is
        // unequal to the one it was handed. An address is necessary and not sufficient.
        FakeNode node = node(NODE, "z1");
        node.start();
        createIndex("my-index", UUID, 1, 0);
        run(supervisor(), List.of(node), 8);

        NodeStatus status = NodeStatus.from(store.get(Keys.nodeStatus(ACME, NODE)).orElseThrow().value());

        assertEquals(node.ephemeralId(), status.ephemeralId());
    }

    @Test
    void restartingANodeUpdatesTheProjectedEphemeralId() {
        // It identifies an incarnation, so it changes on every restart by design. A projection left holding the
        // old one is a coordinator that cannot connect until it catches up.
        FakeNode first = node(NODE, "z1");
        first.start();
        createIndex("my-index", UUID, 1, 0);
        Supervisor supervisor = supervisor();
        run(supervisor, List.of(first), 8);
        String before = NodeStatus.from(store.get(Keys.nodeStatus(ACME, NODE)).orElseThrow().value()).ephemeralId();

        FakeNode restarted = new FakeNode(store, new Registration(
            NODE, "placeholder", NODE.value(), POOL, EnumSet.of(NodeRole.DATA),
            "10.0.0.1", 9300, "3.7.0", Map.of("zone", "z1"), 5000, 0L
        ), clock, "eph-second-incarnation");
        restarted.start();
        run(supervisor, List.of(restarted), 8);
        String after = NodeStatus.from(store.get(Keys.nodeStatus(ACME, NODE)).orElseThrow().value()).ephemeralId();

        assertEquals("eph-second-incarnation", after);
        assertNotEquals(before, after, "a restart must not leave the previous incarnation projected");
    }

    // ------------------------------- a node replaced on the same port (FINDINGS 7, reported from a real pool)

    @Test
    void onlyOneNodePerEndpointIsProjected() {
        // What a coordinator actually hit: "[127.0.0.1:9300] is already claimed by node [ENQg...], so this one is
        // not routed to -- usually a projection for a node that was replaced on the same port and has not been
        // reaped yet". Nothing reaps a departed node's registration, and a fresh supervisor process grants every
        // corpse a full staleness window because silence is measured from when *this* process started watching.
        // So both are projected, both carry one address and port, and the reader cannot tell which is real.
        NodeId dead = NodeId.of("nodeDeadAAAAAAAAAAAAAA");
        NodeId live = NodeId.of("nodeLiveBBBBBBBBBBBBBB");

        // Same endpoint, which is the whole point: one process owns a socket.
        store.put(Keys.nodeRegistration(dead), NodeJson.write(new Registration(
            dead, "eph-dead", dead.value(), POOL, EnumSet.of(NodeRole.DATA), "127.0.0.1", 9300, "3.7.0",
            Map.of("zone", "z1"), 5000, 0L
        )));
        // And the assignment it was left holding, which is what a replaced node leaves behind and what makes it
        // still count as serving the tenant. Without this it is not projected at all and the collision cannot
        // happen -- the first version of this test proved nothing for exactly that reason.
        store.put(Keys.nodeAssignment(dead), NodeJson.write(new Assignment(
            4, POOL, 7, dead, 1L, Map.of(ACME, Assignment.TenantAssignment.coordinator())
        )));

        FakeNode surviving = new FakeNode(store, new Registration(
            live, "placeholder", live.value(), POOL, EnumSet.of(NodeRole.DATA), "127.0.0.1", 9300, "3.7.0",
            Map.of("zone", "z1",
                org.opensearch.supervisor.model.allocate.RemoteStoreDecider.SEGMENT_REPOSITORY_ATTRIBUTE,
                "segments"),
            5000, 0L
        ), clock, "eph-live");
        surviving.start();
        createIndex("my-index", UUID, 1, 0);

        run(supervisor(), List.of(surviving), 8);

        // The dead one registered and never heartbeated, so it is UNKNOWN rather than LOST and its registration is
        // still observed -- which is exactly the window in which both used to be published.
        assertTrue(store.get(Keys.nodeStatus(ACME, live)).isPresent(), "the node answering the port is projected");
        assertTrue(store.get(Keys.nodeStatus(ACME, dead)).isEmpty(),
            "and its predecessor on that port is not, or a reader gets two nodes claiming one socket");
    }

    @Test
    void nodesOnDistinctPortsOfOneHostAreBothProjected() {
        // The case the endpoint rule must not break, and the ordinary shape of a laptop deployment: several nodes
        // on one machine, each with its own port. Keying on address alone would have published one of them.
        FakeNode first = node(NODE, "z1");
        FakeNode second = node(NODE_B, "z1");
        first.start();
        second.start();
        createIndex("my-index", UUID, 2, 0);

        run(supervisor(), List.of(first, second), 10);

        assertTrue(store.get(Keys.nodeStatus(ACME, NODE)).isPresent());
        assertTrue(store.get(Keys.nodeStatus(ACME, NODE_B)).isPresent(),
            "distinct ports on one address are distinct endpoints");
    }
}
