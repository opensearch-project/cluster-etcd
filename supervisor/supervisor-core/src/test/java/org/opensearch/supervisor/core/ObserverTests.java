package org.opensearch.supervisor.core;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.opensearch.controlplane.store.InMemoryKeyValueStore;
import org.opensearch.controlplane.store.KeyValueStore;
import org.opensearch.supervisor.model.Keys;
import org.opensearch.supervisor.model.NodeId;
import org.opensearch.supervisor.model.PoolId;
import org.opensearch.supervisor.model.TenantId;
import org.opensearch.supervisor.model.index.IndexConfig;
import org.opensearch.supervisor.model.node.Heartbeat;
import org.opensearch.supervisor.model.node.NodeJson;
import org.opensearch.supervisor.model.node.NodeRole;
import org.opensearch.supervisor.model.node.Registration;
import org.opensearch.supervisor.model.node.ShardInventory;
import org.opensearch.supervisor.model.node.ShardRole;
import org.opensearch.supervisor.model.node.ShardState;
import org.opensearch.supervisor.model.observe.Observation;
import org.opensearch.supervisor.model.pool.PoolDefinition;
import org.opensearch.supervisor.model.pool.PoolBinding;
import org.opensearch.supervisor.model.pool.PoolKind;
import org.opensearch.supervisor.model.pool.PoolNode;
import org.opensearch.supervisor.model.status.Liveness;
import org.opensearch.supervisor.model.status.Replication;

import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.EnumSet;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Reading the store into an observation, against the same in-memory store the control plane's tests use. */
class ObserverTests {

    private static final PoolId POOL = PoolId.of("shared-a");
    private static final PoolId OTHER = PoolId.of("dedicated-b");
    private static final TenantId ACME = TenantId.of("acme");
    private static final NodeId NODE_A = NodeId.of("nodeAAAAAAAAAAAAAAAAAA");
    private static final NodeId NODE_B = NodeId.of("nodeBBBBBBBBBBBBBBBBBB");
    private static final String UUID = "uzKxLWZu4nPSrTknEcDpoA";
    private static final Instant NOW = Instant.ofEpochMilli(1_000_000);

    private CountingStore store;
    private Observer observer;

    /** Counts reads, because "does not re-read what has not changed" is the property under test. */
    private static final class CountingStore implements KeyValueStore {
        private final KeyValueStore delegate = new InMemoryKeyValueStore();
        final AtomicInteger gets = new AtomicInteger();
        final AtomicInteger scans = new AtomicInteger();

        @Override
        public Optional<Versioned> get(String key) {
            gets.incrementAndGet();
            return delegate.get(key);
        }

        @Override
        public NavigableMap<String, Versioned> scanPrefix(String keyPrefix) {
            scans.incrementAndGet();
            return delegate.scanPrefix(keyPrefix);
        }

        @Override
        public long put(String key, byte[] value) {
            return delegate.put(key, value);
        }

        @Override
        public long put(String key, byte[] value, long expectedRevision) {
            return delegate.put(key, value, expectedRevision);
        }

        @Override
        public boolean delete(String key) {
            return delegate.delete(key);
        }

        @Override
        public boolean delete(String key, long expectedRevision) {
            return delegate.delete(key, expectedRevision);
        }
    }

    @BeforeEach
    void setUp() {
        store = new CountingStore();
        observer = new Observer(store, Clock.fixed(NOW, ZoneOffset.UTC), new LivenessTracker(NOW));

        store.put(Keys.poolDefinition(POOL),
            new PoolDefinition(POOL, PoolKind.SHARED, false, null, null).toBytes());
        store.put(Keys.poolDefinition(OTHER),
            new PoolDefinition(OTHER, PoolKind.DEDICATED, false, null, null).toBytes());
        store.put(Keys.poolBinding(ACME), PoolBinding.to(POOL).toBytes());
    }

    private void register(NodeId node, PoolId pool) {
        store.put(Keys.nodeRegistration(node), NodeJson.write(new Registration(
            node, "eph-" + node.value(), node.value(), pool, EnumSet.of(NodeRole.DATA),
            "10.0.0.1", 9300, "3.7.0", Map.of("zone", "z1"), 5000, 1L
        )));
    }

    private void heartbeat(NodeId node, String digest, int copies) {
        store.put(Keys.nodeHeartbeat(node), NodeJson.write(new Heartbeat(
            node, "eph-" + node.value(), NOW.toEpochMilli(), Heartbeat.Applied.NONE,
            digest, copies, Heartbeat.Capacity.UNKNOWN
        )));
    }

    private String writeInventory(NodeId node, ShardState state) {
        ShardInventory inventory = new ShardInventory(node, "eph-" + node.value(), 1L, Map.of(
            ACME, new ShardInventory.TenantShards(Map.of(
                UUID, new ShardInventory.IndexShards("my-index", 3041, Map.of(
                    0, ShardInventory.ShardCopy.of(ShardRole.PRIMARY, state)
                ))
            ))
        ));
        byte[] bytes = NodeJson.write(inventory);
        store.put(Keys.nodeShards(node), bytes);
        return NodeJson.digest(bytes);
    }

    private void writeIndex(int shards, int replicas) {
        store.put(Keys.indicesPrefix(ACME) + "my-index", IndexConfig.toDocument(
            "my-index", UUID, true, IndexConfig.settingsOf(shards, replicas, Replication.SEGMENT)
        ));
    }

    private Observation observe() {
        return observer.observe(POOL, Pools.read(store), Set.of(ACME));
    }

    // ---------------------------------------------------------------- the basics

    @Test
    void readsNodesAndDesiredStateForOnePool() {
        register(NODE_A, POOL);
        String digest = writeInventory(NODE_A, ShardState.STARTED);
        heartbeat(NODE_A, digest, 1);
        writeIndex(1, 0);

        Observation observation = observe();

        assertEquals(POOL, observation.pool());
        assertEquals(1, observation.nodes().size());
        assertEquals(Liveness.LIVE, observation.nodes().get(NODE_A).liveness());
        assertEquals(1, observation.nodes().get(NODE_A).inventory().copyCount());
        assertEquals(1, observation.tenants().get(ACME).indicesByUuid().size());
        assertEquals(1, observation.tenants().get(ACME).indicesByUuid().get(UUID).numberOfShards());
        assertEquals("my-index", observation.tenants().get(ACME).indicesByUuid().get(UUID).name());
    }

    @Test
    void ignoresNodesBelongingToAnotherPool() {
        // Another pool's nodes are another supervisor's business, and reading their inventories would be
        // work done on someone else's behalf.
        register(NODE_A, POOL);
        heartbeat(NODE_A, null, 0);
        register(NODE_B, OTHER);
        heartbeat(NODE_B, null, 0);

        assertEquals(Set.of(NODE_A), observe().nodes().keySet());
    }

    @Test
    void anOperatorOverrideMovesANodeBetweenPools() {
        register(NODE_B, OTHER);
        heartbeat(NODE_B, null, 0);
        assertTrue(observe().nodes().isEmpty());

        store.put(Keys.poolNode(NODE_B), new PoolNode(POOL).toBytes());

        assertEquals(Set.of(NODE_B), observe().nodes().keySet());
    }

    @Test
    void ignoresTenantsBoundToAnotherPool() {
        writeIndex(1, 0);
        store.put(Keys.poolBinding(ACME), PoolBinding.to(OTHER).toBytes());

        assertTrue(observe().tenants().isEmpty());
    }

    @Test
    void aNodeThatHasRegisteredButNeverHeartbeatedIsStillVisible() {
        // Omitting it would make a node that dies before its first heartbeat completely invisible, which is
        // exactly the failure an operator most wants to see.
        register(NODE_A, POOL);

        Observation.NodeObservation node = observe().nodes().get(NODE_A);
        assertNotNull(node);
        assertNull(node.heartbeat());
        assertFalse(node.isPlaceable(), "nothing should be placed on a node that has never spoken");
    }

    // ---------------------------------------------------------------- the digest handshake

    @Test
    void anUnchangedInventoryIsNotReRead() {
        // The payoff for splitting the node's documents in three. In steady state a pass is one scan of the
        // heartbeats and nothing else, however many shards the cluster holds.
        register(NODE_A, POOL);
        String digest = writeInventory(NODE_A, ShardState.STARTED);
        heartbeat(NODE_A, digest, 1);

        observe();
        int getsAfterFirstPass = store.gets.get();

        observe();
        observe();

        assertEquals(getsAfterFirstPass, store.gets.get(), "inventory was re-read despite an unchanged digest");
    }

    @Test
    void aChangedDigestCausesExactlyOneReRead() {
        register(NODE_A, POOL);
        heartbeat(NODE_A, writeInventory(NODE_A, ShardState.INITIALIZING), 1);
        observe();
        int before = store.gets.get();

        heartbeat(NODE_A, writeInventory(NODE_A, ShardState.STARTED), 1);
        Observation observation = observe();

        assertEquals(before + 1, store.gets.get());
        assertEquals(
            ShardState.STARTED,
            observation.nodes().get(NODE_A).inventory()
                .tenants().get(ACME).indices().get(UUID).shards().get(0).state()
        );
    }

    @Test
    void aNodeWithNoInventoryYetIsObservedWithoutOne() {
        register(NODE_A, POOL);
        heartbeat(NODE_A, null, 0);

        assertNull(observe().nodes().get(NODE_A).inventory());
    }

    // ---------------------------------------------------------------- tolerating bad data

    @Test
    void oneUnreadableHeartbeatDoesNotStopThePass() {
        // R2.5. A single bad document must not prevent every other node and tenant from being observed.
        register(NODE_A, POOL);
        heartbeat(NODE_A, null, 0);
        register(NODE_B, POOL);
        store.put(Keys.nodeHeartbeat(NODE_B), "{ not json".getBytes(StandardCharsets.UTF_8));

        Observation observation = observe();

        assertTrue(observation.nodes().containsKey(NODE_A));
        assertTrue(observation.unreadable().contains(Keys.nodeHeartbeat(NODE_B)));
    }

    @Test
    void anUnreadableIndexDocumentCostsOnlyThatIndex() {
        writeIndex(1, 0);
        store.put(Keys.indicesPrefix(ACME) + "broken", "{ not json".getBytes(StandardCharsets.UTF_8));

        Observation observation = observe();

        assertEquals(1, observation.tenants().get(ACME).indicesByUuid().size());
        assertEquals(1, observation.unreadable().size());
    }

    @Test
    void anUnreadablePoolDocumentIsCarriedIntoTheObservation() {
        store.put(Keys.poolDefinition(PoolId.of("broken")), "{ not json".getBytes(StandardCharsets.UTF_8));

        assertTrue(observe().unreadable().stream().anyMatch(key -> key.contains("broken")));
    }

    // ---------------------------------------------------------------- the revision cache

    @Test
    void unchangedDocumentsAreNotReParsed() {
        register(NODE_A, POOL);
        heartbeat(NODE_A, null, 0);
        writeIndex(1, 0);

        observe();
        int freshAfterFirst = observer.freshParses();
        observe();

        assertEquals(freshAfterFirst, observer.freshParses(), "nothing changed, so nothing should re-parse");
        assertTrue(observer.cachedParses() > 0);
    }

    @Test
    void aChangedDocumentIsReParsed() {
        writeIndex(1, 0);
        observe();
        int before = observer.freshParses();

        writeIndex(3, 1);
        Observation observation = observe();

        assertTrue(observer.freshParses() > before);
        assertEquals(3, observation.tenants().get(ACME).indicesByUuid().get(UUID).numberOfShards());
    }

    @Test
    void thePassNumberAdvancesOncePerObservation() {
        assertEquals(1, observe().pass());
        assertEquals(2, observe().pass());
        assertEquals(2, observer.pass(), "pass() is the last pass observed, not the next one");
    }
}
