package org.opensearch.supervisor.model.balance;

import org.opensearch.supervisor.model.NodeId;
import org.opensearch.supervisor.model.PoolId;
import org.opensearch.supervisor.model.TenantId;
import org.opensearch.supervisor.model.index.IndexConfig;
import org.opensearch.supervisor.model.node.Assignment;
import org.opensearch.supervisor.model.node.Heartbeat;
import org.opensearch.supervisor.model.node.NodeRole;
import org.opensearch.supervisor.model.node.Registration;
import org.opensearch.supervisor.model.node.ShardInventory;
import org.opensearch.supervisor.model.node.ShardRole;
import org.opensearch.supervisor.model.node.ShardState;
import org.opensearch.supervisor.model.observe.Observation;
import org.opensearch.supervisor.model.settings.AllocationEnable;
import org.opensearch.supervisor.model.settings.ClusterSettings;
import org.opensearch.supervisor.model.status.Liveness;
import org.opensearch.supervisor.model.status.Replication;

import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/** Pools of nodes holding sized shard copies, which is the only thing these tests need to talk about. */
final class BalanceFixture {

    static final PoolId POOL = PoolId.of("shared-a");
    static final TenantId ACME = TenantId.of("acme");
    static final String UUID = "uzKxLWZu4nPSrTknEcDpoA";
    static final NodeId A = NodeId.of("nodeAAAAAAAAAAAAAAAAAA");
    static final NodeId B = NodeId.of("nodeBBBBBBBBBBBBBBBBBB");
    static final NodeId C = NodeId.of("nodeCCCCCCCCCCCCCCCCCC");

    static final long MIB = 1024 * 1024;

    private BalanceFixture() {}

    /** One node, with whatever it holds and whatever it has been told to hold. */
    static final class Node {

        private final NodeId id;
        private String address;
        // Remote store by default: a pool serving segment-replication indices has one, and these fixtures place
        // search replicas throughout.
        private final Map<String, String> attributes = new LinkedHashMap<>(Map.of(
            org.opensearch.supervisor.model.allocate.RemoteStoreDecider.SEGMENT_REPOSITORY_ATTRIBUTE, "segments"));
        private long total = 1000;
        private long free = 900;
        private Liveness liveness = Liveness.LIVE;
        private final Map<Integer, ShardInventory.ShardCopy> held = new LinkedHashMap<>();
        private final Map<Integer, ShardRole> assigned = new LinkedHashMap<>();

        private Node(NodeId id, String address) {
            this.id = id;
            this.address = address;
        }

        static Node of(NodeId id) {
            // Distinct addresses: nodes sharing one make SameShardDecider refuse every second copy, which
            // silently turns a balance test into a same-host test.
            return new Node(id, "10.0.0." + (id.value().charAt(4) - 'A' + 1));
        }

        NodeId id() {
            return id;
        }

        Node holding(int shard, ShardRole role, long sizeBytes) {
            held.put(shard, new ShardInventory.ShardCopy(role, ShardState.STARTED, sizeBytes, null));
            return this;
        }

        Node initializing(int shard, ShardRole role) {
            held.put(shard, new ShardInventory.ShardCopy(role, ShardState.INITIALIZING, null, null));
            return this;
        }

        Node failed(int shard, ShardRole role, long sizeBytes) {
            held.put(shard, new ShardInventory.ShardCopy(role, ShardState.FAILED, sizeBytes, "disk error"));
            return this;
        }

        Node told(int shard, ShardRole role) {
            assigned.put(shard, role);
            return this;
        }

        Node disk(long total, long free) {
            this.total = total;
            this.free = free;
            return this;
        }

        Node attribute(String key, String value) {
            attributes.put(key, value);
            return this;
        }

        Node liveness(Liveness value) {
            this.liveness = value;
            return this;
        }

        Observation.NodeObservation build(String indexName) {
            ShardInventory inventory = held.isEmpty() ? null : new ShardInventory(id, "eph", 1L, Map.of(
                ACME, new ShardInventory.TenantShards(Map.of(
                    UUID, new ShardInventory.IndexShards(indexName, 3041, held)
                ))
            ));
            Assignment assignment = assigned.isEmpty() ? null : new Assignment(
                1L, POOL, 1L, id, 1L, Map.of(
                    ACME, new Assignment.TenantAssignment(Set.of(NodeRole.DATA), Map.of(
                        UUID, new Assignment.IndexAssignment(indexName, 3041, assigned)
                    ))
                )
            );
            return new Observation.NodeObservation(
                id,
                new Registration(id, "eph", id.value(), POOL, Set.of(NodeRole.DATA), address, 9300, "3.7.0",
                    attributes, 5000, 1L),
                new Heartbeat(id, "eph", 1L, Heartbeat.Applied.NONE, null, held.size(),
                    new Heartbeat.Capacity(total, free, 10, 10, 0.1)),
                inventory, liveness, POOL, assignment, assignment == null ? 0 : 7
            );
        }
    }

    /** An observation of one index across some nodes. */
    static Observation observe(int shards, int replicas, Replication replication, Node... nodes) {
        return observe(shards, replicas, replication, true, ClusterSettings.EMPTY, nodes);
    }

    static Observation observe(
        int shards,
        int replicas,
        Replication replication,
        boolean open,
        ClusterSettings settings,
        Node... nodes
    ) {
        IndexConfig index = new IndexConfig("my-index", UUID, open, shards, replicas, replication, false,
            AllocationEnable.UNKNOWN, 3041);
        Map<NodeId, Observation.NodeObservation> observed = new LinkedHashMap<>();
        for (Node node : nodes) {
            observed.put(node.id(), node.build(index.name()));
        }
        return new Observation(1, Instant.EPOCH, POOL,
            Map.of(ACME, new Observation.DesiredState(Map.of(UUID, index), List.of(), settings)),
            observed, List.of());
    }

    /** An observation with no desired state at all, for the orphaned-copy paths. */
    static Observation observeWithoutIndex(Node... nodes) {
        Map<NodeId, Observation.NodeObservation> observed = new LinkedHashMap<>();
        for (Node node : nodes) {
            observed.put(node.id(), node.build("my-index"));
        }
        return new Observation(1, Instant.EPOCH, POOL,
            Map.of(ACME, Observation.DesiredState.empty()), observed, List.of());
    }

    static IndexConfig indexOf(Observation observation) {
        return observation.tenants().get(ACME).indicesByUuid().get(UUID);
    }

    static RebalanceContext move(Observation observation, NodeId source, int shard, NodeId target) {
        return move(observation, source, shard, target, MoveThresholdDecider.DEFAULT_THRESHOLD);
    }

    static RebalanceContext move(
        Observation observation,
        NodeId source,
        int shard,
        NodeId target,
        double threshold
    ) {
        LoadModel loads = LoadModel.of(observation);
        LoadModel.Held copy = loads.heldBy(source).stream()
            .filter(held -> held.shard().shard() == shard)
            .findFirst()
            .orElseThrow(() -> new AssertionError("node " + source + " holds no shard " + shard));
        return new RebalanceContext(observation, loads, copy, target, indexOf(observation), threshold);
    }

    static List<Integer> shardsOf(List<LoadModel.Held> held) {
        List<Integer> shards = new ArrayList<>();
        held.forEach(copy -> shards.add(copy.shard().shard()));
        return shards;
    }
}
