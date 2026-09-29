package org.opensearch.supervisor.model.plan;

import org.opensearch.supervisor.model.NodeId;
import org.opensearch.supervisor.model.PoolId;
import org.opensearch.supervisor.model.TenantId;
import org.opensearch.supervisor.model.diff.Diff;
import org.opensearch.supervisor.model.diff.Differ;
import org.opensearch.supervisor.model.diff.FindingAges;
import org.opensearch.supervisor.model.index.IndexConfig;
import org.opensearch.supervisor.model.node.Assignment;
import org.opensearch.supervisor.model.node.ContractFeatures;
import org.opensearch.supervisor.model.node.Heartbeat;
import org.opensearch.supervisor.model.node.NodeRole;
import org.opensearch.supervisor.model.node.Registration;
import org.opensearch.supervisor.model.node.ShardInventory;
import org.opensearch.supervisor.model.node.ShardRole;
import org.opensearch.supervisor.model.node.ShardState;
import org.opensearch.supervisor.model.observe.Observation;
import org.opensearch.supervisor.model.settings.AllocationEnable;
import org.opensearch.supervisor.model.status.Liveness;
import org.opensearch.supervisor.model.status.Replication;
import org.opensearch.supervisor.model.supervisor.PrimaryTerms;

import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/** A pool holding one index, for the term and promotion tests. */
final class PrimaryTermFixture {

    static final PoolId POOL = PoolId.of("shared-a");
    static final TenantId ACME = TenantId.of("acme");
    static final String UUID = "uzKxLWZu4nPSrTknEcDpoA";
    static final NodeId A = NodeId.of("nodeAAAAAAAAAAAAAAAAAA");
    static final NodeId B = NodeId.of("nodeBBBBBBBBBBBBBBBBBB");
    static final NodeId C = NodeId.of("nodeCCCCCCCCCCCCCCCCCC");

    private PrimaryTermFixture() {}

    static IndexConfig index(int shards, int replicas, Replication replication) {
        return new IndexConfig("my-index", UUID, true, shards, replicas, replication, false,
            AllocationEnable.UNKNOWN, 3041);
    }

    /** One node, what it holds, what it was told, and whether it can be told a term. */
    static final class Node {
        private final NodeId id;
        private final Map<Integer, ShardInventory.ShardCopy> held = new LinkedHashMap<>();
        private final Map<Integer, ShardRole> told = new LinkedHashMap<>();
        private final Map<Integer, Long> toldTerms = new LinkedHashMap<>();
        private boolean declaresTerms;
        private Liveness liveness = Liveness.LIVE;

        private Node(NodeId id) {
            this.id = id;
        }

        static Node of(NodeId id) {
            return new Node(id);
        }

        Node holding(int shard, ShardRole role) {
            held.put(shard, ShardInventory.ShardCopy.of(role, ShardState.STARTED, 1000));
            return this;
        }

        Node told(int shard, ShardRole role, long term) {
            told.put(shard, role);
            toldTerms.put(shard, term);
            return this;
        }

        Node declaringPrimaryTerms() {
            declaresTerms = true;
            return this;
        }

        Node lost() {
            liveness = Liveness.LOST;
            return this;
        }

        Observation.NodeObservation build() {
            Registration registration = new Registration(
                id, "eph", id.value(), POOL, Set.of(NodeRole.DATA),
                "10.0.0." + (id.value().charAt(4) - 'A' + 1), 9300, "3.7.0", Map.of(), 5000, 1L,
                declaresTerms ? Set.of(ContractFeatures.PRIMARY_TERMS) : Set.of()
            );
            ShardInventory inventory = held.isEmpty() ? null : new ShardInventory(id, "eph", 1L, Map.of(
                ACME, new ShardInventory.TenantShards(Map.of(
                    UUID, new ShardInventory.IndexShards("my-index", 3041, held)))));
            Assignment assignment = told.isEmpty() ? null : new Assignment(1, POOL, 1, id, 1L, Map.of(
                ACME, new Assignment.TenantAssignment(Set.of(NodeRole.DATA), Map.of(
                    UUID, new Assignment.IndexAssignment("my-index", 3041, told, null, toldTerms)))));
            Heartbeat heartbeat = new Heartbeat(id, "eph", 1L, Heartbeat.Applied.NONE, null, held.size(),
                new Heartbeat.Capacity(2_000_000L, 1_000_000L, 10, 10, 0.1));
            return new Observation.NodeObservation(id, registration, heartbeat, inventory, liveness, POOL,
                assignment, assignment == null ? 0 : 7);
        }
    }

    static Observation observe(IndexConfig index, Map<TenantId, Map<String, Observation.TermCounter>> terms,
                               Node... nodes) {
        Map<NodeId, Observation.NodeObservation> observed = new LinkedHashMap<>();
        for (Node node : nodes) {
            observed.put(node.id, node.build());
        }
        return new Observation(5, Instant.EPOCH, POOL,
            Map.of(ACME, new Observation.DesiredState(Map.of(UUID, index))), observed, List.of(), terms);
    }

    static Map<TenantId, Map<String, Observation.TermCounter>> counter(Map<Integer, Long> shards, long revision) {
        return Map.of(ACME, Map.of(UUID, new Observation.TermCounter(new PrimaryTerms(shards), revision)));
    }

    static Map<TenantId, Map<String, Observation.TermCounter>> unreadableCounter() {
        return Map.of(ACME, Map.of(UUID, new Observation.TermCounter(null, 9)));
    }

    /** The diff as it stands after findings have aged past the planner's patience. */
    static Diff diffAfter(int passes, Observation observation) {
        FindingAges ages = new FindingAges();
        Diff diff = null;
        for (int pass = 1; pass <= passes; pass++) {
            diff = Differ.diff(new Observation(pass, observation.observedAt(), observation.pool(),
                observation.tenants(), observation.nodes(), observation.unreadable(), observation.terms()), ages);
        }
        return diff;
    }
}
