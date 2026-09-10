package org.opensearch.supervisor.model.status;

import org.junit.jupiter.api.Test;
import org.opensearch.supervisor.model.NodeId;
import org.opensearch.supervisor.model.PoolId;
import org.opensearch.supervisor.model.TenantId;
import org.opensearch.supervisor.model.index.IndexConfig;
import org.opensearch.supervisor.model.node.Assignment;
import org.opensearch.supervisor.model.node.NodeRole;
import org.opensearch.supervisor.model.node.Registration;
import org.opensearch.supervisor.model.node.ShardInventory;
import org.opensearch.supervisor.model.node.ShardRole;
import org.opensearch.supervisor.model.node.ShardState;
import org.opensearch.supervisor.model.observe.Observation;

import java.time.Instant;
import java.util.EnumSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * OpenSearch's health definition, computed from an observation and nothing else.
 *
 * <p>These rules are a contract with the control plane, which will render them into an API response verbatim,
 * so getting them subtly wrong would be invisible here and wrong in a customer's monitoring.
 */
class StatusComputationTests {

    private static final PoolId POOL = PoolId.of("shared-a");
    private static final TenantId ACME = TenantId.of("acme");
    private static final TenantId GLOBEX = TenantId.of("globex");
    private static final String UUID = "uzKxLWZu4nPSrTknEcDpoA";
    private static final NodeId NODE_A = NodeId.of("nodeAAAAAAAAAAAAAAAAAA");
    private static final NodeId NODE_B = NodeId.of("nodeBBBBBBBBBBBBBBBBBB");

    private static IndexConfig index(boolean open, int shards, int replicas) {
        return new IndexConfig("my-index", UUID, open, shards, replicas, Replication.SEGMENT, false, 3041);
    }

    private static Registration registration(NodeId node, Map<String, String> attributes) {
        return new Registration(node, "eph", node.value(), POOL, EnumSet.of(NodeRole.DATA),
            "10.0.0." + node.value().charAt(4), 9300, "3.7.0", attributes, 5000, 1L);
    }

    private static Observation.NodeObservation node(
        NodeId id,
        Liveness liveness,
        Map<String, String> attributes,
        Map<Integer, ShardInventory.ShardCopy> shards
    ) {
        ShardInventory inventory = shards == null ? null : new ShardInventory(id, "eph", 1L, Map.of(
            ACME, new ShardInventory.TenantShards(Map.of(
                UUID, new ShardInventory.IndexShards("my-index", 3041, shards)
            ))
        ));
        return new Observation.NodeObservation(
            id, registration(id, attributes), null, inventory, liveness, POOL, null, 0
        );
    }

    private static Observation observe(IndexConfig index, Map<NodeId, Observation.NodeObservation> nodes) {
        Map<TenantId, Observation.DesiredState> tenants = new LinkedHashMap<>();
        tenants.put(ACME, new Observation.DesiredState(
            index == null ? Map.of() : Map.of(UUID, index), List.of()
        ));
        return new Observation(7, Instant.ofEpochMilli(1_000_000), POOL, tenants, nodes, List.of());
    }

    private static ShardInventory.ShardCopy copy(ShardRole role, ShardState state) {
        return ShardInventory.ShardCopy.of(role, state);
    }

    private static IndexStatus statusOf(IndexConfig index, Map<NodeId, Observation.NodeObservation> nodes) {
        return StatusComputation.compute(ACME, observe(index, nodes)).indices().get(UUID);
    }

    // ---------------------------------------------------------------- the health colours

    @Test
    void greenWhenEveryDesiredCopyIsServing() {
        IndexStatus status = statusOf(index(true, 1, 1), Map.of(
            NODE_A, node(NODE_A, Liveness.LIVE, Map.of(), Map.of(0, copy(ShardRole.PRIMARY, ShardState.STARTED))),
            NODE_B, node(NODE_B, Liveness.LIVE, Map.of(), Map.of(0, copy(ShardRole.REPLICA, ShardState.STARTED)))
        ));

        assertEquals(HealthStatus.GREEN, status.status());
        assertEquals(2, status.activeShards());
        assertEquals(1, status.activePrimaryShards());
        assertEquals(0, status.unassignedShards());
    }

    @Test
    void yellowWhenAPrimaryIsServingButAReplicaIsMissing() {
        IndexStatus status = statusOf(index(true, 1, 1), Map.of(
            NODE_A, node(NODE_A, Liveness.LIVE, Map.of(), Map.of(0, copy(ShardRole.PRIMARY, ShardState.STARTED)))
        ));

        assertEquals(HealthStatus.YELLOW, status.status());
        assertEquals(1, status.unassignedShards());
    }

    @Test
    void redWhenAPrimaryIsMissingEntirely() {
        assertEquals(HealthStatus.RED, statusOf(index(true, 1, 0), Map.of()).status());
    }

    @Test
    void redWhenAPrimaryExistsButIsStillInitializing() {
        // The case that could not be expressed before role and state were split, and the one most likely to be
        // got wrong: red means "some shard cannot serve", and a recovering primary cannot. Reporting yellow
        // here would tell a customer's monitoring that writes are being accepted when they are not.
        IndexStatus status = statusOf(index(true, 1, 0), Map.of(
            NODE_A, node(NODE_A, Liveness.LIVE, Map.of(), Map.of(0, copy(ShardRole.PRIMARY, ShardState.INITIALIZING)))
        ));

        assertEquals(HealthStatus.RED, status.status());
        assertEquals(0, status.activePrimaryShards());
        assertEquals(1, status.initializingShards());
        assertEquals(0, status.unassignedShards(), "it exists, so it is not unassigned");
    }

    @Test
    void redWhenOnlyOneShardOfManyHasNoPrimary() {
        IndexStatus status = statusOf(index(true, 2, 0), Map.of(
            NODE_A, node(NODE_A, Liveness.LIVE, Map.of(), Map.of(0, copy(ShardRole.PRIMARY, ShardState.STARTED)))
        ));

        assertEquals(HealthStatus.RED, status.status());
        assertEquals(1, status.activePrimaryShards());
    }

    @Test
    void aClosedIndexIsGreenWithNoShards() {
        // Matching OpenSearch. A closed index is not unhealthy, it is simply not serving -- and reporting it
        // red would make every deliberate close look like an incident.
        IndexStatus status = statusOf(index(false, 3, 1), Map.of());

        assertEquals(HealthStatus.GREEN, status.status());
        assertTrue(status.shards().isEmpty());
        assertEquals(0, status.unassignedShards());
        assertEquals(3, status.numberOfShards(), "it still has a shard count, it just wants no copies");
    }

    @Test
    void aFailedCopyIsNeitherActiveNorInitializing() {
        IndexStatus status = statusOf(index(true, 1, 0), Map.of(
            NODE_A, node(NODE_A, Liveness.LIVE, Map.of(), Map.of(0, copy(ShardRole.PRIMARY, ShardState.FAILED)))
        ));

        assertEquals(HealthStatus.RED, status.status());
        assertEquals(0, status.activeShards());
        assertEquals(0, status.initializingShards());
        assertEquals(0, status.unassignedShards(), "it occupies disk, so it is present");
    }

    @Test
    void aLostNodesCopiesAreNotCountedAsPresent() {
        IndexStatus status = statusOf(index(true, 1, 0), Map.of(
            NODE_A, node(NODE_A, Liveness.LOST, Map.of(), Map.of(0, copy(ShardRole.PRIMARY, ShardState.STARTED)))
        ));

        assertEquals(HealthStatus.RED, status.status());
        assertEquals(1, status.unassignedShards());
    }

    @Test
    void aStaleNodesCopiesStillCount() {
        IndexStatus status = statusOf(index(true, 1, 0), Map.of(
            NODE_A, node(NODE_A, Liveness.STALE, Map.of(), Map.of(0, copy(ShardRole.PRIMARY, ShardState.STARTED)))
        ));

        assertEquals(HealthStatus.GREEN, status.status(), "stale is a hiccup, not a loss");
    }

    // ---------------------------------------------------------------- placement, and who reads it

    @Test
    void everyObservedCopyIsListedIncludingOnesNotYetServing() {
        // Required rather than incidental: a docrep replica reads this to find the primary it must replicate
        // from, and a projection filtered to serving copies would deadlock.
        IndexStatus status = statusOf(index(true, 1, 1), Map.of(
            NODE_A, node(NODE_A, Liveness.LIVE, Map.of(), Map.of(0, copy(ShardRole.PRIMARY, ShardState.STARTED))),
            NODE_B, node(NODE_B, Liveness.LIVE, Map.of(), Map.of(0, copy(ShardRole.REPLICA, ShardState.INITIALIZING)))
        ));

        assertEquals(2, status.shards().get(0).size());
        assertEquals(NODE_A, status.servingPrimary(0).orElseThrow().nodeId());
        assertEquals(1, status.replicasOf(0).size());
    }

    @Test
    void placementOrderIsStableSoAnUnchangedProjectionIsNotRewritten() {
        var nodes = Map.of(
            NODE_B, node(NODE_B, Liveness.LIVE, Map.of(), Map.of(0, copy(ShardRole.REPLICA, ShardState.STARTED))),
            NODE_A, node(NODE_A, Liveness.LIVE, Map.of(), Map.of(0, copy(ShardRole.PRIMARY, ShardState.STARTED)))
        );

        assertEquals(
            List.of(NODE_A, NODE_B),
            statusOf(index(true, 1, 1), nodes).shards().get(0).stream()
                .map(IndexStatus.ShardPlacement::nodeId).toList()
        );
    }

    // ---------------------------------------------------------------- the cluster summary

    @Test
    void clusterStatusIsTheWorstIndexStatus() {
        String other = "otherIndexUuidAAAAAAA";
        Map<TenantId, Observation.DesiredState> tenants = Map.of(ACME, new Observation.DesiredState(Map.of(
            UUID, index(true, 1, 0),
            other, new IndexConfig("other", other, true, 1, 0, Replication.SEGMENT, false, 1)
        ), List.of()));
        Observation observation = new Observation(7, Instant.ofEpochMilli(1_000_000), POOL, tenants, Map.of(
            NODE_A, node(NODE_A, Liveness.LIVE, Map.of(), Map.of(0, copy(ShardRole.PRIMARY, ShardState.STARTED)))
        ), List.of());

        ClusterStatus cluster = StatusComputation.compute(ACME, observation).cluster();

        // my-index is green; other has no primary anywhere, so it is red -- and so is the cluster.
        assertEquals(HealthStatus.RED, cluster.status());
        assertEquals(1, cluster.indicesByStatus().get("green"));
        assertEquals(1, cluster.indicesByStatus().get("red"));
    }

    @Test
    void clusterCountsAreTheSumOfTheIndices() {
        ClusterStatus cluster = StatusComputation.compute(ACME, observe(index(true, 2, 1), Map.of(
            NODE_A, node(NODE_A, Liveness.LIVE, Map.of(), Map.of(
                0, copy(ShardRole.PRIMARY, ShardState.STARTED),
                1, copy(ShardRole.PRIMARY, ShardState.STARTED)
            ))
        ))).cluster();

        assertEquals(2, cluster.activeShards());
        assertEquals(2, cluster.activePrimaryShards());
        assertEquals(2, cluster.unassignedShards(), "two replicas wanted, none present");
        assertEquals(HealthStatus.YELLOW, cluster.status());
    }

    // ---------------------------------------------------------------- the node list

    @Test
    void onlyNodesHoldingSomethingOfThisTenantsAreListed() {
        // A tenant in a shared pool must not learn how many machines its neighbours are using, and its
        // number_of_nodes must not move when somebody else's data does.
        Observation.NodeObservation ours = node(NODE_A, Liveness.LIVE, Map.of("zone", "z1"),
            Map.of(0, copy(ShardRole.PRIMARY, ShardState.STARTED)));
        Observation.NodeObservation theirs = new Observation.NodeObservation(
            NODE_B, registration(NODE_B, Map.of("zone", "z2")), null,
            new ShardInventory(NODE_B, "eph", 1L, Map.of(
                GLOBEX, new ShardInventory.TenantShards(Map.of())
            )),
            Liveness.LIVE, POOL, null, 0
        );

        var status = StatusComputation.compute(ACME, observe(index(true, 1, 0), Map.of(
            NODE_A, ours, NODE_B, theirs
        )));

        assertEquals(java.util.Set.of(NODE_A), status.nodes().keySet());
        assertEquals(1, status.cluster().numberOfNodes());
    }

    @Test
    void aNodeAssignedSomethingCountsAsServingTheTenantBeforeItReportsAnything() {
        Assignment assignment = new Assignment(1, POOL, 1, NODE_B, 1L, Map.of(
            ACME, new Assignment.TenantAssignment(NodeRole.DATA, Map.of())
        ));
        Observation.NodeObservation assigned = new Observation.NodeObservation(
            NODE_B, registration(NODE_B, Map.of("zone", "z2")), null, null, Liveness.LIVE, POOL, assignment, 4
        );

        var status = StatusComputation.compute(ACME, observe(index(true, 1, 0), Map.of(NODE_B, assigned)));

        assertTrue(status.nodes().containsKey(NODE_B));
    }

    // ---------------------------------------------------------------- awareness

    @Test
    void awarenessCollectsTheDistinctValuesActuallyPresent() {
        var status = StatusComputation.compute(ACME, observe(index(true, 1, 1), Map.of(
            NODE_A, node(NODE_A, Liveness.LIVE, Map.of("zone", "z1", "rack", "r7"),
                Map.of(0, copy(ShardRole.PRIMARY, ShardState.STARTED))),
            NODE_B, node(NODE_B, Liveness.LIVE, Map.of("zone", "z2"),
                Map.of(0, copy(ShardRole.REPLICA, ShardState.STARTED)))
        )));

        assertEquals(List.of("z1", "z2"), status.awareness().attributes().get("zone"));
        assertEquals(List.of("r7"), status.awareness().attributes().get("rack"));
    }

    @Test
    void aStaleNodesAwarenessValuesStillCountAndALostNodesDoNot() {
        // The hazard worth guarding: a value disappearing here makes the control plane reject a
        // weighted-routing request naming it, for a reason that reads like a typo. A brief network problem
        // must not do that.
        var stale = StatusComputation.compute(ACME, observe(index(true, 1, 0), Map.of(
            NODE_A, node(NODE_A, Liveness.STALE, Map.of("zone", "z1"),
                Map.of(0, copy(ShardRole.PRIMARY, ShardState.STARTED)))
        )));
        assertEquals(List.of("z1"), stale.awareness().attributes().get("zone"));

        var lost = StatusComputation.compute(ACME, observe(index(true, 1, 0), Map.of(
            NODE_A, node(NODE_A, Liveness.LOST, Map.of("zone", "z1"),
                Map.of(0, copy(ShardRole.PRIMARY, ShardState.STARTED)))
        )));
        assertTrue(lost.awareness().attributes().isEmpty());
    }

    @Test
    void aTenantWithNoDesiredStateProducesNothingRatherThanEmptyDocuments() {
        var status = StatusComputation.compute(GLOBEX, observe(index(true, 1, 0), Map.of()));

        assertTrue(status.indices().isEmpty());
        assertTrue(status.nodes().isEmpty());
        assertFalse(status.cluster() != null && status.awareness() != null,
            "a tenant this pool does not serve should yield no documents at all");
    }
}
