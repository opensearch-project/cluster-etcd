package org.opensearch.supervisor.model.allocate;

import org.junit.jupiter.api.Test;
import org.opensearch.supervisor.model.NodeId;
import org.opensearch.supervisor.model.PoolId;
import org.opensearch.supervisor.model.TenantId;
import org.opensearch.supervisor.model.balance.DeploymentRebalanceDecider;
import org.opensearch.supervisor.model.balance.LoadModel;
import org.opensearch.supervisor.model.balance.RebalanceContext;
import org.opensearch.supervisor.model.deploy.Deployment;
import org.opensearch.supervisor.model.deploy.DeploymentState;
import org.opensearch.supervisor.model.deploy.DrainIntent;
import org.opensearch.supervisor.model.index.IndexConfig;
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

import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Keeping new copies off nodes a deployment is taking away, and balance out of a pool mid-deployment (§29.3). */
class DeploymentDeciderTests {

    private static final PoolId POOL = PoolId.of("shared-a");
    private static final TenantId ACME = TenantId.of("acme");
    private static final String UUID = "uzKxLWZu4nPSrTknEcDpoA";
    private static final NodeId Z1 = NodeId.of("nodeAAAAAAAAAAAAAAAAAA");
    private static final NodeId Z2 = NodeId.of("nodeBBBBBBBBBBBBBBBBBB");
    private static final long NOW = 1_790_000_000_000L;

    private static final IndexConfig INDEX = new IndexConfig("my-index", UUID, true, 1, 1, Replication.DOCUMENT, false,
        AllocationEnable.UNKNOWN, 3041);

    private static Observation.NodeObservation node(NodeId id, String zone, boolean holdsReplica) {
        Registration registration = new Registration(id, "eph", id.value(), POOL, Set.of(NodeRole.DATA),
            "10.0.0." + id.value().charAt(4), 9300, "3.7.0", Map.of("zone", zone), 5000, 1L);
        Heartbeat heartbeat = new Heartbeat(id, "eph", 1L, Heartbeat.Applied.NONE, null, 0,
            new Heartbeat.Capacity(1_000_000L, 900_000L, 10, 10, 0.1));
        ShardInventory inventory = holdsReplica ? new ShardInventory(id, "eph", 1L, Map.of(ACME,
            new ShardInventory.TenantShards(Map.of(UUID, new ShardInventory.IndexShards("my-index", 3041,
                Map.of(0, ShardInventory.ShardCopy.of(ShardRole.REPLICA, ShardState.STARTED, 1000))))))) : null;
        return new Observation.NodeObservation(id, registration, heartbeat, inventory, Liveness.LIVE, POOL, null, 0);
    }

    private static Observation observe(Map<String, Deployment> deployments, List<String> unreadable) {
        Map<NodeId, Observation.NodeObservation> nodes = new LinkedHashMap<>();
        nodes.put(Z1, node(Z1, "z1", true));
        nodes.put(Z2, node(Z2, "z2", false));
        return new Observation(1, Instant.ofEpochMilli(NOW), POOL,
            Map.of(ACME, new Observation.DesiredState(Map.of(UUID, INDEX))), nodes, unreadable, Map.of(), deployments);
    }

    private static Deployment drain(DrainIntent intent, Long until) {
        return new Deployment(DeploymentState.DRAIN, Map.of("zone", "z1"), intent, until, null, null);
    }

    private static Decision decide(Observation observation, ShardRole role, NodeId candidate) {
        return new DeploymentDecider().canAllocate(
            new AllocationContext(ACME, INDEX, 0, role, observation, List.of(), null, List.of()), candidate);
    }

    @Test
    void aReturningDrainTakesNoNewPrimaryAndMayStillTakeAReplica() {
        Observation observation = observe(Map.of("zone-1", drain(DrainIntent.RETURN, NOW + 60_000)), List.of());

        Decision primary = decide(observation, ShardRole.PRIMARY, Z1);
        assertEquals(Decision.Type.NO, primary.type(), primary.because());
        assertTrue(primary.because().contains("drained by deployment [zone-1]"), primary.because());
        assertEquals(Decision.Type.YES, decide(observation, ShardRole.REPLICA, Z1).type(),
            "the node is coming back, and its copies keep counting while it is away");
        assertEquals(Decision.Type.YES, decide(observation, ShardRole.PRIMARY, Z2).type(), "not selected");
    }

    @Test
    void aRemovalTakesNoNewCopyOfAnyRole() {
        Observation observation = observe(Map.of("decommission", drain(DrainIntent.REMOVE, null)), List.of());

        for (ShardRole role : List.of(ShardRole.PRIMARY, ShardRole.REPLICA, ShardRole.SEARCH_REPLICA)) {
            Decision decision = decide(observation, role, Z1);
            assertEquals(Decision.Type.NO, decision.type(), role + ": " + decision.because());
            assertTrue(decision.because().contains("being removed"), decision.because());
        }
    }

    @Test
    void anExpiredDrainRefusesNothing() {
        Observation observation = observe(Map.of("zone-1", drain(DrainIntent.RETURN, NOW - 1)), List.of());

        assertEquals(Decision.Type.YES, decide(observation, ShardRole.PRIMARY, Z1).type());
    }

    @Test
    void aFrozenNodeTakesNoNewPrimaryWhateverFrozeIt() {
        // An invalid deployment freezes the nodes it selects; one that does not parse freezes every node.
        Decision invalid = decide(observe(Map.of("zone-1", drain(DrainIntent.RETURN, null)), List.of()),
            ShardRole.PRIMARY, Z1);
        assertEquals(Decision.Type.NO, invalid.type(), invalid.because());
        assertTrue(invalid.because().contains("frozen"), invalid.because());

        Decision unreadable = decide(observe(Map.of(), List.of("deployments/zone-1")), ShardRole.PRIMARY, Z2);
        assertEquals(Decision.Type.NO, unreadable.type(), unreadable.because());
        assertTrue(unreadable.because().contains("cannot be parsed"), unreadable.because());

        assertEquals(Decision.Type.YES, decide(observe(Map.of(), List.of("deployments/zone-1")), ShardRole.REPLICA, Z2)
            .type(), "freezing holds primaries, not every placement");
    }

    @Test
    void theAllocatorPlacesAPrimaryOffTheDrainAndSaysWhyWhenItCannot() {
        Observation observation = observe(Map.of("zone-1", drain(DrainIntent.RETURN, NOW + 60_000)), List.of());
        AllocationContext context = new AllocationContext(ACME, INDEX, 0, ShardRole.PRIMARY, observation, List.of(), null,
            List.of());

        assertEquals(Z2, new Allocator().choose(context, List.of(Z1, Z2)).node().orElseThrow());

        Allocator.Outcome refused = new Allocator().choose(context, List.of(Z1));
        assertTrue(refused.placed() == false, refused.toString());
        assertTrue(refused.refusal().because().contains("[deployment] node"), refused.refusal().because());
    }

    @Test
    void aPoolMidDeploymentIsNotRebalanced() {
        Observation drained = observe(Map.of("zone-1", drain(DrainIntent.RETURN, NOW + 60_000)), List.of());
        LoadModel loads = LoadModel.of(drained);
        Decision decision = new DeploymentRebalanceDecider().canRebalance(
            new RebalanceContext(drained, loads, loads.heldBy(Z1).getFirst(), Z2, INDEX, 0.1));

        assertEquals(Decision.Type.THROTTLE, decision.type(), decision.because());
        assertTrue(decision.because().contains("[zone-1]"), decision.because());

        Observation quiet = observe(Map.of(), List.of());
        LoadModel quietLoads = LoadModel.of(quiet);
        assertEquals(Decision.Type.YES, new DeploymentRebalanceDecider().canRebalance(
            new RebalanceContext(quiet, quietLoads, quietLoads.heldBy(Z1).getFirst(), Z2, INDEX, 0.1)).type());
    }
}
