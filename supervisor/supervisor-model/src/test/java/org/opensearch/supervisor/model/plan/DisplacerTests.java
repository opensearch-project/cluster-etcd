package org.opensearch.supervisor.model.plan;

import org.junit.jupiter.api.Test;
import org.opensearch.supervisor.model.NodeId;
import org.opensearch.supervisor.model.PoolId;
import org.opensearch.supervisor.model.TenantId;
import org.opensearch.supervisor.model.allocate.RemoteStoreDecider;
import org.opensearch.supervisor.model.diff.Diff;
import org.opensearch.supervisor.model.diff.Differ;
import org.opensearch.supervisor.model.diff.FindingAges;
import org.opensearch.supervisor.model.diff.Finding;
import org.opensearch.supervisor.model.diff.FindingKind;
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
import java.util.EnumSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Giving up a search-only copy so a shard can have a primary again — the only rule here that deletes a copy
 * that is currently serving.
 *
 * <p>So the tests split the same way the rule does: it has to fire on the deadlock, and it has to stay out of
 * the way everywhere else. The case it must never get wrong is deleting a copy and gaining nothing, which is
 * strictly worse than the deadlock, and that is what half of these are about.
 */
class DisplacerTests {

    private static final PoolId POOL = PoolId.of("shared-a");
    private static final TenantId ACME = TenantId.of("acme");
    private static final String UUID = "uzKxLWZu4nPSrTknEcDpoA";
    private static final NodeId ONLY = NodeId.of("nodeAAAAAAAAAAAAAAAAAA");
    private static final NodeId OTHER = NodeId.of("nodeBBBBBBBBBBBBBBBBBB");

    private static Registration registration(NodeId node) {
        return new Registration(
            node, "eph", node.value(), POOL, EnumSet.of(NodeRole.DATA),
            "10.0.0." + node.value().charAt(4), 9300, "3.7.0",
            Map.of(RemoteStoreDecider.SEGMENT_REPOSITORY_ATTRIBUTE, "segments"), 5000, 1L
        );
    }

    private static Heartbeat heartbeat(NodeId node) {
        return new Heartbeat(node, "eph", 1L, Heartbeat.Applied.NONE, null, 0,
            new Heartbeat.Capacity(1000, 500, 10, 10, 0.1));
    }

    private static ShardInventory holding(NodeId node, ShardRole role) {
        return new ShardInventory(node, "eph", 1L, Map.of(
            ACME, new ShardInventory.TenantShards(Map.of(
                UUID, new ShardInventory.IndexShards("my-index", 3041, Map.of(
                    0, ShardInventory.ShardCopy.of(role, ShardState.STARTED)
                ))
            ))
        ));
    }

    private static Assignment assignment(NodeId node, ShardRole role) {
        return new Assignment(1, POOL, 1, node, 1L, Map.of(
            ACME, new Assignment.TenantAssignment(Set.of(NodeRole.DATA), Map.of(
                UUID, new Assignment.IndexAssignment("my-index", 3041, Map.of(0, role))
            ))
        ));
    }

    /** A node holding a copy it was told to hold, which is the ordinary steady state. */
    private static Observation.NodeObservation holder(NodeId node, ShardRole role) {
        return new Observation.NodeObservation(
            node, registration(node), heartbeat(node), holding(node, role), Liveness.LIVE, POOL,
            assignment(node, role), 7
        );
    }

    private static Observation.NodeObservation empty(NodeId node) {
        return new Observation.NodeObservation(
            node, registration(node), heartbeat(node), null, Liveness.LIVE, POOL, null, 0
        );
    }

    /** One shard, one search replica wanted, no ordinary replicas — the shape the deadlock was found in. */
    private static IndexConfig index() {
        return new IndexConfig("my-index", UUID, true, 1, 0, 1, Replication.SEGMENT, false,
            AllocationEnable.UNKNOWN, 3041, Map.of());
    }

    private static Observation observe(
        long pass,
        ClusterSettings settings,
        Map<NodeId, Observation.NodeObservation> nodes
    ) {
        Map<TenantId, Observation.DesiredState> tenants = new LinkedHashMap<>();
        tenants.put(ACME, new Observation.DesiredState(Map.of(UUID, index()), List.of(), settings));
        return new Observation(pass, Instant.EPOCH, POOL, tenants, nodes, List.of());
    }

    private static Observation observe(Map<NodeId, Observation.NodeObservation> nodes) {
        return observe(3, ClusterSettings.EMPTY, nodes);
    }

    private static Plan planAfter(int passes, Observation observation) {
        FindingAges ages = new FindingAges();
        Diff diff = null;
        for (int pass = 1; pass <= passes; pass++) {
            diff = Differ.diff(new Observation(pass, observation.observedAt(), observation.pool(),
                observation.tenants(), observation.nodes(), observation.unreadable()), ages);
        }
        return Planner.plan(observation, diff, 7, Planner.Policy.defaults());
    }

    private static Instruction.RemoveCopy removalIn(Plan plan) {
        return plan.instructions().stream()
            .filter(Instruction.RemoveCopy.class::isInstance)
            .map(Instruction.RemoveCopy.class::cast)
            .findFirst()
            .orElse(null);
    }

    private static Instruction.PlaceCopy placementIn(Plan plan) {
        return plan.instructions().stream()
            .filter(Instruction.PlaceCopy.class::isInstance)
            .map(Instruction.PlaceCopy.class::cast)
            .findFirst()
            .orElse(null);
    }

    private static String refusalForPrimary(Plan plan, Diff diff) {
        Finding finding = diff.of(FindingKind.UNASSIGNED_PRIMARY).get(0);
        return plan.refusalFor(finding);
    }

    // ---------------------------------------------------------------- the deadlock

    @Test
    void theLastNodeGivesUpItsSearchOnlyCopySoItsShardCanHaveAPrimary() {
        // Found on a real one-node pool: the survivor holds the search replica, the node that held the primary is
        // long gone, and SameShardDecider correctly refuses the only candidate. Every rule right, index
        // permanently unwritable.
        Plan plan = planAfter(3, observe(Map.of(ONLY, holder(ONLY, ShardRole.SEARCH_REPLICA))));

        Instruction.RemoveCopy remove = removalIn(plan);
        assertNotNull(remove, "the read-only copy has to be given up: " + plan.instructions());
        assertEquals(ONLY, remove.node());
        assertEquals(0, remove.shard());
        assertNull(placementIn(plan), "the primary waits for the copy to be dropped, so nothing is placed yet");
    }

    @Test
    void thePrimaryIsPlacedOnceTheCopyIsActuallyGone() {
        // The second half, and it needs no memory of the first: an empty node with a shard wanting a primary is
        // just an ordinary placement.
        Plan plan = planAfter(3, observe(Map.of(ONLY, empty(ONLY))));

        Instruction.PlaceCopy place = placementIn(plan);
        assertNotNull(place);
        assertEquals(ShardRole.PRIMARY, place.role());
        assertEquals(ONLY, place.node());
    }

    @Test
    void theRefusalSaysWhatWasGivenUpAndWhy() {
        Observation observation = observe(Map.of(ONLY, holder(ONLY, ShardRole.SEARCH_REPLICA)));
        FindingAges ages = new FindingAges();
        Diff diff = null;
        for (int pass = 1; pass <= 3; pass++) {
            diff = Differ.diff(new Observation(pass, observation.observedAt(), observation.pool(),
                observation.tenants(), observation.nodes(), observation.unreadable()), ages);
        }
        Plan plan = Planner.plan(observation, diff, 7, Planner.Policy.defaults());

        String refusal = refusalForPrimary(plan, diff);
        assertNotNull(refusal);
        assertTrue(refusal.contains("cannot be promoted"),
            "the operator has to be told why the copy was worth less than the primary: " + refusal);
    }

    // ---------------------------------------------------------------- staying out of the way

    @Test
    void aPrimaryThatCanBePlacedSomewhereElseCostsNoCopyAtAll() {
        // The rule only ever runs after a real placement attempt has failed, and this is the test that it does.
        // Displacing here would delete a serving copy to avoid using an idle node.
        Plan plan = planAfter(3, observe(Map.of(
            ONLY, holder(ONLY, ShardRole.SEARCH_REPLICA),
            OTHER, empty(OTHER)
        )));

        assertNull(removalIn(plan), "nothing needed giving up: " + plan.instructions());
        Instruction.PlaceCopy place = placementIn(plan);
        assertNotNull(place);
        assertEquals(OTHER, place.node());
        assertEquals(ShardRole.PRIMARY, place.role());
    }

    @Test
    void aDocumentReplicationReplicaIsNotGivenUp() {
        // A different problem with a different answer. An ordinary replica *can* become a primary upstream, so
        // deleting one to place a primary throws away a copy that was most of the way there; the gap is promotion,
        // and deletion is not a substitute for it.
        Plan plan = planAfter(3, observe(Map.of(ONLY, holder(ONLY, ShardRole.REPLICA))));

        assertNull(removalIn(plan), "a replica is promotion's problem, not this rule's: " + plan.instructions());
    }

    @Test
    void aCopyIsKeptWhenGivingItUpWouldNotHelp() {
        // The mistake that would be worse than the deadlock: deleting a serving copy and still having nowhere to
        // put the primary. The rule asks the allocator again rather than reading `same_shard` and inferring the
        // rest, which is what makes this case answerable at all.
        Observation observation = observe(3,
            new ClusterSettings(Map.of(ClusterSettings.ALLOCATION_ENABLE, "none"), Map.of()),
            Map.of(ONLY, holder(ONLY, ShardRole.SEARCH_REPLICA)));
        FindingAges ages = new FindingAges();
        Diff diff = null;
        for (int pass = 1; pass <= 3; pass++) {
            diff = Differ.diff(new Observation(pass, observation.observedAt(), observation.pool(),
                observation.tenants(), observation.nodes(), observation.unreadable()), ages);
        }
        Plan plan = Planner.plan(observation, diff, 7, Planner.Policy.defaults());

        assertNull(removalIn(plan), "allocation is disabled, so the copy buys nothing by going");
        String refusal = refusalForPrimary(plan, diff);
        assertTrue(refusal.contains("would not help"),
            "`already holds a copy` reads as `delete it and this works`, so it has to say when that is false: "
                + refusal);
    }

    @Test
    void anAlreadyWithdrawnCopyIsWaitedForRatherThanWithdrawnAgain() {
        // The node still reports the copy, but its assignment no longer names it: told, not yet done. Re-planning
        // the removal every pass would write nothing, spend a work-ceiling slot and claim a copy had been dropped.
        Observation.NodeObservation told = new Observation.NodeObservation(
            ONLY, registration(ONLY), heartbeat(ONLY), holding(ONLY, ShardRole.SEARCH_REPLICA),
            Liveness.LIVE, POOL,
            new Assignment(2, POOL, 1, ONLY, 1L, Map.of()), 8
        );

        Observation observation = observe(Map.of(ONLY, told));
        FindingAges ages = new FindingAges();
        Diff diff = null;
        for (int pass = 1; pass <= 3; pass++) {
            diff = Differ.diff(new Observation(pass, observation.observedAt(), observation.pool(),
                observation.tenants(), observation.nodes(), observation.unreadable()), ages);
        }
        Plan plan = Planner.plan(observation, diff, 7, Planner.Policy.defaults());

        assertNull(removalIn(plan), "already withdrawn: " + plan.instructions());
        assertTrue(refusalForPrimary(plan, diff).contains("waiting for the copy to be dropped"),
            refusalForPrimary(plan, diff));
    }

    @Test
    void nothingIsGivenUpForAReplicaThatCannotBePlaced() {
        // Only a primary is worth a serving copy. A search replica that cannot be placed is a yellow index, and
        // deleting another copy to make room for it would be a straight loss.
        Observation observation = observe(Map.of(ONLY, holder(ONLY, ShardRole.PRIMARY)));
        FindingAges ages = new FindingAges();
        Diff diff = null;
        for (int pass = 1; pass <= 3; pass++) {
            diff = Differ.diff(new Observation(pass, observation.observedAt(), observation.pool(),
                observation.tenants(), observation.nodes(), observation.unreadable()), ages);
        }
        Plan plan = Planner.plan(observation, diff, 7, Planner.Policy.defaults());

        assertEquals(1, diff.of(FindingKind.UNASSIGNED_SEARCH_REPLICA).size(), "the search replica is unplaced");
        assertNull(removalIn(plan), "and the primary stays where it is: " + plan.instructions());
    }

    @Test
    void theRuleDoesNotUndoItself() {
        // The oscillation to rule out. Having given up the search replica for the primary, the pool is a node
        // holding a primary and an unplaceable search replica -- and if the rule ran in that direction too it
        // would give up the primary again, for ever. It cannot: it only ever fires for UNASSIGNED_PRIMARY.
        Plan first = planAfter(3, observe(Map.of(ONLY, holder(ONLY, ShardRole.SEARCH_REPLICA))));
        assertNotNull(removalIn(first));

        Plan afterwards = planAfter(3, observe(Map.of(ONLY, holder(ONLY, ShardRole.PRIMARY))));
        assertNull(removalIn(afterwards), "the primary must not be given up in turn: " + afterwards.instructions());
        assertNull(placementIn(afterwards), "and nothing else can be placed on a one-node pool either");
    }
}
