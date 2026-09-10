package org.opensearch.supervisor.model.allocate;

import org.junit.jupiter.api.Test;
import org.opensearch.supervisor.model.NodeId;
import org.opensearch.supervisor.model.PoolId;
import org.opensearch.supervisor.model.TenantId;
import org.opensearch.supervisor.model.index.IndexConfig;
import org.opensearch.supervisor.model.node.Heartbeat;
import org.opensearch.supervisor.model.node.NodeRole;
import org.opensearch.supervisor.model.node.Registration;
import org.opensearch.supervisor.model.node.ShardInventory;
import org.opensearch.supervisor.model.node.ShardRole;
import org.opensearch.supervisor.model.node.ShardState;
import org.opensearch.supervisor.model.observe.Observation;
import org.opensearch.supervisor.model.status.Liveness;
import org.opensearch.supervisor.model.status.Replication;

import java.time.Instant;
import java.util.EnumSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * One rule at a time.
 *
 * <p>The point of a decider per rule is that each can be examined on its own, so these tests are the payoff:
 * every rule gets a situation it should refuse and one it should allow, and no test needs to reason about what
 * any other rule would have said.
 */
class AllocationDeciderTests {

    private static final PoolId POOL = PoolId.of("shared-a");
    private static final TenantId ACME = TenantId.of("acme");
    private static final String UUID = "uzKxLWZu4nPSrTknEcDpoA";
    private static final NodeId A = NodeId.of("nodeAAAAAAAAAAAAAAAAAA");
    private static final NodeId B = NodeId.of("nodeBBBBBBBBBBBBBBBBBB");
    private static final NodeId C = NodeId.of("nodeCCCCCCCCCCCCCCCCCC");

    // ---------------------------------------------------------------- fixtures

    private static Registration registration(
        NodeId node,
        String address,
        Set<NodeRole> roles,
        Map<String, String> attributes
    ) {
        return new Registration(node, "eph", node.value(), POOL, roles, address, 9300, "3.7.0",
            attributes, 5000, 1L);
    }

    private static Heartbeat heartbeat(NodeId node, long total, long free) {
        return new Heartbeat(node, "eph", 1L, Heartbeat.Applied.NONE, null, 0,
            new Heartbeat.Capacity(total, free, 10, 10, 0.1));
    }

    private record NodeBuilder(
        NodeId id,
        String address,
        Set<NodeRole> roles,
        Map<String, String> attributes,
        long total,
        long free,
        Map<Integer, ShardInventory.ShardCopy> shards,
        Liveness liveness
    ) {
        static NodeBuilder of(NodeId id) {
            return new NodeBuilder(id, "10.0.0." + id.value().charAt(4), Set.of(NodeRole.DATA), Map.of(),
                1000, 900, Map.of(), Liveness.LIVE);
        }

        NodeBuilder at(String address) {
            return new NodeBuilder(id, address, roles, attributes, total, free, shards, liveness);
        }

        NodeBuilder roles(NodeRole... values) {
            return new NodeBuilder(id, address, Set.of(values), attributes, total, free, shards, liveness);
        }

        NodeBuilder attribute(String key, String value) {
            Map<String, String> next = new LinkedHashMap<>(attributes);
            next.put(key, value);
            return new NodeBuilder(id, address, roles, next, total, free, shards, liveness);
        }

        NodeBuilder disk(long total, long free) {
            return new NodeBuilder(id, address, roles, attributes, total, free, shards, liveness);
        }

        NodeBuilder holding(int shard, ShardRole role, ShardState state) {
            Map<Integer, ShardInventory.ShardCopy> next = new LinkedHashMap<>(shards);
            next.put(shard, ShardInventory.ShardCopy.of(role, state));
            return new NodeBuilder(id, address, roles, attributes, total, free, next, liveness);
        }

        Observation.NodeObservation build() {
            ShardInventory inventory = shards.isEmpty() ? null : new ShardInventory(id, "eph", 1L, Map.of(
                ACME, new ShardInventory.TenantShards(Map.of(
                    UUID, new ShardInventory.IndexShards("my-index", 3041, shards)
                ))
            ));
            return new Observation.NodeObservation(
                id, registration(id, address, roles, attributes),
                heartbeat(id, total, free), inventory, liveness, POOL, null, 0
            );
        }
    }

    private static AllocationContext context(
        int replicas,
        Replication replication,
        ShardRole role,
        List<String> awareness,
        Integer maxPerNode,
        NodeBuilder... nodes
    ) {
        Map<NodeId, Observation.NodeObservation> observed = new LinkedHashMap<>();
        for (NodeBuilder node : nodes) {
            observed.put(node.id(), node.build());
        }
        IndexConfig index = new IndexConfig("my-index", UUID, true, 1, replicas, replication, false, 3041);
        Observation observation = new Observation(1, Instant.EPOCH, POOL,
            Map.of(ACME, new Observation.DesiredState(Map.of(UUID, index), List.of())), observed, List.of());
        return new AllocationContext(ACME, index, 0, role, observation, awareness, maxPerNode, List.of());
    }

    private static AllocationContext simple(ShardRole role, NodeBuilder... nodes) {
        return context(1, Replication.SEGMENT, role, List.of(), null, nodes);
    }

    // ---------------------------------------------------------------- node role

    @Test
    void aCoordinatorOnlyNodeTakesNoShardCopies() {
        // This rule was missing: isPlaceable checks registered, pooled and live but never what a node is for,
        // so a coordinator was a perfectly good target. It would have accepted the assignment, done nothing
        // with it, and left the shard unrecovered with the diff insisting it was placed.
        var decision = new NodeRoleDecider().canAllocate(
            simple(ShardRole.PRIMARY, NodeBuilder.of(A).roles(NodeRole.COORDINATOR)), A
        );

        assertEquals(Decision.Type.NO, decision.type());
        assertTrue(decision.because().contains("not data"), decision.because());
    }

    @Test
    void aDataNodeIsAllowed() {
        assertTrue(new NodeRoleDecider()
            .canAllocate(simple(ShardRole.PRIMARY, NodeBuilder.of(A)), A).isAllowed());
    }

    // ---------------------------------------------------------------- same shard

    @Test
    void asSecondCopyNeverSharesANodeWithTheFirst() {
        var context = simple(ShardRole.SEARCH_REPLICA,
            NodeBuilder.of(A).holding(0, ShardRole.PRIMARY, ShardState.STARTED), NodeBuilder.of(B));

        assertEquals(Decision.Type.NO, new SameShardDecider().canAllocate(context, A).type());
        assertTrue(new SameShardDecider().canAllocate(context, B).isAllowed());
    }

    @Test
    void twoNodesOnOneAddressAreOneFailureDomain() {
        var context = simple(ShardRole.SEARCH_REPLICA,
            NodeBuilder.of(A).at("10.0.0.7").holding(0, ShardRole.PRIMARY, ShardState.STARTED),
            NodeBuilder.of(B).at("10.0.0.7"));

        var decision = new SameShardDecider().canAllocate(context, B);
        assertEquals(Decision.Type.NO, decision.type());
        assertTrue(decision.because().contains("shares address"), decision.because());
    }

    @Test
    void aCopyPlannedEarlierInTheSamePassAlreadyCounts() {
        // Two placements in one plan must not both land on the same node. Without this the pass would produce
        // an assignment that violates the rule it just applied.
        var context = simple(ShardRole.SEARCH_REPLICA, NodeBuilder.of(A), NodeBuilder.of(B))
            .plus(new AllocationContext.PlannedCopy(ACME, UUID, 0, ShardRole.PRIMARY, A));

        assertEquals(Decision.Type.NO, new SameShardDecider().canAllocate(context, A).type());
        assertTrue(new SameShardDecider().canAllocate(context, B).isAllowed());
    }

    // ---------------------------------------------------------------- awareness

    @Test
    void awarenessSpreadsTwoCopiesAcrossTwoZones() {
        var context = context(1, Replication.SEGMENT, ShardRole.SEARCH_REPLICA, List.of("zone"), null,
            NodeBuilder.of(A).attribute("zone", "z1").holding(0, ShardRole.PRIMARY, ShardState.STARTED),
            NodeBuilder.of(B).attribute("zone", "z1"),
            NodeBuilder.of(C).attribute("zone", "z2"));

        var decider = new AwarenessDecider();
        assertEquals(Decision.Type.NO, decider.canAllocate(context, B).type(), "z1 already has its share");
        assertTrue(decider.canAllocate(context, C).isAllowed(), "z2 has none");
    }

    @Test
    void awarenessUsesCeilingSoAShardIsNeverUnplaceable() {
        // Three copies across two zones: forbidding two in one zone would make the third copy unplaceable
        // rather than imperfectly placed, which is a worse answer than an uneven spread.
        var context = context(2, Replication.SEGMENT, ShardRole.SEARCH_REPLICA, List.of("zone"), null,
            NodeBuilder.of(A).attribute("zone", "z1").holding(0, ShardRole.PRIMARY, ShardState.STARTED),
            NodeBuilder.of(B).attribute("zone", "z1"),
            NodeBuilder.of(C).attribute("zone", "z2"));

        // ceil(3 copies / 2 zones) = 2, and z1 has one, so a second in z1 is allowed.
        assertTrue(new AwarenessDecider().canAllocate(context, B).isAllowed());
    }

    @Test
    void aNodeWithNoAwarenessAttributeIsRefusedWhenThePoolSpreadsOnIt() {
        var context = context(1, Replication.SEGMENT, ShardRole.PRIMARY, List.of("zone"), null,
            NodeBuilder.of(A));

        var decision = new AwarenessDecider().canAllocate(context, A);
        assertEquals(Decision.Type.NO, decision.type());
        assertTrue(decision.because().contains("declares no [zone]"), decision.because());
    }

    @Test
    void awarenessIsOffWhenThePoolNamesNoAttributes() {
        assertTrue(new AwarenessDecider()
            .canAllocate(simple(ShardRole.PRIMARY, NodeBuilder.of(A)), A).isAllowed());
    }

    // ---------------------------------------------------------------- disk

    @Test
    void aNodeOverTheLowWatermarkTakesNothingNew() {
        // 86% full: past the low watermark and short of the high one, which is the band this test is about.
        var context = simple(ShardRole.PRIMARY, NodeBuilder.of(A).disk(1000, 140));

        var decision = new DiskThresholdDecider().canAllocate(context, A);
        assertEquals(Decision.Type.NO, decision.type());
        assertTrue(decision.because().contains("low watermark"), decision.because());
    }

    @Test
    void aNodeOverTheHighWatermarkSaysSoDifferently() {
        // The two watermarks answer different questions, and an operator needs to tell "stop adding" from
        // "start removing".
        var context = simple(ShardRole.PRIMARY, NodeBuilder.of(A).disk(1000, 50));

        assertTrue(new DiskThresholdDecider().canAllocate(context, A).because().contains("high watermark"));
    }

    @Test
    void aNodeWithRoomIsAllowed() {
        assertTrue(new DiskThresholdDecider()
            .canAllocate(simple(ShardRole.PRIMARY, NodeBuilder.of(A).disk(1000, 900)), A).isAllowed());
    }

    @Test
    void aNodeReportingNoDiskFiguresIsRefusedRatherThanAssumedEmpty() {
        // Guessing optimistically fills up the one node nobody can see, and disk is the failure the rule exists
        // for. There is nothing conservative about an optimistic default here.
        var context = simple(ShardRole.PRIMARY, NodeBuilder.of(A).disk(0, 0));

        assertEquals(Decision.Type.NO, new DiskThresholdDecider().canAllocate(context, A).type());
    }

    @Test
    void watermarksMustBeOrdered() {
        assertThrows(IllegalArgumentException.class, () -> new DiskThresholdDecider(0.95, 0.85));
    }

    // ---------------------------------------------------------------- shard limits

    @Test
    void aNodeAtThePoolsShardLimitTakesNothingMore() {
        var context = context(1, Replication.SEGMENT, ShardRole.PRIMARY, List.of(), 1,
            NodeBuilder.of(A).holding(5, ShardRole.PRIMARY, ShardState.STARTED));

        var decision = new ShardsLimitDecider().canAllocate(context, A);
        assertEquals(Decision.Type.NO, decision.type());
        assertTrue(decision.because().contains("limit of 1"), decision.because());
    }

    @Test
    void theShardLimitIsOffWhenThePoolSetsNone() {
        // Guessing a cap would refuse placements for a reason nobody chose.
        var context = context(1, Replication.SEGMENT, ShardRole.PRIMARY, List.of(), null,
            NodeBuilder.of(A).holding(5, ShardRole.PRIMARY, ShardState.STARTED));

        assertTrue(new ShardsLimitDecider().canAllocate(context, A).isAllowed());
    }

    // ---------------------------------------------------------------- throttling

    @Test
    void aNodeAlreadyRecoveringItsLimitIsThrottledRatherThanRefused() {
        // THROTTLE and NO are different answers: one paces the work, the other blocks it, and an operator
        // reading the diff needs to know which.
        var context = simple(ShardRole.PRIMARY,
            NodeBuilder.of(A)
                .holding(1, ShardRole.PRIMARY, ShardState.INITIALIZING)
                .holding(2, ShardRole.PRIMARY, ShardState.INITIALIZING));

        var decision = new ThrottlingDecider(2).canAllocate(context, A);
        assertEquals(Decision.Type.THROTTLE, decision.type());
        assertFalse(decision.isAllowed());
        assertTrue(decision.because().contains("pacing"), decision.because());
    }

    @Test
    void aNodeRecoveringLessThanItsLimitIsAllowed() {
        var context = simple(ShardRole.PRIMARY,
            NodeBuilder.of(A).holding(1, ShardRole.PRIMARY, ShardState.INITIALIZING));

        assertTrue(new ThrottlingDecider(2).canAllocate(context, A).isAllowed());
    }

    @Test
    void aThrottleOfZeroWouldPlaceNothingEverAndIsRejected() {
        assertThrows(IllegalArgumentException.class, () -> new ThrottlingDecider(0));
    }

    // ---------------------------------------------------------------- replica after primary

    @Test
    void aDocrepReplicaWaitsForAServingPrimary() {
        // DESIGN.md open question 6, closed as a decider: the rule is a property of a candidate placement, not
        // a phase of the planner.
        var waiting = context(1, Replication.DOCUMENT, ShardRole.REPLICA, List.of(), null,
            NodeBuilder.of(A).holding(0, ShardRole.PRIMARY, ShardState.INITIALIZING), NodeBuilder.of(B));

        var decision = new ReplicaAfterPrimaryDecider().canAllocate(waiting, B);
        assertEquals(Decision.Type.THROTTLE, decision.type(), "a wait, not a refusal");

        var ready = context(1, Replication.DOCUMENT, ShardRole.REPLICA, List.of(), null,
            NodeBuilder.of(A).holding(0, ShardRole.PRIMARY, ShardState.STARTED), NodeBuilder.of(B));
        assertTrue(new ReplicaAfterPrimaryDecider().canAllocate(ready, B).isAllowed());
    }

    @Test
    void aSegrepReplicaWaitsForNothing() {
        // It syncs from the remote store and never contacts the primary, which is why the rule is per index
        // rather than global.
        var context = context(1, Replication.SEGMENT, ShardRole.SEARCH_REPLICA, List.of(), null,
            NodeBuilder.of(A).holding(0, ShardRole.PRIMARY, ShardState.INITIALIZING), NodeBuilder.of(B));

        assertTrue(new ReplicaAfterPrimaryDecider().canAllocate(context, B).isAllowed());
    }

    @Test
    void aPrimaryNeverWaitsForItself() {
        var context = context(1, Replication.DOCUMENT, ShardRole.PRIMARY, List.of(), null, NodeBuilder.of(A));

        assertTrue(new ReplicaAfterPrimaryDecider().canAllocate(context, A).isAllowed());
    }

    // ---------------------------------------------------------------- the allocator over all of them

    @Test
    void theAllocatorPicksTheLeastLoadedAllowedNode() {
        var context = simple(ShardRole.PRIMARY,
            NodeBuilder.of(A).holding(7, ShardRole.PRIMARY, ShardState.STARTED),
            NodeBuilder.of(B));

        var outcome = new Allocator().choose(context, List.of(A, B));
        assertEquals(B, outcome.node().orElseThrow(), "B holds nothing");
    }

    @Test
    void shardCountBeatsFreeDisk() {
        // Balancing on disk alone concentrates shards on whichever machine has the largest volume, which is
        // how a cluster ends up with one node holding most of its shards and plenty of space left.
        var context = simple(ShardRole.PRIMARY,
            NodeBuilder.of(A).disk(1_000_000, 900_000).holding(7, ShardRole.PRIMARY, ShardState.STARTED),
            NodeBuilder.of(B).disk(1000, 900));

        assertEquals(B, new Allocator().choose(context, List.of(A, B)).node().orElseThrow());
    }

    @Test
    void aRefusalKeepsEveryReasonRatherThanTheFirst() {
        // A candidate rejected for three reasons is worth knowing about in full: fixing only the first would
        // not help, and an operator who cannot see the other two will try.
        var context = context(1, Replication.SEGMENT, ShardRole.PRIMARY, List.of("zone"), 0,
            NodeBuilder.of(A).roles(NodeRole.COORDINATOR).disk(1000, 10));

        var outcome = new Allocator().choose(context, List.of(A));
        assertFalse(outcome.placed());

        var summary = outcome.refusal().verdicts().get(A).summary();
        assertTrue(summary.contains("node_role"), summary);
        assertTrue(summary.contains("awareness"), summary);
        assertTrue(summary.contains("disk_threshold"), summary);
    }

    @Test
    void anAllThrottledRefusalSaysItIsPacedRatherThanBlocked() {
        var context = simple(ShardRole.PRIMARY,
            NodeBuilder.of(A)
                .holding(1, ShardRole.PRIMARY, ShardState.INITIALIZING)
                .holding(2, ShardRole.PRIMARY, ShardState.INITIALIZING));

        var outcome = new Allocator(List.of(new ThrottlingDecider(2))).choose(context, List.of(A));
        assertTrue(outcome.refusal().throttledOnly());
        assertTrue(outcome.refusal().because().contains("paced"), outcome.refusal().because());
    }

    @Test
    void noCandidatesIsItsOwnAnswer() {
        var outcome = new Allocator().choose(simple(ShardRole.PRIMARY, NodeBuilder.of(A)), List.of());

        assertFalse(outcome.placed());
        assertTrue(outcome.refusal().because().contains("no candidate nodes"));
    }

    @Test
    void anAllocatorWithNoDecidersIsRejected() {
        // It would place anything anywhere, which is worse than having no allocator at all.
        assertThrows(IllegalArgumentException.class, () -> new Allocator(List.of()));
    }

    @Test
    void everyDefaultDeciderHasAUniqueName() {
        var names = Allocator.defaultDeciders().stream().map(AllocationDecider::name).toList();
        assertEquals(names.size(), Set.copyOf(names).size(), names.toString());
    }
}
