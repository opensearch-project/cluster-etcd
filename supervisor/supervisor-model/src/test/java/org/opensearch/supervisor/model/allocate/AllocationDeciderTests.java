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

    private static AllocationContext braked(
        String clusterValue,
        String indexValue,
        ShardRole role,
        NodeBuilder... nodes
    ) {
        Map<NodeId, Observation.NodeObservation> observed = new LinkedHashMap<>();
        for (NodeBuilder node : nodes) {
            observed.put(node.id(), node.build());
        }
        IndexConfig index = new IndexConfig("my-index", UUID, true, 1, 1, Replication.SEGMENT, false,
            AllocationEnable.from(indexValue), 3041);
        ClusterSettings settings = clusterValue == null
            ? ClusterSettings.EMPTY
            : new ClusterSettings(Map.of(ClusterSettings.ALLOCATION_ENABLE, clusterValue), Map.of());
        Observation observation = new Observation(1, Instant.EPOCH, POOL,
            Map.of(ACME, new Observation.DesiredState(Map.of(UUID, index), List.of(), settings)),
            observed, List.of());
        return new AllocationContext(ACME, index, 0, role, observation, List.of(), null, List.of());
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
        IndexConfig index = new IndexConfig("my-index", UUID, true, 1, replicas, replication, false,
            org.opensearch.supervisor.model.settings.AllocationEnable.UNKNOWN, 3041);
        Observation observation = new Observation(1, Instant.EPOCH, POOL,
            Map.of(ACME, new Observation.DesiredState(Map.of(UUID, index), List.of())), observed, List.of());
        return new AllocationContext(ACME, index, 0, role, observation, awareness, maxPerNode, List.of());
    }

    private static AllocationContext simple(ShardRole role, NodeBuilder... nodes) {
        return context(1, Replication.SEGMENT, role, List.of(), null, nodes);
    }

    /** The same, for a pool that has opted into refusing copies that share a host. */
    private static AllocationContext refusingSharedHosts(ShardRole role, NodeBuilder... nodes) {
        Map<NodeId, Observation.NodeObservation> observed = new LinkedHashMap<>();
        for (NodeBuilder node : nodes) {
            observed.put(node.id(), node.build());
        }
        IndexConfig index = new IndexConfig("my-index", UUID, true, 1, 1, Replication.SEGMENT, false,
            org.opensearch.supervisor.model.settings.AllocationEnable.UNKNOWN, 3041);
        Observation observation = new Observation(1, Instant.EPOCH, POOL,
            Map.of(ACME, new Observation.DesiredState(Map.of(UUID, index), List.of())), observed, List.of());
        return new AllocationContext(ACME, index, 0, role, observation, List.of(), null, true, List.of());
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
    void twoNodesOnOneAddressAreOneFailureDomainWhenThePoolSaysSo() {
        var context = refusingSharedHosts(ShardRole.SEARCH_REPLICA,
            NodeBuilder.of(A).at("10.0.0.7").holding(0, ShardRole.PRIMARY, ShardState.STARTED),
            NodeBuilder.of(B).at("10.0.0.7"));

        var decision = new SameShardDecider().canAllocate(context, B);
        assertEquals(Decision.Type.NO, decision.type());
        assertTrue(decision.because().contains("shares address"), decision.because());
    }

    @Test
    void sharingAnAddressIsAllowedUnlessThePoolAsks() {
        // The default, and it matches OpenSearch: cluster.routing.allocation.same_shard.host is false unless
        // set. It has to be the default here for a blunter reason too -- every node of a single-machine
        // deployment reports 127.0.0.1, so refusing by address means no replica of anything is ever placed,
        // which is what a two-node laptop test is.
        var context = simple(ShardRole.SEARCH_REPLICA,
            NodeBuilder.of(A).at("127.0.0.1").holding(0, ShardRole.PRIMARY, ShardState.STARTED),
            NodeBuilder.of(B).at("127.0.0.1"));

        var decision = new SameShardDecider().canAllocate(context, B);
        assertTrue(decision.isAllowed(), decision.because());
    }

    @Test
    void thePoolRuleNeverOverridesTheOneAboutOneNode() {
        // Opting into the host rule must not be readable as opting out of anything: the node already holding the
        // copy is still refused, by the rule that is not optional.
        var context = refusingSharedHosts(ShardRole.SEARCH_REPLICA,
            NodeBuilder.of(A).at("10.0.0.7").holding(0, ShardRole.PRIMARY, ShardState.STARTED));

        assertEquals(Decision.Type.NO, new SameShardDecider().canAllocate(context, A).type());
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

    // ---------------------------------------------------------------- the operator's brake

    @Test
    void allocationIsEnabledByDefault() {
        assertTrue(new EnableAllocationDecider()
            .canAllocate(braked(null, null, ShardRole.PRIMARY, NodeBuilder.of(A)), A).isAllowed());
    }

    @Test
    void noneStopsEverything() {
        var decision = new EnableAllocationDecider()
            .canAllocate(braked("none", null, ShardRole.PRIMARY, NodeBuilder.of(A)), A);

        assertEquals(Decision.Type.NO, decision.type());
        assertTrue(decision.because().contains("somebody asked it to stop"), decision.because());
    }

    @Test
    void primariesAllowsAPrimaryAndRefusesAReplica() {
        var decider = new EnableAllocationDecider();

        assertTrue(decider.canAllocate(braked("primaries", null, ShardRole.PRIMARY, NodeBuilder.of(A)), A)
            .isAllowed());
        assertEquals(Decision.Type.NO,
            decider.canAllocate(braked("primaries", null, ShardRole.SEARCH_REPLICA, NodeBuilder.of(A)), A).type());
    }

    @Test
    void anIndexSettingBeatsTheClusterOne() {
        // Asymmetric on purpose: an operator freezing one index should not have to know the cluster-wide value,
        // while one freezing the cluster expects it to hold wherever they have not said otherwise.
        var decider = new EnableAllocationDecider();

        assertTrue(decider.canAllocate(braked("none", "all", ShardRole.PRIMARY, NodeBuilder.of(A)), A)
            .isAllowed(), "the index says all");
        assertEquals(Decision.Type.NO,
            decider.canAllocate(braked("all", "none", ShardRole.PRIMARY, NodeBuilder.of(A)), A).type(),
            "the index says none");
    }

    @Test
    void newPrimariesIsAppliedAsNoneAndSaysWhy() {
        // Telling a never-allocated shard from one whose copies have all gone needs per-shard history that
        // nothing here keeps. Refusing is the safe direction: an operator who set this was protecting the
        // cluster from data movement, and allowing recovery would give them more of it than they asked for.
        var decision = new EnableAllocationDecider()
            .canAllocate(braked("new_primaries", null, ShardRole.PRIMARY, NodeBuilder.of(A)), A);

        assertEquals(Decision.Type.NO, decision.type());
        assertTrue(decision.because().contains("applied as"), decision.because());
        assertTrue(decision.because().contains("per-shard history"), decision.because());
    }

    @Test
    void anUnrecognisedValueStopsAllocationRatherThanBeingIgnored() {
        // A value an operator meant something by. Assuming [all] would silently ignore an instruction to stop,
        // which is the one failure mode a brake must not have.
        var decision = new EnableAllocationDecider()
            .canAllocate(braked("primaries_only_maybe", null, ShardRole.PRIMARY, NodeBuilder.of(A)), A);

        assertEquals(Decision.Type.NO, decision.type());
        assertTrue(decision.because().contains("does not recognise"), decision.because());
    }

    @Test
    void transientSettingsBeatPersistentOnes() {
        // The whole point of the distinction: transient is what an operator sets for the duration of an
        // incident, and it would be useless if a persistent value could override it.
        var settings = new ClusterSettings(
            Map.of(ClusterSettings.ALLOCATION_ENABLE, "all"),
            Map.of(ClusterSettings.ALLOCATION_ENABLE, "none")
        );
        assertEquals(AllocationEnable.NONE, settings.allocationEnable());
    }

    @Test
    void clusterSettingsReadValuesThatAreNotStrings() {
        // cluster/settings carries whatever a client sent, unlike index settings which the control plane
        // normalises, so a bare token has to survive being read.
        var settings = ClusterSettings.parse(
            "{\"persistent\":{\"cluster.routing.allocation.enable\":\"none\"},\"transient\":{}}"
                .getBytes(java.nio.charset.StandardCharsets.UTF_8)
        );
        assertEquals(AllocationEnable.NONE, settings.allocationEnable());
    }

    @Test
    void theBrakeIsTheFirstThingARefusalMentions() {
        // A refusal an operator caused should read before every refusal the cluster caused.
        var context = braked("none", null, ShardRole.PRIMARY, NodeBuilder.of(A).roles(NodeRole.COORDINATOR));
        var outcome = new Allocator().choose(context, List.of(A));

        assertFalse(outcome.placed());
        String summary = outcome.refusal().verdicts().get(A).summary();
        assertTrue(summary.indexOf("enable_allocation") < summary.indexOf("node_role"), summary);
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

    // ------------------------------------------- grouped refusals (FINDINGS_FROM_THE_DATA_PLANE 2)

    private static NodeId nodeNumbered(int n) {
        return NodeId.of("node" + String.format("%018d", n));
    }

    @Test
    void everyDistinctRefusalReasonGetsAnExample() {
        // Two candidates, two different reasons: showing one of them is showing half the answer.
        var outcome = new Allocator().choose(
            simple(ShardRole.SEARCH_REPLICA,
                NodeBuilder.of(A).holding(0, ShardRole.PRIMARY, ShardState.STARTED),
                NodeBuilder.of(B).roles(NodeRole.COORDINATOR)),
            List.of(A, B)
        );

        assertFalse(outcome.placed());
        String because = outcome.refusal().because();
        assertTrue(because.contains("2 distinct reason(s)"), because);
        assertTrue(because.contains("already holds"), because);
        assertTrue(because.contains("not data"), because);
    }

    @Test
    void theReasonBlockingMostCandidatesLeadsRatherThanWhicheverNodeSortsFirst() {
        // The regression this comes from. Node A legitimately holds the primary and is refused by the rule that is
        // obviously right and tells nobody anything; four other nodes are refused for sharing A's host, which is
        // the refusal that actually blocked progress. A sorts first, so the old code showed A's.
        List<NodeBuilder> nodes = new java.util.ArrayList<>();
        List<NodeId> candidates = new java.util.ArrayList<>();
        NodeId holder = nodeNumbered(0);
        nodes.add(NodeBuilder.of(holder).at("127.0.0.1").holding(0, ShardRole.PRIMARY, ShardState.STARTED));
        candidates.add(holder);
        for (int i = 1; i <= 4; i++) {
            nodes.add(NodeBuilder.of(nodeNumbered(i)).at("127.0.0.1"));
            candidates.add(nodeNumbered(i));
        }

        var outcome = new Allocator().choose(
            refusingSharedHosts(ShardRole.SEARCH_REPLICA, nodes.toArray(NodeBuilder[]::new)), candidates);

        assertFalse(outcome.placed());
        String because = outcome.refusal().because();
        assertTrue(because.contains("5 candidate(s) refused for 2 distinct reason(s)"), because);
        // Both present, and the one affecting four nodes comes first.
        int sharedHost = because.indexOf("shares address");
        int alreadyHolds = because.indexOf("already holds");
        assertTrue(sharedHost > 0 && alreadyHolds > 0, because);
        assertTrue(sharedHost < alreadyHolds,
            "the reason blocking four candidates must lead the one blocking a single node: " + because);
        assertTrue(because.contains("(4 nodes)"), because);
        assertTrue(because.contains("(1 node)"), because);
    }

    @Test
    void manyCandidatesRefusedTheSameWayAreOneClauseWithACount() {
        // Not twenty clauses. The string is projected into the store, so a large pool must not produce a large
        // document, and twenty copies of one sentence carry no more information than one.
        List<NodeBuilder> nodes = new java.util.ArrayList<>();
        List<NodeId> candidates = new java.util.ArrayList<>();
        for (int i = 0; i < 20; i++) {
            nodes.add(NodeBuilder.of(nodeNumbered(i)).roles(NodeRole.COORDINATOR));
            candidates.add(nodeNumbered(i));
        }

        var outcome = new Allocator().choose(
            simple(ShardRole.PRIMARY, nodes.toArray(NodeBuilder[]::new)), candidates);

        String because = outcome.refusal().because();
        assertTrue(because.contains("20 candidate(s) refused for 1 distinct reason(s)"), because);
        assertTrue(because.contains("(20 nodes)"), because);
        assertEquals(1, because.split("not data", -1).length - 1,
            "the sentence appears once, not once per node: " + because);
    }

    @Test
    void aRefusalGroupedByReasonIgnoresTheNodeNamesAndNumbersInsideIt() {
        // The property that makes grouping possible at all. These nodes are each refused for being over the disk
        // watermark, and every sentence differs -- its own node id, its own percentage -- so grouping on the prose
        // would call all three distinct. The code is what says they are the same rule.
        var outcome = new Allocator().choose(
            simple(ShardRole.PRIMARY,
                NodeBuilder.of(A).disk(1000, 40),
                NodeBuilder.of(B).disk(1000, 50),
                NodeBuilder.of(C).disk(1000, 60)),
            List.of(A, B, C)
        );

        String because = outcome.refusal().because();
        assertTrue(because.contains("3 candidate(s) refused for 1 distinct reason(s)"), because);
        assertTrue(because.contains("(3 nodes)"), because);
    }

    @Test
    void tooManyDistinctReasonsAreCappedAndTheOmissionIsStated() {
        // Bounded, and honest about being bounded.
        List<NodeBuilder> nodes = List.of(
            NodeBuilder.of(nodeNumbered(1)).roles(NodeRole.COORDINATOR),
            NodeBuilder.of(nodeNumbered(2)).disk(1000, 40),
            NodeBuilder.of(nodeNumbered(3)).holding(0, ShardRole.PRIMARY, ShardState.STARTED),
            NodeBuilder.of(nodeNumbered(4)).roles(NodeRole.DATA).at("10.9.9.9")
        );
        // Four candidates; the fourth is refused by the shard limit, giving four distinct reasons.
        var context = context(1, Replication.SEGMENT, ShardRole.SEARCH_REPLICA, List.of(), 0,
            nodes.toArray(NodeBuilder[]::new));

        var outcome = new Allocator().choose(context,
            List.of(nodeNumbered(1), nodeNumbered(2), nodeNumbered(3), nodeNumbered(4)));

        String because = outcome.refusal().because();
        assertTrue(because.contains("further distinct reason(s) not shown"), because);
        assertEquals(Allocator.MAX_DISTINCT_REASONS,
            because.split("\\(1 node\\)|\\([0-9]+ nodes\\)", -1).length - 1,
            "exactly the cap is shown: " + because);
    }

    @Test
    void theThrottledOnlySentenceIsUnchanged() {
        // "Everything is throttled, so this is paced rather than blocked" is already the right answer, and
        // grouping identical pacing messages would bury it in arithmetic.
        var outcome = new Allocator(List.of(new ThrottlingDecider(1))).choose(
            simple(ShardRole.SEARCH_REPLICA,
                NodeBuilder.of(A).holding(1, ShardRole.PRIMARY, ShardState.INITIALIZING),
                NodeBuilder.of(B).holding(2, ShardRole.PRIMARY, ShardState.INITIALIZING)),
            List.of(A, B)
        );

        assertFalse(outcome.placed());
        assertTrue(outcome.refusal().throttledOnly());
        String because = outcome.refusal().because();
        assertTrue(because.contains("is throttled, so this is paced rather than blocked"), because);
        assertFalse(because.contains("distinct reason(s)"), because);
    }

    @Test
    void theExplanationIsBoundedAndNeverEndsMidSentence() {
        // Six candidates refused in three distinct ways, each clause naming every rule that refused it. The
        // guarantee is not a character count -- it is that at most MAX_DISTINCT_REASONS clauses appear, that no
        // clause after the first is added once the budget is spent, and that characters are never cut inside a
        // sentence. Written after expecting truncation here and not getting it: with clauses this length the
        // group cap binds first, which is the right way round and worth knowing.
        List<NodeBuilder> nodes = new java.util.ArrayList<>();
        List<NodeId> candidates = new java.util.ArrayList<>();
        for (int i = 1; i <= 6; i++) {
            NodeBuilder node = NodeBuilder.of(nodeNumbered(i)).disk(1000, 1000 - i * 150);
            nodes.add(i % 2 == 0 ? node.roles(NodeRole.COORDINATOR) : node);
            candidates.add(nodeNumbered(i));
        }

        String because = new Allocator()
            .choose(context(1, Replication.SEGMENT, ShardRole.SEARCH_REPLICA, List.of(), 0,
                nodes.toArray(NodeBuilder[]::new)), candidates)
            .refusal().because();

        long clauses = because.split("\\(1 node\\)|\\([0-9]+ nodes\\)", -1).length - 1;
        assertTrue(clauses <= Allocator.MAX_DISTINCT_REASONS,
            clauses + " clauses, above the cap: " + because);
        assertTrue(because.contains("6 candidate(s) refused"), because);
        // Every clause is complete: the string ends either on a node count or on the omission sentence.
        assertTrue(because.endsWith("node)") || because.endsWith("nodes)")
                || because.endsWith("not shown"),
            "ends mid-sentence: " + because);
    }
}
