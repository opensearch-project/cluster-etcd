package org.opensearch.supervisor.model.balance;

import org.junit.jupiter.api.Test;
import org.opensearch.supervisor.model.allocate.Decision;
import org.opensearch.supervisor.model.node.ShardRole;
import org.opensearch.supervisor.model.observe.Observation;
import org.opensearch.supervisor.model.settings.ClusterSettings;
import org.opensearch.supervisor.model.settings.RebalanceEnable;
import org.opensearch.supervisor.model.status.Replication;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.opensearch.supervisor.model.balance.BalanceFixture.A;
import static org.opensearch.supervisor.model.balance.BalanceFixture.B;
import static org.opensearch.supervisor.model.balance.BalanceFixture.C;
import static org.opensearch.supervisor.model.balance.BalanceFixture.MIB;
import static org.opensearch.supervisor.model.balance.BalanceFixture.Node;
import static org.opensearch.supervisor.model.balance.BalanceFixture.move;
import static org.opensearch.supervisor.model.balance.BalanceFixture.observe;

class RebalanceDeciderTests {

    /** Two nodes, one lopsided: A holds a big replica and the primary, C holds nothing. */
    private static Observation lopsided(Replication replication, ClusterSettings settings) {
        return observe(2, 1, replication, true, settings,
            Node.of(A).holding(0, ShardRole.PRIMARY, 100 * MIB)
                .holding(1, replication == Replication.SEGMENT
                    ? ShardRole.SEARCH_REPLICA : ShardRole.REPLICA, 900 * MIB),
            Node.of(B).holding(0, replication == Replication.SEGMENT
                    ? ShardRole.SEARCH_REPLICA : ShardRole.REPLICA, 100 * MIB)
                .holding(1, ShardRole.PRIMARY, 900 * MIB),
            Node.of(C)
        );
    }

    private static Observation lopsided() {
        return lopsided(Replication.SEGMENT, ClusterSettings.EMPTY);
    }

    // ---------------------------------------------------------------- the brake

    @Test
    void rebalancingOffStopsMovesButSaysItIsNotAllocationsFault() {
        Observation observation = lopsided(Replication.SEGMENT,
            new ClusterSettings(Map.of(ClusterSettings.REBALANCE_ENABLE, "none"), Map.of()));

        Decision decision = new RebalanceEnabledDecider().canRebalance(move(observation, A, 1, C));

        assertEquals(Decision.Type.NO, decision.type());
        assertTrue(decision.because().contains("rebalancing is set to [none]"), decision.because());
    }

    @Test
    void allocationOffAlsoStopsMovesBecauseAMoveBeginsWithAnAllocation() {
        // The implication that runs one way. An operator who stopped allocation has stopped moves too, and the
        // refusal has to say which knob is responsible or they will go looking at the wrong one.
        Observation observation = lopsided(Replication.SEGMENT,
            new ClusterSettings(Map.of(ClusterSettings.ALLOCATION_ENABLE, "none"), Map.of()));

        Decision decision = new RebalanceEnabledDecider().canRebalance(move(observation, A, 1, C));

        assertEquals(Decision.Type.NO, decision.type());
        assertTrue(decision.because().contains("allocation is set to [none]"), decision.because());
        assertTrue(decision.because().contains("rebalance setting is not the one holding this up"),
            decision.because());
    }

    @Test
    void rebalancingOffDoesNotStopAllocation() {
        // The other direction, which must not hold: turning off tidying during a rolling restart is the normal
        // healthy thing to do, and it must not also stop a lost shard being rebuilt.
        assertTrue(RebalanceEnable.NONE.allows(false) == false);
        assertTrue(new ClusterSettings(Map.of(ClusterSettings.REBALANCE_ENABLE, "none"), Map.of())
            .allocationEnable().allows(false), "allocation is untouched by the rebalance setting");
    }

    @Test
    void replicasOnlyAllowsAReplicaAndRefusesAPrimary() {
        Observation observation = lopsided(Replication.SEGMENT,
            new ClusterSettings(Map.of(ClusterSettings.REBALANCE_ENABLE, "replicas"), Map.of()));
        RebalanceEnabledDecider decider = new RebalanceEnabledDecider();

        assertEquals(Decision.Type.YES, decider.canRebalance(move(observation, A, 1, C)).type());
        assertEquals(Decision.Type.NO, decider.canRebalance(move(observation, A, 0, C)).type());
    }

    @Test
    void anUnrecognisedRebalanceValueStopsMovesRatherThanBeingIgnored() {
        Observation observation = lopsided(Replication.SEGMENT,
            new ClusterSettings(Map.of(ClusterSettings.REBALANCE_ENABLE, "sometimes"), Map.of()));

        Decision decision = new RebalanceEnabledDecider().canRebalance(move(observation, A, 1, C));

        assertEquals(Decision.Type.NO, decision.type());
        assertTrue(decision.because().contains("does not"), decision.because());
        assertTrue(decision.because().contains("only an untidy pool"), decision.because());
    }

    @Test
    void unsetMeansOnBecauseSilenceIsNotAnOpinion() {
        assertEquals(RebalanceEnable.ALL, ClusterSettings.EMPTY.rebalanceEnable());
        assertEquals(Decision.Type.YES,
            new RebalanceEnabledDecider().canRebalance(move(lopsided(), A, 1, C)).type());
    }

    // ---------------------------------------------------------------- recovery first

    @Test
    void nothingMovesWhileAnyShardIsShortOfCopies() {
        // Shard 1 wants two copies and has one. Balancing must wait, because both jobs want the same bandwidth
        // and only one of them is protecting data.
        Observation observation = observe(2, 1, Replication.SEGMENT,
            Node.of(A).holding(0, ShardRole.PRIMARY, 100 * MIB).holding(1, ShardRole.PRIMARY, 900 * MIB),
            Node.of(B).holding(0, ShardRole.SEARCH_REPLICA, 100 * MIB),
            Node.of(C)
        );

        Decision decision = new RecoveryPriorityDecider().canRebalance(move(observation, A, 1, C));

        assertEquals(Decision.Type.THROTTLE, decision.type(), decision.because());
        assertTrue(decision.because().contains("1 shard(s)"), decision.because());
        assertTrue(decision.because().contains("only one of them is protecting data"), decision.because());
    }

    @Test
    void aFullyReplicatedPoolFreesTheBandwidthForBalancing() {
        assertEquals(Decision.Type.YES,
            new RecoveryPriorityDecider().canRebalance(move(lopsided(), A, 1, C)).type());
    }

    @Test
    void beingBusyWithRecoveryThrottlesRatherThanBlocks() {
        // The distinction that stops an operator investigating a pacing decision as though it were a fault.
        Observation observation = observe(2, 1, Replication.SEGMENT,
            Node.of(A).holding(0, ShardRole.PRIMARY, 100 * MIB).holding(1, ShardRole.PRIMARY, 900 * MIB),
            Node.of(B).holding(0, ShardRole.SEARCH_REPLICA, 100 * MIB),
            Node.of(C)
        );

        assertFalse(new RecoveryPriorityDecider().canRebalance(move(observation, A, 1, C)).because()
            .contains("cannot"));
    }

    // ---------------------------------------------------------------- primaries

    @Test
    void aPrimaryDoesNotMoveAndTheRefusalNamesTheMissingMechanism() {
        Decision segment = new PrimaryMoveDecider().canRebalance(move(lopsided(), A, 0, C));

        assertEquals(Decision.Type.NO, segment.type());
        assertTrue(segment.because().contains("same remote path"), segment.because());
        assertTrue(segment.because().contains("per-primary generation"), segment.because());
    }

    @Test
    void documentReplicationGivesTheOtherReasonBecauseItClosesOnItsOwnSchedule() {
        Observation observation = lopsided(Replication.DOCUMENT, ClusterSettings.EMPTY);

        Decision decision = new PrimaryMoveDecider().canRebalance(move(observation, B, 1, C));

        assertEquals(Decision.Type.NO, decision.type());
        assertTrue(decision.because().contains("primary term"), decision.because());
        assertFalse(decision.because().contains("remote path"), decision.because());
    }

    @Test
    void aReplicaMovesBecauseNothingStopsBeingTheAuthority() {
        Decision decision = new PrimaryMoveDecider().canRebalance(move(lopsided(), A, 1, C));

        assertEquals(Decision.Type.YES, decision.type());
        assertTrue(decision.because().contains("built beside the original"), decision.because());
    }

    // ---------------------------------------------------------------- the asymmetry

    @Test
    void aBusySourceDoesNotBlockARemoteStoreMoveBecauseTheSourcePaysNothing() {
        // The headline consequence of costing a move per node rather than as one scalar. A is already busy with
        // two recoveries, but under segment replication to a remote store it contributes nothing to this move:
        // the target restores from the object store. Metering the source here would refuse a free move, which is
        // exactly the wrong answer when the node being drained is the one in trouble.
        Observation observation = observe(4, 1, Replication.SEGMENT,
            Node.of(A).holding(0, ShardRole.PRIMARY, 100 * MIB)
                .holding(1, ShardRole.SEARCH_REPLICA, 900 * MIB)
                .initializing(2, ShardRole.SEARCH_REPLICA)
                .initializing(3, ShardRole.SEARCH_REPLICA),
            Node.of(B).holding(0, ShardRole.SEARCH_REPLICA, 100 * MIB)
                .holding(1, ShardRole.PRIMARY, 900 * MIB)
                .holding(2, ShardRole.PRIMARY, 10 * MIB)
                .holding(3, ShardRole.PRIMARY, 10 * MIB),
            Node.of(C)
        );

        // Pool limit raised so that the pool-wide rule is not what answers; this test is about the source.
        Decision decision = new ConcurrentRebalanceDecider(10, 2, 2)
            .canRebalance(move(observation, A, 1, C));

        assertEquals(Decision.Type.YES, decision.type(), decision.because());
        assertTrue(decision.because().contains("source is not metered"), decision.because());
        assertTrue(decision.because().contains("source pays nothing"), decision.because());
    }

    @Test
    void thatSameBusySourceDoesBlockADocumentReplicationMove() {
        // Same shape, same node, opposite answer -- and the reason is a property of the index rather than
        // anything an operator can tune. The segments come off A's disk and out of A's network card, so moving
        // more off it now would slow down the node this move is meant to relieve.
        Observation observation = observe(4, 1, Replication.DOCUMENT, true, ClusterSettings.EMPTY,
            Node.of(A).holding(0, ShardRole.PRIMARY, 100 * MIB)
                .holding(1, ShardRole.REPLICA, 900 * MIB)
                .initializing(2, ShardRole.REPLICA)
                .initializing(3, ShardRole.REPLICA),
            Node.of(B).holding(0, ShardRole.REPLICA, 100 * MIB)
                .holding(1, ShardRole.PRIMARY, 900 * MIB)
                .holding(2, ShardRole.PRIMARY, 10 * MIB)
                .holding(3, ShardRole.PRIMARY, 10 * MIB),
            Node.of(C)
        );

        Decision decision = new ConcurrentRebalanceDecider(10, 2, 2)
            .canRebalance(move(observation, A, 1, C));

        assertEquals(Decision.Type.THROTTLE, decision.type(), decision.because());
        assertTrue(decision.because().contains("already busy with 2 recoveries"), decision.because());
        assertTrue(decision.because().contains("slow down the node this move is meant to relieve"),
            decision.because());
    }

    @Test
    void theTargetIsMeteredWhicheverReplicationModeIsInUse() {
        // Because the target always writes the bytes. No asymmetry on this side.
        for (Replication replication : Replication.values()) {
            Observation observation = observe(3, 1, replication, true, ClusterSettings.EMPTY,
                Node.of(A).holding(0, ShardRole.PRIMARY, 100 * MIB)
                    .holding(1, replication == Replication.SEGMENT
                        ? ShardRole.SEARCH_REPLICA : ShardRole.REPLICA, 900 * MIB),
                Node.of(B).holding(0, replication == Replication.SEGMENT
                        ? ShardRole.SEARCH_REPLICA : ShardRole.REPLICA, 100 * MIB)
                    .holding(1, ShardRole.PRIMARY, 900 * MIB)
                    .holding(2, ShardRole.PRIMARY, 10 * MIB),
                Node.of(C).initializing(2, replication == Replication.SEGMENT
                    ? ShardRole.SEARCH_REPLICA : ShardRole.REPLICA)
            );

            Decision decision = new ConcurrentRebalanceDecider(10, 1, 10)
                .canRebalance(move(observation, A, 1, C));

            assertEquals(Decision.Type.THROTTLE, decision.type(), replication + ": " + decision.because());
            assertTrue(decision.because().contains("whichever replication"), decision.because());
        }
    }

    @Test
    void thePoolWideLimitIsCheckedBeforeAnythingPerNode() {
        // So that a pool correcting a large imbalance does so over minutes, rather than saturating every link
        // it has and becoming the reason it looks slow.
        Observation observation = observe(4, 1, Replication.SEGMENT,
            Node.of(A).holding(0, ShardRole.PRIMARY, 100 * MIB).holding(1, ShardRole.SEARCH_REPLICA, 900 * MIB),
            Node.of(B).holding(0, ShardRole.SEARCH_REPLICA, 100 * MIB).holding(1, ShardRole.PRIMARY, 900 * MIB)
                .initializing(2, ShardRole.PRIMARY),
            Node.of(C).initializing(3, ShardRole.PRIMARY)
        );

        Decision decision = new ConcurrentRebalanceDecider().canRebalance(move(observation, A, 1, C));

        assertEquals(Decision.Type.THROTTLE, decision.type(), decision.because());
        assertTrue(decision.because().contains("already in flight across this pool"), decision.because());
    }

    @Test
    void anIdlePoolIsNotThrottledAtAll() {
        Decision decision = new ConcurrentRebalanceDecider().canRebalance(move(lopsided(), A, 1, C));

        assertEquals(Decision.Type.YES, decision.type(), decision.because());
        assertTrue(decision.because().contains("0/2 in the pool"), decision.because());
    }

    // ---------------------------------------------------------------- the threshold

    @Test
    void aMoveThatWouldOvershootIsRefusedRatherThanMadeAndUnmade() {
        // The oscillation this rule exists to prevent, in its purest form: A is heavier than B, but only by
        // less than the copy weighs, so moving it makes B the heavier of the two and the same reasoning then
        // proposes moving it straight back. Forever.
        Observation observation = observe(3, 0, Replication.SEGMENT,
            Node.of(A).holding(0, ShardRole.SEARCH_REPLICA, 100 * MIB)
                .holding(1, ShardRole.SEARCH_REPLICA, 10 * MIB),
            Node.of(B).holding(2, ShardRole.SEARCH_REPLICA, 100 * MIB)
        );

        Decision decision = new MoveThresholdDecider().canRebalance(move(observation, A, 0, B));

        assertEquals(Decision.Type.NO, decision.type());
        assertTrue(decision.because().contains("would overshoot"), decision.because());
        assertTrue(decision.because().contains("propose moving it back"), decision.because());
    }

    @Test
    void aMoveThatStrictlyReducesTheSpreadIsAllowed() {
        Decision decision = new MoveThresholdDecider().canRebalance(move(lopsided(), A, 1, C));

        assertEquals(Decision.Type.YES, decision.type(), decision.because());
        assertTrue(decision.because().contains("strictly less lopsided"), decision.because());
    }

    @Test
    void theMarginLeavesANearlyBalancedPoolAlone() {
        // A pool can be slightly uneven. An operator would rather that than a cluster permanently moving data
        // to avoid being slightly uneven.
        // Tuned so the move lands strictly between the two refusals: it *would* reduce the spread, unlike the
        // overshoot case above, but by less than the 10% margin. A is 105 MiB heavier than B and the copy on
        // offer is 100 MiB, so moving it improves matters by 5 MiB and is not worth 100 MiB of traffic.
        Observation observation = observe(3, 0, Replication.SEGMENT,
            Node.of(A).holding(0, ShardRole.SEARCH_REPLICA, 100 * MIB)
                .holding(1, ShardRole.SEARCH_REPLICA, 200 * MIB),
            Node.of(B).holding(2, ShardRole.SEARCH_REPLICA, 195 * MIB)
        );

        Decision decision = new MoveThresholdDecider().canRebalance(move(observation, A, 0, B));

        assertEquals(Decision.Type.NO, decision.type(), decision.because());
        assertTrue(decision.because().contains("margin required"), decision.because());
        assertTrue(decision.because().contains("would improve the spread"), decision.because());
    }

    @Test
    void aThresholdRefusalIsPermanentRatherThanPacedAndSaysSo() {
        // Unlike the other rebalance rules. Nothing about waiting changes the shape of the pool, so reporting
        // "try again shortly" would send an operator back every few minutes to watch nothing happen.
        Observation observation = observe(3, 0, Replication.SEGMENT,
            Node.of(A).holding(0, ShardRole.SEARCH_REPLICA, 100 * MIB)
                .holding(1, ShardRole.SEARCH_REPLICA, 10 * MIB),
            Node.of(B).holding(2, ShardRole.SEARCH_REPLICA, 100 * MIB)
        );

        assertEquals(Decision.Type.NO,
            new MoveThresholdDecider().canRebalance(move(observation, A, 0, B)).type());
    }

    // ---------------------------------------------------------------- the cost model itself

    @Test
    void segmentReplicationCostsTheTargetOnlyAndDocumentReplicationCostsBoth() {
        MoveCost remote = MoveCost.of(500 * MIB, Replication.SEGMENT);
        MoveCost node = MoveCost.of(500 * MIB, Replication.DOCUMENT);

        assertEquals(0L, remote.sourceBytes());
        assertEquals(500 * MIB, remote.targetBytes());
        assertFalse(remote.loadsSource());

        assertEquals(500 * MIB, node.sourceBytes());
        assertEquals(500 * MIB, node.targetBytes());
        assertTrue(node.loadsSource());
    }

    @Test
    void anIndexWhoseConfigurationHasGoneIsCostedAsTheExpensiveKind() {
        // Not the moment to assume the cheap reading. A copy of a deleted index still weighs on its node, and
        // guessing "remote store, so free" about it would let the source be hammered.
        Observation observation = BalanceFixture.observeWithoutIndex(
            Node.of(A).holding(0, ShardRole.SEARCH_REPLICA, 100 * MIB),
            Node.of(B)
        );
        LoadModel loads = LoadModel.of(observation);
        RebalanceContext context = new RebalanceContext(
            observation, loads, loads.heldBy(A).getFirst(), B, null, 0.1
        );

        assertEquals(Replication.DOCUMENT, context.replication());
        assertTrue(context.cost().loadsSource());
    }
}
