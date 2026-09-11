package org.opensearch.supervisor.model.balance;

import org.junit.jupiter.api.Test;
import org.opensearch.supervisor.model.NodeId;
import org.opensearch.supervisor.model.node.ShardRole;
import org.opensearch.supervisor.model.observe.Observation;
import org.opensearch.supervisor.model.settings.ClusterSettings;
import org.opensearch.supervisor.model.status.Replication;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.opensearch.supervisor.model.balance.BalanceFixture.A;
import static org.opensearch.supervisor.model.balance.BalanceFixture.B;
import static org.opensearch.supervisor.model.balance.BalanceFixture.C;
import static org.opensearch.supervisor.model.balance.BalanceFixture.MIB;
import static org.opensearch.supervisor.model.balance.BalanceFixture.Node;
import static org.opensearch.supervisor.model.balance.BalanceFixture.observe;

class RebalancerTests {

    /** A holds a big replica, B the matching primary, C is empty. */
    private static Observation lopsided() {
        return observe(2, 1, Replication.SEGMENT,
            Node.of(A).holding(0, ShardRole.PRIMARY, 100 * MIB)
                .holding(1, ShardRole.SEARCH_REPLICA, 900 * MIB),
            Node.of(B).holding(0, ShardRole.SEARCH_REPLICA, 100 * MIB)
                .holding(1, ShardRole.PRIMARY, 900 * MIB),
            Node.of(C)
        );
    }

    @Test
    void theHeaviestCopyOnTheBusiestNodeGoesToTheIdlestOne() {
        Rebalancer.Outcome outcome = new Rebalancer().propose(lopsided());

        assertTrue(outcome.moving(), () -> outcome.refusal().because());
        Rebalancer.Move move = outcome.move();
        assertEquals(1, move.shard().shard(), "the 900 MiB copy, not the 100 MiB one");
        assertEquals(A, move.source());
        assertEquals(C, move.target());
        assertEquals(ShardRole.SEARCH_REPLICA, move.role());
        assertTrue(move.because().contains("source pays nothing"), move.because());
    }

    @Test
    void aBalancedPoolIsAFixpointAndSaysSoWithoutBlamingAnything() {
        // The property that matters most: an already-even pool proposes nothing, so the balancer terminates.
        Observation observation = observe(3, 1, Replication.SEGMENT,
            Node.of(A).holding(0, ShardRole.PRIMARY, 100 * MIB).holding(1, ShardRole.SEARCH_REPLICA, 100 * MIB),
            Node.of(B).holding(1, ShardRole.PRIMARY, 100 * MIB).holding(2, ShardRole.SEARCH_REPLICA, 100 * MIB),
            Node.of(C).holding(2, ShardRole.PRIMARY, 100 * MIB).holding(0, ShardRole.SEARCH_REPLICA, 100 * MIB)
        );

        Rebalancer.Outcome outcome = new Rebalancer().propose(observation);

        assertFalse(outcome.moving(), () -> "proposed " + outcome.move());
        assertTrue(outcome.refusal().because().contains("no single move would reduce it"),
            outcome.refusal().because());
    }

    @Test
    void acceptingAMoveStrictlyReducesTheSpread() {
        // The termination argument in one test: spread() is non-negative and every accepted move decreases it,
        // so there can be no infinite sequence of accepted moves. Applying the proposal by hand and remeasuring
        // is the only honest way to check the claim.
        Observation before = lopsided();
        Rebalancer.Move move = new Rebalancer().propose(before).move();
        assertNotNull(move);

        Observation after = observe(2, 1, Replication.SEGMENT,
            Node.of(A).holding(0, ShardRole.PRIMARY, 100 * MIB),
            Node.of(B).holding(0, ShardRole.SEARCH_REPLICA, 100 * MIB)
                .holding(1, ShardRole.PRIMARY, 900 * MIB),
            Node.of(C).holding(1, ShardRole.SEARCH_REPLICA, 900 * MIB)
        );

        assertTrue(LoadModel.of(after).spread() < LoadModel.of(before).spread(),
            "before " + LoadModel.of(before).spread() + ", after " + LoadModel.of(after).spread());
    }

    @Test
    void oneMovePerPassEvenWhenThePoolIsWildlyLopsided() {
        // Proposing a second would mean predicting the pool's load after the first, and a prediction is the one
        // thing a level-triggered reconciler is built not to need.
        Observation observation = observe(4, 1, Replication.SEGMENT,
            Node.of(A).holding(0, ShardRole.PRIMARY, 500 * MIB)
                .holding(1, ShardRole.SEARCH_REPLICA, 500 * MIB)
                .holding(2, ShardRole.SEARCH_REPLICA, 500 * MIB)
                .holding(3, ShardRole.SEARCH_REPLICA, 500 * MIB),
            Node.of(B).holding(0, ShardRole.SEARCH_REPLICA, 500 * MIB)
                .holding(1, ShardRole.PRIMARY, 500 * MIB)
                .holding(2, ShardRole.PRIMARY, 500 * MIB)
                .holding(3, ShardRole.PRIMARY, 500 * MIB),
            Node.of(C)
        );

        Rebalancer.Outcome outcome = new Rebalancer().propose(observation);

        assertTrue(outcome.moving());
        // There is nothing here that could return more than one; the type says so. Pinned anyway, because the
        // shape of this record is the thing stopping a future change from batching moves by accident.
        assertNotNull(outcome.move());
    }

    @Test
    void theTargetMustBeSomewhereAllocationWouldHavePutACopyAnyway() {
        // The balancer does not restate what a legal placement is; it asks the allocation deciders about its
        // chosen target. Here A is much heavier than B and the threshold is happy, so every rebalance rule says
        // go -- and the move is still refused, because B already holds a copy of that shard. A balancer with its
        // own private notion of legality is one that will eventually put two copies on one node.
        Observation observation = observe(2, 1, Replication.SEGMENT,
            Node.of(A).holding(0, ShardRole.SEARCH_REPLICA, 900 * MIB)
                .holding(1, ShardRole.SEARCH_REPLICA, 900 * MIB),
            Node.of(B).holding(0, ShardRole.PRIMARY, 100 * MIB)
                .holding(1, ShardRole.PRIMARY, 100 * MIB)
        );

        Rebalancer.Outcome outcome = new Rebalancer().propose(observation);

        assertFalse(outcome.moving(), () -> "proposed " + outcome.move());
        assertTrue(outcome.refusal().because().contains("same_shard"), outcome.refusal().because());
        assertTrue(outcome.refusal().byDecider().containsKey("allocation"),
            outcome.refusal().byDecider().toString());
    }

    @Test
    void awarenessCanRefuseAMoveAndTheExplanationSaysWhichRuleDidIt() {
        // A three-zone pool where the idlest node is in a zone that already holds a copy. The balancer would
        // like to move there on load grounds; awareness says no; the operator gets told which.
        Observation observation = observe(2, 1, Replication.SEGMENT,
            Node.of(A).attribute("zone", "z1").holding(0, ShardRole.PRIMARY, 100 * MIB)
                .holding(1, ShardRole.SEARCH_REPLICA, 900 * MIB),
            Node.of(B).attribute("zone", "z2").holding(0, ShardRole.SEARCH_REPLICA, 100 * MIB)
                .holding(1, ShardRole.PRIMARY, 900 * MIB),
            Node.of(C).attribute("zone", "z2")
        );

        Rebalancer.Outcome outcome = new Rebalancer().propose(
            observation, LoadModel.of(observation), List.of("zone"), null
        );

        assertFalse(outcome.moving(), () -> "proposed " + outcome.move());
        assertTrue(outcome.refusal().because().contains("zone"), outcome.refusal().because());
    }

    @Test
    void aSingleNodePoolHasNowhereToMoveTo() {
        Observation observation = observe(1, 0, Replication.SEGMENT,
            Node.of(A).holding(0, ShardRole.PRIMARY, 100 * MIB)
        );

        Rebalancer.Outcome outcome = new Rebalancer().propose(observation);

        assertFalse(outcome.moving());
        assertTrue(outcome.refusal().because().contains("nowhere to move anything to"),
            outcome.refusal().because());
    }

    @Test
    void aRecoveringCopyIsNotMovedAndThatIsTheReasonGiven() {
        // Moving a copy mid-recovery throws away every byte transferred so far.
        Observation observation = observe(2, 1, Replication.SEGMENT,
            Node.of(A).holding(0, ShardRole.PRIMARY, 100 * MIB).initializing(1, ShardRole.SEARCH_REPLICA),
            Node.of(B).holding(0, ShardRole.SEARCH_REPLICA, 100 * MIB)
                .holding(1, ShardRole.PRIMARY, 900 * MIB),
            Node.of(C)
        );

        Rebalancer.Outcome outcome = new Rebalancer().propose(observation);

        assertFalse(outcome.moving(), () -> "proposed " + outcome.move());
        // And the reason is redundancy rather than the copy's state: shard 1 has one *started* copy where it
        // wants two, so the whole pool waits. This test caught the opposite behaviour -- an earlier version
        // counted an initializing copy as redundancy, which let balancing start the moment a replacement was
        // requested instead of when it landed, in precisely the aftermath-of-a-node-loss case the rule is for.
        assertTrue(outcome.refusal().because().contains("fewer copies than they want"),
            outcome.refusal().because());
    }

    @Test
    void theExplanationPrefersTheAnswerAnOperatorCanActOn() {
        // Two reasons hold at once: rebalancing is switched off, and the pool is also under-replicated. The
        // setting is the one they can change, so it is the one that leads.
        Observation observation = observe(2, 1, Replication.SEGMENT, true,
            new ClusterSettings(Map.of(ClusterSettings.REBALANCE_ENABLE, "none"), Map.of()),
            Node.of(A).holding(0, ShardRole.PRIMARY, 100 * MIB)
                .holding(1, ShardRole.SEARCH_REPLICA, 900 * MIB),
            Node.of(B).holding(1, ShardRole.PRIMARY, 900 * MIB),
            Node.of(C)
        );

        Rebalancer.Outcome outcome = new Rebalancer().propose(observation);

        assertFalse(outcome.moving());
        assertTrue(outcome.refusal().because().contains("rebalancing is set to [none]"),
            outcome.refusal().because());
        assertTrue(outcome.refusal().because().contains("load spread across this pool is"),
            "the spread belongs in the sentence whatever the reason: " + outcome.refusal().because());
    }

    @Test
    void everyRefusalIsKeptPerRuleNotJustTheFirst() {
        Observation observation = observe(2, 1, Replication.SEGMENT, true,
            new ClusterSettings(Map.of(ClusterSettings.REBALANCE_ENABLE, "none"), Map.of()),
            Node.of(A).holding(0, ShardRole.PRIMARY, 100 * MIB)
                .holding(1, ShardRole.SEARCH_REPLICA, 900 * MIB),
            Node.of(B).holding(1, ShardRole.PRIMARY, 900 * MIB),
            Node.of(C)
        );

        Map<String, String> byDecider = new Rebalancer().propose(observation).refusal().byDecider();

        assertTrue(byDecider.containsKey(RebalanceEnabledDecider.NAME), byDecider.toString());
        assertTrue(byDecider.containsKey(RecoveryPriorityDecider.NAME),
            "fixing only the setting would not unblock this, and the operator needs to know: " + byDecider);
    }

    @Test
    void aPoolWithNothingInItProposesNothingRatherThanFailing() {
        Observation observation = observe(1, 0, Replication.SEGMENT, Node.of(A), Node.of(B));

        Rebalancer.Outcome outcome = new Rebalancer().propose(observation);

        assertFalse(outcome.moving());
        assertEquals(0.0, LoadModel.of(observation).spread(), 1e-9);
    }

    @Test
    void theMoveIsDeterministicSoTwoSupervisorsChooseTheSameOne() {
        // Not cosmetic. Two supervisors that briefly both believe they hold a pool's lease must not create two
        // copies between them, so every tiebreak in the search order is total.
        Observation observation = lopsided();

        for (int attempt = 0; attempt < 20; attempt++) {
            Rebalancer.Move move = new Rebalancer().propose(observation).move();
            assertEquals(A, move.source());
            assertEquals(C, move.target());
            assertEquals(1, move.shard().shard());
        }
    }

    @Test
    void aCopyWithNoIndexBehindItIsNotMovedBecauseThereIsNothingToAllocateAgainst() {
        // An orphan is surplus, not misplaced -- moving it would faithfully preserve data nobody asked for.
        Observation observation = BalanceFixture.observeWithoutIndex(
            Node.of(A).holding(0, ShardRole.SEARCH_REPLICA, 900 * MIB)
                .holding(1, ShardRole.SEARCH_REPLICA, 900 * MIB),
            Node.of(B)
        );

        Rebalancer.Outcome outcome = new Rebalancer().propose(observation);

        assertFalse(outcome.moving(), () -> "proposed " + outcome.move());
        assertTrue(outcome.refusal().because().contains("surplus rather than misplaced"),
            outcome.refusal().because());
    }
}
