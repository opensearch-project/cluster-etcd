package org.opensearch.supervisor.model.balance;

import org.junit.jupiter.api.Test;
import org.opensearch.supervisor.model.node.ShardRole;
import org.opensearch.supervisor.model.observe.Observation;
import org.opensearch.supervisor.model.settings.ClusterSettings;
import org.opensearch.supervisor.model.status.Replication;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.opensearch.supervisor.model.balance.BalanceFixture.A;
import static org.opensearch.supervisor.model.balance.BalanceFixture.B;
import static org.opensearch.supervisor.model.balance.BalanceFixture.C;
import static org.opensearch.supervisor.model.balance.BalanceFixture.MIB;
import static org.opensearch.supervisor.model.balance.BalanceFixture.Node;
import static org.opensearch.supervisor.model.balance.BalanceFixture.observe;

class SurplusReaperTests {

    private static List<SurplusReaper.Removal> reap(Observation observation) {
        return SurplusReaper.reap(observation, LoadModel.of(observation));
    }

    @Test
    void aShardWithExactlyTheCopiesItWantsLosesNothing() {
        Observation observation = observe(1, 1, Replication.SEGMENT,
            Node.of(A).holding(0, ShardRole.PRIMARY, 100 * MIB),
            Node.of(B).holding(0, ShardRole.SEARCH_REPLICA, 100 * MIB)
        );

        assertEquals(List.of(), reap(observation));
    }

    @Test
    void aThirdStartedCopyIsDroppedFromTheBusiestNodeHoldingIt() {
        // Which is what makes this the second half of a move without knowing that a move happened.
        Observation observation = observe(2, 1, Replication.SEGMENT,
            Node.of(A).holding(0, ShardRole.SEARCH_REPLICA, 100 * MIB)
                .holding(1, ShardRole.SEARCH_REPLICA, 900 * MIB),
            Node.of(B).holding(0, ShardRole.PRIMARY, 100 * MIB),
            Node.of(C).holding(0, ShardRole.SEARCH_REPLICA, 100 * MIB)
        );

        List<SurplusReaper.Removal> removals = reap(observation);

        assertEquals(1, removals.size(), removals.toString());
        assertEquals(A, removals.getFirst().node(), "A is the busiest node holding a copy of shard 0");
        assertEquals(0, removals.getFirst().shard().shard());
        assertTrue(removals.getFirst().because().contains("busiest node holding it"),
            removals.getFirst().because());
    }

    @Test
    void aMoveCompletesInTheDirectionItWasAimed() {
        // The property claimed in SurplusReaper's javadoc, checked rather than asserted in prose. The threshold
        // rule only accepts a move when the source is heavier than the target by more than the copy weighs, so
        // after the copy lands the source is *still* the heavier of the two -- which means "drop the copy on the
        // busiest node" necessarily picks the source. The rule that stops oscillation is the rule that makes the
        // move land where it was aimed.
        Observation before = observe(2, 1, Replication.SEGMENT,
            Node.of(A).holding(0, ShardRole.PRIMARY, 100 * MIB)
                .holding(1, ShardRole.SEARCH_REPLICA, 900 * MIB),
            Node.of(B).holding(0, ShardRole.SEARCH_REPLICA, 100 * MIB)
                .holding(1, ShardRole.PRIMARY, 900 * MIB),
            Node.of(C)
        );
        Rebalancer.Move move = new Rebalancer().propose(before).move();
        assertEquals(A, move.source());
        assertEquals(C, move.target());

        // Now the target's copy has started, so the shard has three started copies.
        Observation landed = observe(2, 1, Replication.SEGMENT,
            Node.of(A).holding(0, ShardRole.PRIMARY, 100 * MIB)
                .holding(1, ShardRole.SEARCH_REPLICA, 900 * MIB),
            Node.of(B).holding(0, ShardRole.SEARCH_REPLICA, 100 * MIB)
                .holding(1, ShardRole.PRIMARY, 900 * MIB),
            Node.of(C).holding(1, ShardRole.SEARCH_REPLICA, 900 * MIB)
        );

        List<SurplusReaper.Removal> removals = reap(landed);

        assertEquals(1, removals.size(), removals.toString());
        assertEquals(A, removals.getFirst().node(), "the copy dropped must be the one the move was made from");
        assertEquals(1, removals.getFirst().shard().shard());
    }

    @Test
    void anArrivingCopyIsNotMistakenForRedundancy() {
        // The failure that would cost data: two started copies plus one initializing is not three copies, and
        // dropping one now would leave the shard on a single node while the third is still being built.
        Observation observation = observe(1, 1, Replication.SEGMENT,
            Node.of(A).holding(0, ShardRole.PRIMARY, 100 * MIB),
            Node.of(B).holding(0, ShardRole.SEARCH_REPLICA, 100 * MIB),
            Node.of(C).initializing(0, ShardRole.SEARCH_REPLICA)
        );

        assertEquals(List.of(), reap(observation), "nothing may be dropped until the third copy has started");
    }

    @Test
    void aPrimaryIsNeverTheCopyThatGoes() {
        // Dropping a primary is a demotion, which needs fencing this service does not have.
        Observation observation = observe(1, 1, Replication.SEGMENT,
            Node.of(A).holding(0, ShardRole.PRIMARY, 900 * MIB).disk(1000, 100),
            Node.of(B).holding(0, ShardRole.SEARCH_REPLICA, 900 * MIB),
            Node.of(C).holding(0, ShardRole.SEARCH_REPLICA, 900 * MIB)
        );

        List<SurplusReaper.Removal> removals = reap(observation);

        assertEquals(1, removals.size());
        assertTrue(removals.getFirst().node().equals(B) || removals.getFirst().node().equals(C),
            "the primary on A must survive even though A is the busiest: " + removals);
        assertEquals(ShardRole.SEARCH_REPLICA, removals.getFirst().role());
    }

    @Test
    void everySurplusCopyBeingAPrimaryMeansSomethingElseIsWrongAndNothingIsDeleted() {
        // Not a plan this service could produce, so not a state to start deleting data on the strength of.
        Observation observation = observe(1, 0, Replication.SEGMENT,
            Node.of(A).holding(0, ShardRole.PRIMARY, 100 * MIB),
            Node.of(B).holding(0, ShardRole.PRIMARY, 100 * MIB)
        );

        assertEquals(List.of(), reap(observation));
    }

    @Test
    void aFailedCopyGoesFirstBecauseDroppingItCostsNoRedundancyAtAll() {
        Observation observation = observe(1, 1, Replication.SEGMENT,
            Node.of(A).holding(0, ShardRole.PRIMARY, 100 * MIB),
            Node.of(B).holding(0, ShardRole.SEARCH_REPLICA, 100 * MIB),
            Node.of(C).holding(0, ShardRole.SEARCH_REPLICA, 100 * MIB).failed(1, ShardRole.SEARCH_REPLICA, 0)
        );

        List<SurplusReaper.Removal> removals = reap(observation);

        assertEquals(1, removals.size(), removals.toString());
        assertEquals(0, removals.getFirst().shard().shard());
        assertTrue(removals.getFirst().because().contains("busiest node holding it"),
            removals.getFirst().because());
    }

    @Test
    void aShardWithTwoSurplusCopiesLosesThemOnePassAtATime() {
        // So an observation that was wrong about this shard costs one copy rather than all of them.
        Observation observation = observe(1, 0, Replication.SEGMENT,
            Node.of(A).holding(0, ShardRole.PRIMARY, 100 * MIB),
            Node.of(B).holding(0, ShardRole.SEARCH_REPLICA, 100 * MIB),
            Node.of(C).holding(0, ShardRole.SEARCH_REPLICA, 100 * MIB)
        );

        assertEquals(1, reap(observation).size());
    }

    @Test
    void aClosedIndexKeepsItsCopiesBecauseTheyAreDormantNotSurplus() {
        // A closed index wants no copies serving, and must survive being reopened. Reading "wants zero" as
        // "delete everything" would make closing an index a destructive operation.
        Observation observation = observe(1, 1, Replication.SEGMENT, false, ClusterSettings.EMPTY,
            Node.of(A).holding(0, ShardRole.PRIMARY, 100 * MIB),
            Node.of(B).holding(0, ShardRole.SEARCH_REPLICA, 100 * MIB),
            Node.of(C).holding(0, ShardRole.SEARCH_REPLICA, 100 * MIB)
        );

        assertEquals(List.of(), reap(observation));
    }

    @Test
    void aCopyOfAnIndexNobodyDesiresIsLeftForTheOrphanRuleToHandle() {
        // Deliberately out of scope here. An index absent from desired state may be genuinely deleted or may be
        // a torn read of the control plane's own keys, and the differ already ages ORPHANED_COPY over several
        // passes for exactly that reason. Reaping it as a surplus would bypass that patience.
        Observation observation = BalanceFixture.observeWithoutIndex(
            Node.of(A).holding(0, ShardRole.PRIMARY, 100 * MIB),
            Node.of(B).holding(0, ShardRole.SEARCH_REPLICA, 100 * MIB)
        );

        assertEquals(List.of(), reap(observation));
    }

    @Test
    void theChoiceIsDeterministicSoTwoSupervisorsDropTheSameCopy() {
        // Between them they would otherwise drop two, which is how a surplus becomes a shortage.
        Observation observation = observe(1, 1, Replication.SEGMENT,
            Node.of(A).holding(0, ShardRole.PRIMARY, 100 * MIB),
            Node.of(B).holding(0, ShardRole.SEARCH_REPLICA, 100 * MIB),
            Node.of(C).holding(0, ShardRole.SEARCH_REPLICA, 100 * MIB)
        );

        for (int attempt = 0; attempt < 20; attempt++) {
            assertEquals(reap(observation), reap(observation));
        }
    }
}
