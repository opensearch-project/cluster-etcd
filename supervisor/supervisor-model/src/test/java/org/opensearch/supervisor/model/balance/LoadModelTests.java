package org.opensearch.supervisor.model.balance;

import org.junit.jupiter.api.Test;
import org.opensearch.supervisor.model.node.ShardRole;
import org.opensearch.supervisor.model.observe.Observation;
import org.opensearch.supervisor.model.status.Replication;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.opensearch.supervisor.model.balance.BalanceFixture.A;
import static org.opensearch.supervisor.model.balance.BalanceFixture.B;
import static org.opensearch.supervisor.model.balance.BalanceFixture.C;
import static org.opensearch.supervisor.model.balance.BalanceFixture.MIB;
import static org.opensearch.supervisor.model.balance.BalanceFixture.Node;
import static org.opensearch.supervisor.model.balance.BalanceFixture.observe;

class LoadModelTests {

    @Test
    void aNodeHoldingTheBytesIsLoadedEvenWhenItHoldsFewerShards() {
        // The whole reason for a size signal, and the case OpenSearch's count-only balancer cannot see: by
        // shard count B is three times as busy as A, and by any measure an operator cares about it is idle.
        Observation observation = observe(4, 1, Replication.SEGMENT,
            Node.of(A).holding(0, ShardRole.PRIMARY, 900 * MIB),
            Node.of(B).holding(1, ShardRole.PRIMARY, 10 * MIB)
                .holding(2, ShardRole.PRIMARY, 10 * MIB)
                .holding(3, ShardRole.PRIMARY, 10 * MIB)
        );

        LoadModel loads = LoadModel.of(observation);

        assertTrue(loads.loadOf(A) > loads.loadOf(B),
            "A holds 900 MiB in one shard, B holds 30 MiB in three: " + loads.loads());
        assertEquals(List.of(A, B), loads.byLoadDescending());
    }

    @Test
    void countStillCountsForSomething() {
        // Not a size-only model. Two nodes holding the same bytes are not equally loaded if one spreads them
        // over far more shards, because per-shard overhead is real even when the shards are small.
        Observation observation = observe(5, 0, Replication.SEGMENT,
            Node.of(A).holding(0, ShardRole.PRIMARY, 100 * MIB),
            Node.of(B).holding(1, ShardRole.PRIMARY, 25 * MIB)
                .holding(2, ShardRole.PRIMARY, 25 * MIB)
                .holding(3, ShardRole.PRIMARY, 25 * MIB)
                .holding(4, ShardRole.PRIMARY, 25 * MIB)
        );

        LoadModel loads = LoadModel.of(observation);

        assertTrue(loads.loadOf(B) > loads.loadOf(A),
            "equal bytes, but B carries four shards' worth of overhead: " + loads.loads());
    }

    @Test
    void withEqualSizedShardsTheTwoSignalsAreTheSameSignal() {
        // Worth pinning because it is the case where the weights provably cannot matter, and it explains why
        // tuning them does nothing on a uniform cluster: normalised by their own means, size and count are
        // identical functions when every shard is identical.
        Observation observation = observe(4, 0, Replication.SEGMENT,
            Node.of(A).holding(0, ShardRole.PRIMARY, 50 * MIB).holding(1, ShardRole.PRIMARY, 50 * MIB),
            Node.of(B).holding(2, ShardRole.PRIMARY, 50 * MIB),
            Node.of(C).holding(3, ShardRole.PRIMARY, 50 * MIB)
        );

        LoadModel sizeHeavy = LoadModel.of(observation, LoadModel.defaultSignals(),
            new LoadModel.Importance(Map.of(ShardSizeSignal.NAME, 1.0, ShardCountSignal.NAME, 0.0)));
        LoadModel countHeavy = LoadModel.of(observation, LoadModel.defaultSignals(),
            new LoadModel.Importance(Map.of(ShardSizeSignal.NAME, 0.0, ShardCountSignal.NAME, 1.0)));

        assertEquals(sizeHeavy.loadOf(A), countHeavy.loadOf(A), 1e-9);
        assertEquals(sizeHeavy.byLoadDescending(), countHeavy.byLoadDescending());
    }

    @Test
    void unitsCannotDominateEachOther() {
        // The arithmetic bug this class exists to prevent. Bytes are ~10^8 and counts are ~1, so an unnormalised
        // weighted sum is the size signal wearing a hat -- and an operator who set count to 100 would still see
        // no change. After normalisation, weighting count above size actually reverses the ordering.
        Observation observation = observe(3, 0, Replication.SEGMENT,
            Node.of(A).holding(0, ShardRole.PRIMARY, 1000 * MIB),
            Node.of(B).holding(1, ShardRole.PRIMARY, 1 * MIB).holding(2, ShardRole.PRIMARY, 1 * MIB)
        );

        LoadModel bySize = LoadModel.of(observation, LoadModel.defaultSignals(),
            new LoadModel.Importance(Map.of(ShardSizeSignal.NAME, 1.0, ShardCountSignal.NAME, 0.0)));
        LoadModel byCount = LoadModel.of(observation, LoadModel.defaultSignals(),
            new LoadModel.Importance(Map.of(ShardSizeSignal.NAME, 0.0, ShardCountSignal.NAME, 1.0)));

        assertEquals(List.of(A, B), bySize.byLoadDescending());
        assertEquals(List.of(B, A), byCount.byLoadDescending(), "count must be able to win: " + byCount.loads());
    }

    @Test
    void aCopyOnItsWayAlreadyCountsAgainstItsTarget() {
        // Otherwise the balancer stampedes: a node handed ten replicas one second ago still looks empty, so it
        // gets ten more. The assignment is the statement that the work is coming.
        Observation empty = observe(2, 1, Replication.SEGMENT,
            Node.of(A).holding(0, ShardRole.PRIMARY, 100 * MIB),
            Node.of(B)
        );
        Observation told = observe(2, 1, Replication.SEGMENT,
            Node.of(A).holding(0, ShardRole.PRIMARY, 100 * MIB),
            Node.of(B).told(0, ShardRole.SEARCH_REPLICA)
        );

        assertEquals(0.0, LoadModel.of(empty).loadOf(B), 1e-9);
        assertTrue(LoadModel.of(told).loadOf(B) > 0.0,
            "a node told to hold a copy is not idle: " + LoadModel.of(told).loads());
    }

    @Test
    void anArrivingCopyIsWeighedAtTheSizeOfItsOwnShardElsewhere() {
        // The subtle half of the previous test. An initializing copy reports no size, and a shard with no size
        // weighs nothing -- so without the peer estimate the stampede comes back through the door marked
        // "counted, but at zero". A replica ends up about the size of the primary it is built from, and that
        // figure is an observation of this very shard rather than a guess.
        Observation observation = observe(1, 1, Replication.SEGMENT,
            Node.of(A).holding(0, ShardRole.PRIMARY, 800 * MIB),
            Node.of(B).initializing(0, ShardRole.SEARCH_REPLICA)
        );

        LoadModel loads = LoadModel.of(observation);
        LoadModel.Held arriving = loads.heldBy(B).getFirst();

        assertEquals(800 * MIB, arriving.sizeBytes(),
            "the arriving copy should be weighed at the primary's size");
        assertEquals(loads.loadOf(A), loads.loadOf(B), 1e-9,
            "which makes the two nodes equally loaded, because they will be: " + loads.loads());
    }

    @Test
    void aBrandNewShardWithNoSizedCopyAnywhereWeighsNothingRatherThanBeingInvented() {
        Observation observation = observe(1, 1, Replication.SEGMENT,
            Node.of(A).initializing(0, ShardRole.PRIMARY),
            Node.of(B)
        );

        LoadModel loads = LoadModel.of(observation);

        assertEquals(0L, loads.heldBy(A).getFirst().sizeBytes());
        // Still not zero load: the count signal sees it, which is right -- one shard is being built here and
        // none over there.
        assertTrue(loads.loadOf(A) > loads.loadOf(B), loads.loads().toString());
    }

    @Test
    void aLandedCopyIsNotCountedTwiceForBeingBothHeldAndAssigned() {
        // The overlap that would make every settled node look twice as loaded as it is, for as long as its
        // assignment stood -- which is forever.
        Observation observation = observe(1, 1, Replication.SEGMENT,
            Node.of(A).holding(0, ShardRole.PRIMARY, 100 * MIB).told(0, ShardRole.PRIMARY),
            Node.of(B)
        );

        LoadModel loads = LoadModel.of(observation);

        assertEquals(1, loads.heldBy(A).size(), "one copy, not two: " + loads.heldBy(A));
    }

    @Test
    void copiesComeBackHeaviestFirst() {
        // The order the rebalancer walks, and the order that fixes the most imbalance per byte moved.
        Observation observation = observe(3, 0, Replication.SEGMENT,
            Node.of(A).holding(0, ShardRole.PRIMARY, 10 * MIB)
                .holding(1, ShardRole.PRIMARY, 900 * MIB)
                .holding(2, ShardRole.PRIMARY, 100 * MIB),
            Node.of(B)
        );

        assertEquals(List.of(1, 2, 0), BalanceFixture.shardsOf(LoadModel.of(observation).heldBy(A)));
    }

    @Test
    void aDeadNodesLoadIsNobodysProblem() {
        // Consistent with the planner, the task queue and the allocation context: a lost node's copies are not
        // redundancy and its load is not something to relieve. Counting it would make the pool look lopsided in
        // a direction no move can fix.
        Observation observation = observe(2, 1, Replication.SEGMENT,
            Node.of(A).holding(0, ShardRole.PRIMARY, 900 * MIB)
                .liveness(org.opensearch.supervisor.model.status.Liveness.LOST),
            Node.of(B).holding(1, ShardRole.PRIMARY, 100 * MIB)
        );

        LoadModel loads = LoadModel.of(observation);

        assertEquals(Map.of(B, loads.loadOf(B)), loads.loads());
        assertEquals(0.0, loads.spread(), 1e-9, "one live node cannot be unbalanced against itself");
    }

    @Test
    void spreadIsTheGapBetweenTheBusiestAndIdlestNode() {
        Observation observation = observe(2, 0, Replication.SEGMENT,
            Node.of(A).holding(0, ShardRole.PRIMARY, 100 * MIB).holding(1, ShardRole.PRIMARY, 100 * MIB),
            Node.of(B),
            Node.of(C)
        );

        LoadModel loads = LoadModel.of(observation);

        assertEquals(loads.loadOf(A) - loads.loadOf(B), loads.spread(), 1e-9);
        assertNotEquals(0.0, loads.spread());
    }
}
