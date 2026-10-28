package org.opensearch.supervisor.model.plan;

import org.junit.jupiter.api.Test;
import org.opensearch.supervisor.model.node.ShardRole;
import org.opensearch.supervisor.model.observe.Observation;
import org.opensearch.supervisor.model.status.Replication;

import java.time.Instant;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.opensearch.supervisor.model.plan.PrimaryTermFixture.A;
import static org.opensearch.supervisor.model.plan.PrimaryTermFixture.ACME;
import static org.opensearch.supervisor.model.plan.PrimaryTermFixture.B;
import static org.opensearch.supervisor.model.plan.PrimaryTermFixture.Node;
import static org.opensearch.supervisor.model.plan.PrimaryTermFixture.POOL;
import static org.opensearch.supervisor.model.plan.PrimaryTermFixture.UUID;
import static org.opensearch.supervisor.model.plan.PrimaryTermFixture.counter;
import static org.opensearch.supervisor.model.plan.PrimaryTermFixture.index;
import static org.opensearch.supervisor.model.plan.PrimaryTermFixture.observe;
import static org.opensearch.supervisor.model.plan.PrimaryTermFixture.unreadableCounter;

class TermLedgerTests {

    private static Plan planPlacing(ShardRole role, int... shards) {
        List<Instruction> instructions = new java.util.ArrayList<>();
        for (int shard : shards) {
            instructions.add(new Instruction.PlaceCopy(ACME, UUID, "my-index", shard, role, A, 3041, "test"));
        }
        return new Plan(5, Instant.EPOCH, POOL, 1, instructions, false, Map.of());
    }

    private static Plan nothing() {
        return Plan.empty(5, Instant.EPOCH, POOL, 1);
    }

    @Test
    void anAbsentCounterIsCreatedAtOneForEveryShard() {
        // Created with the index, so that "absent" for an index already seen can never be read as a default.
        TermLedger.Resolution resolution = TermLedger.resolve(nothing(), observe(index(3, 0, Replication.SEGMENT), Map.of()));

        assertEquals(1, resolution.writes().size());
        TermLedger.Write write = resolution.writes().get(0);
        assertEquals(Map.of(0, 1L, 1, 1L, 2, 1L), write.next().shards());
        assertEquals(0L, write.expectedRevision(), "revision 0 is create-if-absent");
    }

    @Test
    void placingAPrimaryAdvancesExactlyThatShardByOne() {
        Observation observation = observe(index(3, 0, Replication.SEGMENT), counter(Map.of(0, 4L, 1, 2L, 2, 7L), 11));

        TermLedger.Resolution resolution = TermLedger.resolve(planPlacing(ShardRole.PRIMARY, 1), observation);

        assertEquals(Map.of(0, 4L, 1, 3L, 2, 7L), resolution.termsOf(ACME, UUID).orElseThrow().shards());
        assertEquals(1, resolution.writes().size());
        assertEquals(11L, resolution.writes().get(0).expectedRevision(), "compare-and-set on what was observed");
    }

    @Test
    void placingAReplicaAdvancesNothing() {
        // "Goes up on promotion and not otherwise": a term that advanced for unrelated reasons would make every pass
        // look like a promotion, and a started primary asserts its term never changes.
        Observation observation = observe(index(1, 1, Replication.SEGMENT), counter(Map.of(0, 4L), 11));

        TermLedger.Resolution resolution = TermLedger.resolve(planPlacing(ShardRole.SEARCH_REPLICA, 0), observation);

        assertEquals(4L, resolution.termsOf(ACME, UUID).orElseThrow().termOf(0));
        assertTrue(resolution.writes().isEmpty(), "nothing changed, so nothing is written: " + resolution.writes());
    }

    @Test
    void twoInstructionsForOneShardAdvanceItOnce() {
        Observation observation = observe(index(1, 0, Replication.SEGMENT), counter(Map.of(0, 4L), 11));

        TermLedger.Resolution resolution = TermLedger.resolve(planPlacing(ShardRole.PRIMARY, 0, 0), observation);

        assertEquals(5L, resolution.termsOf(ACME, UUID).orElseThrow().termOf(0));
    }

    @Test
    void aLostCounterIsRebuiltAboveEveryTermAlreadyInCirculation() {
        // The reuse finding 10 warned about, one level up. A node -- lost ones included, since a node that stopped
        // heartbeating may still be running -- was told term 6, so the rebuilt counter must not issue 6 again.
        Observation observation = observe(index(1, 1, Replication.DOCUMENT), Map.of(),
            Node.of(A).told(0, ShardRole.PRIMARY, 6).lost(),
            Node.of(B).told(0, ShardRole.REPLICA, 6));

        TermLedger.Resolution rebuilt = TermLedger.resolve(nothing(), observation);
        assertEquals(6L, rebuilt.termsOf(ACME, UUID).orElseThrow().termOf(0));

        TermLedger.Resolution promoting = TermLedger.resolve(planPlacing(ShardRole.PRIMARY, 0), observation);
        assertEquals(7L, promoting.termsOf(ACME, UUID).orElseThrow().termOf(0),
            "the next primary must be told a term nobody has been told");
    }

    @Test
    void aCounterBehindWhatNodesWereToldIsSkippedForwardRatherThanTrusted() {
        Observation observation = observe(index(1, 0, Replication.DOCUMENT), counter(Map.of(0, 2L), 11),
            Node.of(A).told(0, ShardRole.PRIMARY, 5));

        TermLedger.Resolution resolution = TermLedger.resolve(nothing(), observation);

        assertEquals(5L, resolution.termsOf(ACME, UUID).orElseThrow().termOf(0));
        assertEquals(1, resolution.writes().size(), "and the counter is corrected in the store");
    }

    @Test
    void anUnreadableCounterIsNeitherRebuiltNorWritten() {
        // Unreadable is not absent. Rebuilding it would overwrite the only record of which terms have been issued on
        // the strength of a parse failure.
        Observation observation = observe(index(1, 0, Replication.SEGMENT), unreadableCounter(),
            Node.of(A).told(0, ShardRole.PRIMARY, 3));

        TermLedger.Resolution resolution = TermLedger.resolve(nothing(), observation);

        assertTrue(resolution.termsOf(ACME, UUID).isEmpty());
        assertTrue(resolution.writes().isEmpty());
        assertEquals(3L, resolution.floorOf(ACME, UUID, 0), "but what nodes were told is still known");
    }

    @Test
    void aSettledCounterIsNotRewritten() {
        Observation observation = observe(index(2, 0, Replication.SEGMENT), counter(Map.of(0, 1L, 1, 1L), 11));

        assertTrue(TermLedger.resolve(nothing(), observation).writes().isEmpty());
    }
}
