package org.opensearch.supervisor.model.plan;

import org.junit.jupiter.api.Test;
import org.opensearch.supervisor.model.diff.Diff;
import org.opensearch.supervisor.model.diff.Finding;
import org.opensearch.supervisor.model.diff.FindingKind;
import org.opensearch.supervisor.model.node.ShardRole;
import org.opensearch.supervisor.model.observe.Observation;
import org.opensearch.supervisor.model.status.Replication;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.opensearch.supervisor.model.plan.PrimaryTermFixture.A;
import static org.opensearch.supervisor.model.plan.PrimaryTermFixture.B;
import static org.opensearch.supervisor.model.plan.PrimaryTermFixture.C;
import static org.opensearch.supervisor.model.plan.PrimaryTermFixture.Node;
import static org.opensearch.supervisor.model.plan.PrimaryTermFixture.counter;
import static org.opensearch.supervisor.model.plan.PrimaryTermFixture.diffAfter;
import static org.opensearch.supervisor.model.plan.PrimaryTermFixture.index;
import static org.opensearch.supervisor.model.plan.PrimaryTermFixture.observe;
import static org.opensearch.supervisor.model.plan.PrimaryTermFixture.unreadableCounter;

class PromotionTests {

    private static Plan plan(Observation observation) {
        Diff diff = diffAfter(3, observation);
        assertEquals(1, diff.of(FindingKind.UNASSIGNED_PRIMARY).size(), "precondition: the primary is gone");
        return Planner.plan(observation, diff, 7, Planner.Policy.defaults());
    }

    private static String refusalFor(Plan plan, Observation observation) {
        Finding primary = diffAfter(3, observation).of(FindingKind.UNASSIGNED_PRIMARY).get(0);
        return plan.refusalFor(primary);
    }

    @Test
    void aLostDocumentReplicationPrimaryIsReplacedByPromotingItsReplica() {
        // Finding 11. The replica is the data, so it becomes the primary where it stands -- not a fresh primary on
        // the empty node C, which is what the allocator would have chosen and what the replica would then have
        // recovered from, discarding everything it held.
        Observation observation = observe(index(1, 1, Replication.DOCUMENT), counter(Map.of(0, 1L), 3),
            Node.of(A).told(0, ShardRole.PRIMARY, 1).lost(),
            Node.of(B).holding(0, ShardRole.REPLICA).told(0, ShardRole.REPLICA, 1).declaringPrimaryTerms(),
            Node.of(C).declaringPrimaryTerms());

        Plan plan = plan(observation);

        assertEquals(1, plan.instructions().size(), plan.instructions().toString());
        Instruction.PlaceCopy place = (Instruction.PlaceCopy) plan.instructions().get(0);
        assertEquals(B, place.node(), "the replica's node, not the empty one");
        assertEquals(ShardRole.PRIMARY, place.role());
        assertTrue(place.because().contains("promoting the started replica"), place.because());

        // And the term goes up with it, because promotion in place is unreachable at an unchanged term.
        assertEquals(2L, TermLedger.resolve(plan, observation).termsOf(PrimaryTermFixture.ACME, PrimaryTermFixture.UUID)
            .orElseThrow().termOf(0));
    }

    @Test
    void aReplicaOnANodeThatCannotBeToldATermIsNotWipedInsteadOfPromoted() {
        // The silent data loss this closes. Node B cannot honour a term, so it cannot be promoted safely -- and the
        // old behaviour would have placed an empty primary on C, from which B's replica would recover. A shard that
        // cannot take writes is recoverable; a wiped replica is not. So nothing is placed, and the reason says so.
        Observation observation = observe(index(1, 1, Replication.DOCUMENT), counter(Map.of(0, 1L), 3),
            Node.of(A).told(0, ShardRole.PRIMARY, 1).lost(),
            Node.of(B).holding(0, ShardRole.REPLICA).told(0, ShardRole.REPLICA, 1),
            Node.of(C).declaringPrimaryTerms());

        Plan plan = plan(observation);

        assertTrue(plan.instructions().isEmpty(), "no empty primary on C: " + plan.instructions());
        String refusal = refusalFor(plan, observation);
        assertTrue(refusal.contains("primary_terms"), refusal);
        assertTrue(refusal.contains("discard its data"), refusal);
    }

    @Test
    void theChoiceAmongReplicasIsDeterministic() {
        // Two supervisors briefly both believing they own this pool must promote the same copy, or between them
        // they make two primaries.
        Observation observation = observe(index(1, 2, Replication.DOCUMENT), counter(Map.of(0, 1L), 3),
            Node.of(A).told(0, ShardRole.PRIMARY, 1).lost(),
            Node.of(C).holding(0, ShardRole.REPLICA).declaringPrimaryTerms(),
            Node.of(B).holding(0, ShardRole.REPLICA).declaringPrimaryTerms());

        for (int attempt = 0; attempt < 10; attempt++) {
            assertEquals(B, ((Instruction.PlaceCopy) plan(observation).instructions().get(0)).node());
        }
    }

    @Test
    void aSegmentReplicationPrimaryIsStillReplacedFromTheRemoteStore() {
        // Its read-only copies can never be promoted, and a fresh primary restores from the remote store, so the
        // allocator's answer is right there and nothing about it changes -- except that it gets a new term.
        Observation observation = observe(index(1, 1, Replication.SEGMENT), counter(Map.of(0, 1L), 3),
            Node.of(A).told(0, ShardRole.PRIMARY, 1).lost(),
            Node.of(B).holding(0, ShardRole.SEARCH_REPLICA),
            Node.of(C));

        Plan plan = plan(observation);

        assertEquals(1, plan.instructions().size(), plan.instructions().toString());
        Instruction.PlaceCopy place = (Instruction.PlaceCopy) plan.instructions().get(0);
        assertEquals(C, place.node());
        assertEquals(ShardRole.PRIMARY, place.role());
    }

    @Test
    void anUnreadableCounterStopsAnyPrimaryBeingPlacedForItsIndex() {
        Observation observation = observe(index(1, 0, Replication.SEGMENT), unreadableCounter(),
            Node.of(A).told(0, ShardRole.PRIMARY, 3).lost(),
            Node.of(C));

        Plan plan = plan(observation);

        assertTrue(plan.instructions().isEmpty(), plan.instructions().toString());
        String refusal = refusalFor(plan, observation);
        assertTrue(refusal.contains("cannot be read"), refusal);
        assertTrue(refusal.contains("supervisor/terms/acme/"), "and it names the key to look at: " + refusal);
    }

    @Test
    void aPromotionInFlightIsNotRepeatedEveryPass() {
        // B has been told PRIMARY and still reports REPLICA, because what a node holds beats what it was told. The
        // shard still looks primary-less; promoting again would advance the term every pass until B caught up.
        Observation observation = observe(index(1, 1, Replication.DOCUMENT), counter(Map.of(0, 2L), 3),
            Node.of(A).told(0, ShardRole.PRIMARY, 1).lost(),
            Node.of(B).holding(0, ShardRole.REPLICA).told(0, ShardRole.PRIMARY, 2).declaringPrimaryTerms(),
            Node.of(C).declaringPrimaryTerms());

        Plan plan = plan(observation);

        assertTrue(plan.instructions().isEmpty(), "nothing re-promoted and nothing placed on C: " + plan.instructions());
        assertTrue(refusalFor(plan, observation).contains("has not reported it yet"), refusalFor(plan, observation));
        assertTrue(TermLedger.resolve(plan, observation).writes().isEmpty(), "and the term does not move");
    }

    @Test
    void aReplicaOnAStaleNodeIsNotWipedByAnEmptyPrimaryOnALiveOne() {
        // B's copies still count through the stale window, so the shard looks primary-less while B holds its data.
        // Searching only live nodes found no replica, and the allocator put an empty primary on C, which B's replica
        // would recover from when B came back. B cannot be promoted while it is not reporting, so the shard waits.
        Observation observation = observe(index(1, 1, Replication.DOCUMENT), counter(Map.of(0, 1L), 3),
            Node.of(A).told(0, ShardRole.PRIMARY, 1).lost(),
            Node.of(B).holding(0, ShardRole.REPLICA).told(0, ShardRole.REPLICA, 1).declaringPrimaryTerms().stale(),
            Node.of(C).declaringPrimaryTerms());

        Plan plan = plan(observation);

        assertTrue(plan.instructions().isEmpty(), "no empty primary on C: " + plan.instructions());
        String refusal = refusalFor(plan, observation);
        assertTrue(refusal.contains("not live"), refusal);
        assertTrue(refusal.contains("discard its data"), refusal);
    }

    @Test
    void aPromotionToANodeThatHasSinceGoneStaleIsNotRepeatedElsewhere() {
        // B was told PRIMARY and went quiet before reporting it. It may still apply it, so promoting C as well would
        // make two primaries.
        Observation observation = observe(index(1, 2, Replication.DOCUMENT), counter(Map.of(0, 2L), 3),
            Node.of(A).told(0, ShardRole.PRIMARY, 1).lost(),
            Node.of(B).holding(0, ShardRole.REPLICA).told(0, ShardRole.PRIMARY, 2).declaringPrimaryTerms().stale(),
            Node.of(C).holding(0, ShardRole.REPLICA).told(0, ShardRole.REPLICA, 2).declaringPrimaryTerms());

        Plan plan = plan(observation);

        assertTrue(plan.instructions().isEmpty(), "C is not promoted too: " + plan.instructions());
        assertTrue(refusalFor(plan, observation).contains("has not reported it yet"), refusalFor(plan, observation));
    }

    private static final org.opensearch.supervisor.model.NodeId D = org.opensearch.supervisor.model.NodeId.of(
        "nodeDDDDDDDDDDDDDDDDDD");

    @Test
    void onlyAReplicaTheLostPrimaryLastCalledInSyncIsPromoted() {
        // Finding 12. B sorts first and would have been chosen, but A last reported only C in sync: B may be missing
        // acknowledged writes, and promoting it would lose them silently.
        Observation observation = observe(index(1, 2, Replication.DOCUMENT), counter(Map.of(0, 1L), 3),
            Node.of(A).holdingDocrep(0, ShardRole.PRIMARY, "alloc-a", "alloc-a", "alloc-c")
                .told(0, ShardRole.PRIMARY, 1).lost(),
            Node.of(B).holdingDocrep(0, ShardRole.REPLICA, "alloc-b").declaringPrimaryTerms(),
            Node.of(C).holdingDocrep(0, ShardRole.REPLICA, "alloc-c").declaringPrimaryTerms());

        Plan plan = plan(observation);

        assertEquals(1, plan.instructions().size(), plan.instructions().toString());
        assertEquals(C, ((Instruction.PlaceCopy) plan.instructions().get(0)).node());
    }

    @Test
    void withNoInSyncReplicaTheShardIsReportedRatherThanPromotedOrReplaced() {
        // Neither promoting B nor an empty primary on D: the second would have B recover from it and discard its
        // data, which is at least as bad as losing the writes B is missing.
        Observation observation = observe(index(1, 1, Replication.DOCUMENT), counter(Map.of(0, 1L), 3),
            Node.of(A).holdingDocrep(0, ShardRole.PRIMARY, "alloc-a", "alloc-a").told(0, ShardRole.PRIMARY, 1).lost(),
            Node.of(B).holdingDocrep(0, ShardRole.REPLICA, "alloc-b").declaringPrimaryTerms(),
            Node.of(D).declaringPrimaryTerms());

        Plan plan = plan(observation);

        assertTrue(plan.instructions().isEmpty(), "nothing promoted and nothing placed: " + plan.instructions());
        String refusal = refusalFor(plan, observation);
        assertTrue(refusal.contains("acknowledged writes"), refusal);
        assertTrue(refusal.contains("[alloc-a]"), "and it names the set it went by: " + refusal);
    }

    @Test
    void theSetComesFromTheLostPrimaryWithTheHighestTerm() {
        // A was primary at term 1 and lost, B was promoted at term 2 and lost in turn. A's inventory is still there
        // and its set is out of date; intersecting the two would exclude C, which B last called in sync.
        Observation observation = observe(index(1, 2, Replication.DOCUMENT), counter(Map.of(0, 2L), 3),
            Node.of(A).holdingDocrep(0, ShardRole.PRIMARY, "alloc-a", "alloc-a", "alloc-b")
                .told(0, ShardRole.PRIMARY, 1).lost(),
            Node.of(B).holdingDocrep(0, ShardRole.PRIMARY, "alloc-b", "alloc-b", "alloc-c")
                .told(0, ShardRole.PRIMARY, 2).lost(),
            Node.of(C).holdingDocrep(0, ShardRole.REPLICA, "alloc-c").told(0, ShardRole.REPLICA, 2)
                .declaringPrimaryTerms());

        Plan plan = plan(observation);

        assertEquals(1, plan.instructions().size(), refusalFor(plan, observation));
        assertEquals(C, ((Instruction.PlaceCopy) plan.instructions().get(0)).node());
    }

    @Test
    void aLostPrimaryThatNeverPublishedASetLeavesAnyStartedReplicaPromotable() {
        // A node from before finding 12: today's behaviour, not a refusal.
        Observation observation = observe(index(1, 1, Replication.DOCUMENT), counter(Map.of(0, 1L), 3),
            Node.of(A).holding(0, ShardRole.PRIMARY).told(0, ShardRole.PRIMARY, 1).lost(),
            Node.of(B).holdingDocrep(0, ShardRole.REPLICA, "alloc-b").declaringPrimaryTerms());

        assertEquals(B, ((Instruction.PlaceCopy) plan(observation).instructions().get(0)).node());
    }
}
