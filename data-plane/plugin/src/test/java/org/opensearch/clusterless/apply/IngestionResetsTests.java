/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.clusterless.apply;

import org.opensearch.Version;
import org.opensearch.cluster.ClusterName;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.IngestionStatus;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.clusterless.contract.Assignment;
import org.opensearch.clusterless.contract.IndexConfig;
import org.opensearch.clusterless.contract.IngestionReset;
import org.opensearch.clusterless.contract.NodeRole;
import org.opensearch.clusterless.contract.ShardRole;
import org.opensearch.clusterless.contract.TenantId;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.test.OpenSearchTestCase;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Carrying out a consumer-pointer reset, which is the only instruction this plugin acts on.
 *
 * <p>The rules being checked are the three the contract puts on this side, and each has a plausible wrong answer
 * that would look like working code: applying a reset without pausing first (works only when the reset happens to
 * arrive before the resume), keying idempotence on the assignment version (re-applies whenever anything else in
 * the document moves), and treating a reset it cannot carry out as nothing to report (tells the supervisor a
 * pointer moved when nothing moved it).
 */
public class IngestionResetsTests extends OpenSearchTestCase {

    private static final TenantId ACME = TenantId.of("acme");
    private static final String UUID = "uzKxLWZu4nPSrTknEcDpoA";
    private static final String INDEX = "acme.my-index-" + UUID;

    /** Records what it was asked to do, in order, and can be told to fail or to be unready. */
    private static final class FakePollers implements IngestionResets.Pollers {

        private final List<String> calls = new ArrayList<>();
        private Readiness readiness = Readiness.READY;
        private Exception failWith;

        @Override
        public Readiness readinessOf(ShardId shard) {
            return readiness;
        }

        @Override
        public void pauseThenReset(ShardId shard, IngestionReset.Mode mode, String value, boolean landPaused) throws Exception {
            calls.add(shard.id() + ":" + mode + ":" + value + ":landPaused=" + landPaused);
            if (failWith != null) {
                throw failWith;
            }
        }
    }

    // ---------------------------------------------------------------- the ordering, which is the whole point

    public void testAResetIsCarriedOutEvenWhenTheDocumentSaysTheStreamShouldBeRunning() {
        // The index document says running -- the resume arrived several passes ago, because desired state travels
        // faster than an instruction. The reset must still be carried out. An implementation that only reset
        // paused indices, reasoning that a reset needs a paused poller, would do nothing at all here.
        FakePollers pollers = new FakePollers();
        IngestionResets resets = new IngestionResets(pollers);

        IngestionResets.Outcome outcome = resets.apply(assignmentWithReset("task-1", 0, "offset", "1000"), desiredWith(false), stateWith(false));

        assertEquals("the reset must be attempted, not skipped", 1, pollers.calls.size());
        assertTrue("nothing should be left over", outcome.isSettled());
    }

    public void testTheStateThePollerLandsOnTracksTheDocumentInBothDirections() {
        // Asserted in one test over both values, deliberately: either half alone passes against an implementation
        // that hardcodes the other, which is exactly what reverting the rule showed. Only the pair pins it.
        FakePollers running = new FakePollers();
        new IngestionResets(running).apply(assignmentWithReset("task-1", 0, "offset", "1000"), desiredWith(false), stateWith(false));

        FakePollers paused = new FakePollers();
        new IngestionResets(paused).apply(assignmentWithReset("task-1", 0, "offset", "1000"), desiredWith(true), stateWith(true));

        assertEquals(List.of("0:OFFSET:1000:landPaused=false"), running.calls);
        assertEquals(List.of("0:OFFSET:1000:landPaused=true"), paused.calls);
    }

    public void testTheLandingStateComesFromTheDocumentAndNotFromTheReset() {
        // Stated separately because it is the assertion that fails if anyone "simplifies" the two calls into one.
        // Upstream applies a reset before its pause flag within one call, so a single call could never both pause
        // first and land on the document's state. Checked on a paused index, so it cannot pass against a
        // hardcoded false.
        FakePollers pollers = new FakePollers();
        IngestionResets resets = new IngestionResets(pollers);
        IngestionResets.Outcome planned = resets.plan(assignmentWithReset("task-1", 0, "timestamp", "1788969977933"), desiredWith(true), stateWith(true));

        assertEquals(1, planned.actions().size());
        IngestionResets.Action action = planned.actions().get(0);
        assertEquals(IngestionReset.Mode.TIMESTAMP, action.mode());
        assertEquals("1788969977933", action.value());
        assertTrue("the document says paused, so that is where the poller has to end up", action.landPaused());
    }

    // ---------------------------------------------------------------- idempotence, keyed on the id

    public void testTheSameResetIsNotAppliedTwice() {
        FakePollers pollers = new FakePollers();
        IngestionResets resets = new IngestionResets(pollers);
        Assignment assignment = assignmentWithReset("task-1", 0, "offset", "1000");
        ClusterState state = stateWith(false);

        resets.apply(assignment, desiredWith(false), state);
        IngestionResets.Outcome second = resets.apply(assignment, desiredWith(false), state);

        assertEquals("the second pass must move nothing", 1, pollers.calls.size());
        assertTrue("and must still report settled, or the supervisor never completes the task", second.isSettled());
    }

    public void testTheResetSurvivesTheWholeHandOverAcknowledgeWithdrawCycle() {
        // The supervisor measured that cycle at about three of its passes, so the reset stays in the assignment
        // for about three passes after this node has carried it out -- it is withdrawn only once the
        // acknowledgement has been observed. Two passes is therefore not enough of a test: every pass in that
        // window re-reads an assignment still asking for a reset that has already happened, and every one of them
        // must decline to do it again while still reporting settled, or the ack that triggers the withdrawal never
        // comes and the reset is re-applied for ever.
        FakePollers pollers = new FakePollers();
        IngestionResets resets = new IngestionResets(pollers);
        Assignment stillAsking = assignmentWithReset("task-1", 0, "offset", "1000");
        ClusterState state = stateWith(false);

        for (int pass = 1; pass <= 4; pass++) {
            IngestionResets.Outcome outcome = resets.apply(stillAsking, desiredWith(false), state);
            assertTrue("pass " + pass + " must report settled", outcome.isSettled());
            assertEquals("after pass " + pass, 1, pollers.calls.size());
        }

        // And the withdrawal itself is a non-event: the instruction simply stops being there.
        Assignment withdrawn = new Assignment(
            11,
            0,
            Map.of(ACME, new Assignment.TenantAssignment(Set.of(NodeRole.DATA), Map.of(UUID, new Assignment.IndexAssignment(
                "my-index",
                3041,
                Map.of(0, ShardRole.PRIMARY)
            ))))
        );
        assertTrue(resets.apply(withdrawn, desiredWith(false), state).isSettled());
        assertEquals("withdrawing it must not be mistaken for a new instruction", 1, pollers.calls.size());
    }

    public void testARestartInsideTheAcknowledgementWindowReappliesTheReset() {
        // The documented hole, pinned so that nobody later reads the id-keyed memory as durable. A new
        // IngestionResets is a new process: the assignment still carries the reset, because the supervisor has not
        // observed an acknowledgement to withdraw it on, and this node no longer remembers having applied it.
        //
        // Left as it is rather than closed, and the reasoning is in MILESTONES.md: the replay re-consumes rather
        // than loses, since every ingested message carries an _id. The only durable place that would actually work
        // is the Lucene commit beside StreamPoller.BATCH_START, which is the engine's to write and not a plugin's,
        // and the pointer itself cannot be used to detect a reset -- a rewind looks like a finished job before it
        // is applied, which is the whole reason the acknowledgement exists.
        FakePollers pollers = new FakePollers();
        Assignment stillAsking = assignmentWithReset("task-1", 0, "offset", "1000");
        ClusterState state = stateWith(false);

        new IngestionResets(pollers).apply(stillAsking, desiredWith(false), state);
        assertEquals(1, pollers.calls.size());

        new IngestionResets(pollers).apply(stillAsking, desiredWith(false), state);
        assertEquals("a restarted node applies it again, and this is known rather than accidental", 2, pollers.calls.size());
    }

    public void testAnAssignmentVersionThatMovesForAnotherReasonDoesNotReapplyTheReset() {
        // The rewind-for-ever failure arriving by another route, and the reason the contract says to key on the
        // id. A shard placed elsewhere in the same document moves the version; nothing about this reset changed.
        FakePollers pollers = new FakePollers();
        IngestionResets resets = new IngestionResets(pollers);

        resets.apply(assignmentWithReset("task-1", 0, "offset", "1000"), desiredWith(false), stateWith(false));
        Assignment later = new Assignment(
            99,
            0,
            Map.of(ACME, new Assignment.TenantAssignment(Set.of(NodeRole.DATA), Map.of(UUID, new Assignment.IndexAssignment(
                "my-index",
                3041,
                Map.of(0, ShardRole.PRIMARY),
                new IngestionReset("task-1", Map.of(0, new IngestionReset.Pointer(IngestionReset.Mode.OFFSET, "1000")))
            ))))
        );
        resets.apply(later, desiredWith(false), stateWith(false));

        assertEquals("one call, across two versions of the same instruction", 1, pollers.calls.size());
    }

    public void testANewResetIdIsAppliedEvenOnAShardThatHasHadOne() {
        FakePollers pollers = new FakePollers();
        IngestionResets resets = new IngestionResets(pollers);

        resets.apply(assignmentWithReset("task-1", 0, "offset", "1000"), desiredWith(false), stateWith(false));
        resets.apply(assignmentWithReset("task-2", 0, "offset", "2000"), desiredWith(false), stateWith(false));

        assertEquals(List.of("0:OFFSET:1000:landPaused=false", "0:OFFSET:2000:landPaused=false"), pollers.calls);
    }

    // ---------------------------------------------------------------- what holds the ack open, and what fails it

    public void testAShardThatIsNotStartedYetIsOutstandingRatherThanRefused() {
        // The ordinary first pass: the state carrying the shard was applied moments ago and the engine is not up.
        // Nothing is wrong, so this must not be a failure -- but it must not be silence either, because the ack
        // would then say applied and the supervisor would complete the task.
        FakePollers pollers = new FakePollers();
        pollers.readiness = IngestionResets.Pollers.Readiness.NOT_YET;

        IngestionResets.Outcome outcome = new IngestionResets(pollers).apply(assignmentWithReset("task-1", 0, "offset", "1000"), desiredWith(false), stateWith(false));

        assertTrue(pollers.calls.isEmpty());
        assertEquals(1, outcome.outstanding().size());
        assertTrue(outcome.refused().isEmpty());
        assertFalse(outcome.isSettled());
    }

    public void testAnIndexNotInThisNodesStateYetIsOutstanding() {
        IngestionResets.Outcome outcome = new IngestionResets(new FakePollers())
            .apply(assignmentWithReset("task-1", 0, "offset", "1000"), desiredWith(false), ClusterState.EMPTY_STATE);

        assertEquals(1, outcome.outstanding().size());
        assertTrue(outcome.outstanding().get(0), outcome.outstanding().get(0).contains("not in this node's state yet"));
    }

    public void testAnUnknownModeIsRefusedAndReportedRatherThanSkipped() {
        // The contract's own addition, and the reasoning is worth keeping: an unknown shard role means a copy is
        // not built, which the supervisor's differ sees for itself. Nothing sees an unapplied reset except this
        // node saying so.
        FakePollers pollers = new FakePollers();
        IngestionResets.Outcome outcome = new IngestionResets(pollers)
            .apply(assignmentWithReset("task-1", 0, "by_sequence_number", "1000"), desiredWith(false), stateWith(false));

        assertTrue("nothing may be guessed at", pollers.calls.isEmpty());
        assertEquals(1, outcome.refused().size());
        assertTrue(outcome.refused().get(0), outcome.refused().get(0).contains("not one this node understands"));
    }

    public void testTheLandingStateComesFromTheDocumentAndNotFromTheAppliedState() {
        // Found on a running node, from this plugin's own log line saying it had "left ingestion paused" on a
        // resume. Applying a cluster state is asynchronous -- ClusterApplierServiceStateApplier queues it and
        // returns -- so on the pass an operator's resume arrives, the applied state still says paused while the
        // index document says running. Here they disagree exactly that way, and the document has to win.
        //
        // Getting it wrong is not self-correcting, which is what makes it worth its own test: the reset leaves
        // the poller paused, the next pass fabricates an identical state, StateEquivalence sees no change, so
        // nothing is published and updateShardIngestionState is never called again. The stream stays stopped for
        // ever, by the instruction that was supposed to restart it.
        FakePollers pollers = new FakePollers();
        ClusterState stillSaysPaused = stateWith(true);
        Assignment resumeWithReset = assignmentWithReset("task-1", 0, "offset", "2");

        new IngestionResets(pollers).apply(resumeWithReset, desiredWith(false), stillSaysPaused);

        assertEquals(List.of("0:OFFSET:2:landPaused=false"), pollers.calls);
    }

    public void testAResetForAnIndexThisPassDidNotResolveWaits() {
        // A copy the node declined, or an index document that has not landed yet. Outstanding rather than
        // refused, because the usual cause is the torn read the contract promises and the next pass fixes.
        FakePollers pollers = new FakePollers();
        IngestionResets.Outcome outcome = new IngestionResets(pollers)
            .apply(assignmentWithReset("task-1", 0, "offset", "1000"), DesiredState.nothing(), stateWith(false));

        assertTrue(pollers.calls.isEmpty());
        assertEquals(1, outcome.outstanding().size());
        assertTrue(outcome.outstanding().get(0), outcome.outstanding().get(0).contains("not resolved that index"));
    }

    public void testAResetForAShardThisNodeIsNotThePrimaryOfIsRefused() {
        // The supervisor promises to write a reset only into the primary holder's assignment. Checked rather than
        // assumed: a search replica has no consumer of its own, so applying here would report a pointer moved on
        // a shard whose primary never heard of it.
        Assignment assignment = new Assignment(
            7,
            0,
            Map.of(ACME, new Assignment.TenantAssignment(Set.of(NodeRole.DATA), Map.of(UUID, new Assignment.IndexAssignment(
                "my-index",
                3041,
                Map.of(0, ShardRole.SEARCH_REPLICA),
                new IngestionReset("task-1", Map.of(0, new IngestionReset.Pointer(IngestionReset.Mode.OFFSET, "1000")))
            ))))
        );

        FakePollers pollers = new FakePollers();
        IngestionResets.Outcome outcome = new IngestionResets(pollers).apply(assignment, desiredWith(false), stateWith(false));

        assertTrue(pollers.calls.isEmpty());
        assertEquals(1, outcome.refused().size());
        assertTrue(outcome.refused().get(0), outcome.refused().get(0).contains("not assigned that shard's primary"));
    }

    public void testAnIndexWithNoIngestionSourceIsRefused() {
        FakePollers pollers = new FakePollers();
        pollers.readiness = IngestionResets.Pollers.Readiness.NOT_INGESTING;

        IngestionResets.Outcome outcome = new IngestionResets(pollers).apply(assignmentWithReset("task-1", 0, "offset", "1000"), desiredWith(false), stateWith(false));

        assertEquals(1, outcome.refused().size());
        assertTrue(outcome.refused().get(0), outcome.refused().get(0).contains("no ingestion source"));
    }

    public void testAFailureIsRetriedRatherThanRememberedAsDone() {
        // The safe direction. An unapplied reset holds the supervisor's task open; a wrongly recorded one lets it
        // complete having moved nothing, which is the one outcome nothing downstream can contradict.
        FakePollers pollers = new FakePollers();
        pollers.failWith = new IllegalStateException("Cannot reset consumer when poller is not paused");
        IngestionResets resets = new IngestionResets(pollers);
        Assignment assignment = assignmentWithReset("task-1", 0, "offset", "1000");

        IngestionResets.Outcome first = resets.apply(assignment, desiredWith(false), stateWith(false));
        assertFalse(first.isSettled());
        assertEquals(1, first.outstanding().size());
        assertTrue(first.outstanding().get(0), first.outstanding().get(0).contains("Cannot reset consumer"));

        pollers.failWith = null;
        IngestionResets.Outcome second = resets.apply(assignment, desiredWith(false), stateWith(false));
        assertEquals("it must have been tried again", 2, pollers.calls.size());
        assertTrue(second.isSettled());
    }

    public void testAnAssignmentWithNoResetDoesNothingAtAll() {
        FakePollers pollers = new FakePollers();
        Assignment plain = new Assignment(
            7,
            0,
            Map.of(ACME, new Assignment.TenantAssignment(Set.of(NodeRole.DATA), Map.of(UUID, new Assignment.IndexAssignment(
                "my-index",
                3041,
                Map.of(0, ShardRole.PRIMARY)
            ))))
        );

        IngestionResets.Outcome outcome = new IngestionResets(pollers).apply(plain, desiredWith(false), stateWith(false));

        assertTrue(pollers.calls.isEmpty());
        assertTrue(outcome.isSettled());
        assertSame("the common case should allocate nothing", IngestionResets.Outcome.NOTHING, outcome);
    }

    public void testAResetOnARecreatedIndexIsNotSatisfiedByItsPredecessor() {
        // ShardId equality includes the index uuid, which is what keeps the applied-id memory from being the
        // torn-read problem in a new place: a name reused by a different index inherits nothing.
        FakePollers pollers = new FakePollers();
        IngestionResets resets = new IngestionResets(pollers);

        resets.apply(assignmentWithReset("task-1", 0, "offset", "1000"), desiredWith(false), stateWith(false));

        String recreated = "aDifferentIndexUuid12";
        Assignment sameNameNewIndex = new Assignment(
            8,
            0,
            Map.of(ACME, new Assignment.TenantAssignment(Set.of(NodeRole.DATA), Map.of(recreated, new Assignment.IndexAssignment(
                "my-index",
                3042,
                Map.of(0, ShardRole.PRIMARY),
                new IngestionReset("task-1", Map.of(0, new IngestionReset.Pointer(IngestionReset.Mode.OFFSET, "1000")))
            ))))
        );
        resets.apply(sameNameNewIndex, desiredWith(false, recreated), stateWith(false, recreated));

        assertEquals("the new index's shard has had no reset", 2, pollers.calls.size());
    }

    // ---------------------------------------------------------------- fixtures

    private static Assignment assignmentWithReset(String id, int shard, String mode, String value) {
        Map<Integer, IngestionReset.Pointer> shards = Map.of(shard, new IngestionReset.Pointer(IngestionReset.Mode.from(mode), value));
        return new Assignment(
            7,
            0,
            Map.of(ACME, new Assignment.TenantAssignment(Set.of(NodeRole.DATA), Map.of(UUID, new Assignment.IndexAssignment(
                "my-index",
                3041,
                Map.of(shard, ShardRole.PRIMARY),
                new IngestionReset(id, shards)
            ))))
        );
    }

    /** What this pass read from {@code tenant/<t>/indices/<name>}, which is the authority on the pause. */
    private static DesiredState desiredWith(boolean paused) {
        return desiredWith(paused, UUID);
    }

    private static DesiredState desiredWith(boolean paused, String uuid) {
        DesiredState.IndexKey key = new DesiredState.IndexKey(ACME, uuid);
        IndexConfig config = new IndexConfig(
            "my-index",
            uuid,
            1788969977933L,
            true,
            paused,
            Map.of("index.number_of_shards", "1", "index.number_of_replicas", "0", "index.ingestion_source.type", "FILE"),
            Map.of(),
            3041
        );
        return new DesiredState(
            7,
            Map.of(key, new DesiredState.DesiredIndex(key, config, Map.of(0, ShardRole.PRIMARY))),
            List.of(),
            List.of()
        );
    }

    private static ClusterState stateWith(boolean paused) {
        return stateWith(paused, UUID);
    }

    /** One index, one shard, with an ingestion source and the given pause state. */
    private static ClusterState stateWith(boolean paused, String uuid) {
        IndexMetadata index = IndexMetadata.builder(uuid.equals(UUID) ? INDEX : "acme.my-index-" + uuid)
            .settings(
                Settings.builder()
                    .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
                    .put(IndexMetadata.SETTING_INDEX_UUID, uuid)
                    .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                    .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                    .put("index.ingestion_source.type", "FILE")
            )
            .ingestionStatus(new IngestionStatus(paused))
            .build();
        return ClusterState.builder(new ClusterName("clusterless")).metadata(Metadata.builder().put(index, false)).build();
    }
}
