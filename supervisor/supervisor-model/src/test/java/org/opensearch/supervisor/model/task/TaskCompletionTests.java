package org.opensearch.supervisor.model.task;

import org.junit.jupiter.api.Test;
import org.opensearch.supervisor.model.NodeId;
import org.opensearch.supervisor.model.PoolId;
import org.opensearch.supervisor.model.TenantId;
import org.opensearch.supervisor.model.index.IndexConfig;
import org.opensearch.supervisor.model.node.Assignment;
import org.opensearch.supervisor.model.node.AppliedOutcome;
import org.opensearch.supervisor.model.node.Heartbeat;
import org.opensearch.supervisor.model.node.NodeRole;
import org.opensearch.supervisor.model.node.Registration;
import org.opensearch.supervisor.model.node.ShardInventory;
import org.opensearch.supervisor.model.node.ResetMode;
import org.opensearch.supervisor.model.node.ShardRole;
import org.opensearch.supervisor.model.node.ShardState;
import org.opensearch.supervisor.model.observe.Observation;
import org.opensearch.supervisor.model.status.Liveness;
import org.opensearch.supervisor.model.status.Replication;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * What each kind of task means, decided against an observation and nothing else.
 *
 * <p>Completion is a question about the world rather than about whether the supervisor remembers acting, which
 * is what makes a duplicated task harmless and a lost one irrelevant.
 */
class TaskCompletionTests {

    private static final PoolId POOL = PoolId.of("shared-a");
    private static final TenantId ACME = TenantId.of("acme");
    private static final String UUID = "uzKxLWZu4nPSrTknEcDpoA";
    private static final NodeId NODE = NodeId.of("nodeAAAAAAAAAAAAAAAAAA");

    private static TaskRecord task(TaskType type) {
        return new TaskRecord("task-1", type, "my-index", UUID, TaskState.PENDING,
            1000, 1, null, 0, 0, 0, null, 0, null);
    }

    private static TaskRecord resumeWithResets(TaskRecord.IngestionReset... resets) {
        return new TaskRecord("task-1", TaskType.RESUME_INGESTION, "my-index", UUID, TaskState.PENDING,
            1000, 1, null, 0, 0, 0, null, 0, new TaskRecord.Details(java.util.List.of(resets)));
    }

    private static IndexConfig index(boolean open, int shards, long configRevision) {
        return new IndexConfig("my-index", UUID, open, shards, 0, Replication.SEGMENT, false, org.opensearch.supervisor.model.settings.AllocationEnable.UNKNOWN, configRevision);
    }

    private static Registration registration() {
        return new Registration(NODE, "eph", "n", POOL, EnumSet.of(NodeRole.DATA),
            "10.0.0.1", 9300, "3.7.0", Map.of(), 5000, 1L);
    }

    private static Observation.NodeObservation holding(long configRevision, ShardState state) {
        ShardInventory inventory = new ShardInventory(NODE, "eph", 1L, Map.of(
            ACME, new ShardInventory.TenantShards(Map.of(
                UUID, new ShardInventory.IndexShards("my-index", configRevision, Map.of(
                    0, ShardInventory.ShardCopy.of(ShardRole.PRIMARY, state)
                ))
            ))
        ));
        return new Observation.NodeObservation(
            NODE, registration(), null, inventory, Liveness.LIVE, POOL, null, 0
        );
    }

    /**
     * A node holding the primary of shard {@code shard}, told to at assignment version {@code version}, and
     * reporting that it applied {@code appliedVersion} with {@code outcome}.
     */
    private static Observation.NodeObservation primaryReporting(
        NodeId node,
        int shard,
        long version,
        long appliedVersion,
        AppliedOutcome outcome
    ) {
        return primaryReporting(node, shard, version, appliedVersion, outcome, "task-1");
    }

    /**
     * @param resetId the id of the reset the observed assignment carries, or null for one carrying none. The
     *     acknowledgement asks whether the node applied a document containing <em>this</em> instruction, so the
     *     document has to contain it.
     */
    private static Observation.NodeObservation primaryReporting(
        NodeId node,
        int shard,
        long version,
        long appliedVersion,
        AppliedOutcome outcome,
        String resetId
    ) {
        ShardInventory inventory = new ShardInventory(node, "eph", 1L, Map.of(
            ACME, new ShardInventory.TenantShards(Map.of(
                UUID, new ShardInventory.IndexShards("my-index", 3041, Map.of(
                    shard, ShardInventory.ShardCopy.of(ShardRole.PRIMARY, ShardState.STARTED)
                ))
            ))
        ));
        Assignment.IngestionReset carried = resetId == null ? null : new Assignment.IngestionReset(
            resetId, Map.of(shard, new Assignment.ShardReset(ResetMode.OFFSET, "1000")));
        Assignment assignment = new Assignment(version, POOL, 1, node, 1L, Map.of(
            ACME, new Assignment.TenantAssignment(Set.of(NodeRole.DATA), Map.of(
                UUID, new Assignment.IndexAssignment("my-index", 3041, Map.of(shard, ShardRole.PRIMARY), carried)
            ))
        ));
        Heartbeat heartbeat = new Heartbeat(node, "eph", 1L,
            new Heartbeat.Applied(appliedVersion, outcome, null), null, 1,
            new Heartbeat.Capacity(1000, 900, 10, 10, 0.1));
        return new Observation.NodeObservation(
            node, new Registration(node, "eph", "n", POOL, EnumSet.of(NodeRole.DATA), "10.0.0.1", 9300, "3.7.0",
                Map.of(), 5000, 1L),
            heartbeat, inventory, Liveness.LIVE, POOL, assignment, 5
        );
    }

    private static TaskCompletion.Verdict evaluateResume(
        TaskRecord task,
        Map<NodeId, Observation.NodeObservation> nodes
    ) {
        Observation observation = new Observation(1, Instant.ofEpochMilli(5000), POOL,
            Map.of(ACME, new Observation.DesiredState(Map.of(UUID, index(true, 2, 3041)), List.of(task))),
            nodes, List.of());
        return TaskCompletion.evaluate(task, ACME, observation);
    }

    private static Observation.NodeObservation assignedOnly() {
        Assignment assignment = new Assignment(1, POOL, 1, NODE, 1L, Map.of(
            ACME, new Assignment.TenantAssignment(Set.of(NodeRole.DATA), Map.of(
                UUID, new Assignment.IndexAssignment("my-index", 3041, Map.of(0, ShardRole.PRIMARY))
            ))
        ));
        return new Observation.NodeObservation(
            NODE, registration(), null, null, Liveness.LIVE, POOL, assignment, 5
        );
    }

    private static Observation.NodeObservation empty() {
        return new Observation.NodeObservation(NODE, registration(), null, null, Liveness.LIVE, POOL, null, 0);
    }

    private static Observation observe(IndexConfig index, Observation.NodeObservation node) {
        Map<String, IndexConfig> indices = index == null ? Map.of() : Map.of(UUID, index);
        return new Observation(1, Instant.ofEpochMilli(5000), POOL,
            Map.of(ACME, new Observation.DesiredState(indices, List.of())),
            node == null ? Map.of() : Map.of(NODE, node), List.of());
    }

    private static TaskCompletion.Verdict evaluate(TaskType type, IndexConfig index, Observation.NodeObservation node) {
        return TaskCompletion.evaluate(task(type), ACME, observe(index, node));
    }

    // ---------------------------------------------------------------- create and open

    @Test
    void createIsDoneOnceEveryShardHasAServingPrimary() {
        var verdict = evaluate(TaskType.CREATE_INDEX, index(true, 1, 3041), holding(3041, ShardState.STARTED));
        assertTrue(verdict.satisfied(), verdict.because());
        assertTrue(verdict.because().contains("usable"), verdict.because());
    }

    @Test
    void createIsNotDoneWhileAShardIsMerelyAssigned() {
        // Convergence, not "the supervisor has done its part". An index whose shards are assigned but not yet
        // recovered is not usable, so the task is not done -- and a task that stays open while a large index
        // recovers is the queue being accurate rather than the queue being slow.
        var verdict = evaluate(TaskType.CREATE_INDEX, index(true, 1, 3041), assignedOnly());
        assertFalse(verdict.satisfied(), verdict.because());
    }

    @Test
    void createIsNotDoneWhileAPrimaryIsStillInitializing() {
        var verdict = evaluate(TaskType.CREATE_INDEX, index(true, 1, 3041), holding(3041, ShardState.INITIALIZING));
        assertFalse(verdict.satisfied(), verdict.because());
        assertTrue(verdict.because().contains("no started primary"), verdict.because());
    }

    @Test
    void createReportsHowFarItHasGotSoSlowCanBeToldFromStuck() {
        // Only the direction of this number is used, and it is what stops a large index failing its own
        // creation: a task still advancing renews its claim rather than spending an attempt.
        var partial = evaluate(TaskType.CREATE_INDEX, index(true, 3, 3041), holding(3041, ShardState.STARTED));
        assertFalse(partial.satisfied());
        assertEquals(1, partial.progress(), "one of three shards serving");
        assertTrue(partial.because().contains("shard 1"), partial.because());

        var none = evaluate(TaskType.CREATE_INDEX, index(true, 3, 3041), empty());
        assertEquals(0, none.progress());
    }

    @Test
    void createIsDoneWhenTheIndexHasSinceBeenDeleted() {
        // The recurring case, and the one worth being explicit about: work nobody wants any more is complete,
        // not failed. Failing it would leave a permanent record of something that never went wrong.
        var verdict = evaluate(TaskType.CREATE_INDEX, null, empty());
        assertTrue(verdict.satisfied());
        assertTrue(verdict.because().contains("unwanted"), verdict.because());
    }

    @Test
    void openIsDoneIfTheIndexIsClosedInDesiredState() {
        // The task says open, the index document says closed. The document is the intent; the task is a hint
        // about where to look, and this one is out of date.
        assertTrue(evaluate(TaskType.OPEN_INDEX, index(false, 1, 3041), empty()).satisfied());
    }

    // ---------------------------------------------------------------- delete and close

    @Test
    void deleteIsNotDoneWhileTheIndexIsStillInDesiredState() {
        var verdict = evaluate(TaskType.DELETE_INDEX, index(true, 1, 3041), empty());
        assertFalse(verdict.satisfied());
        assertTrue(verdict.because().contains("still in desired state"), verdict.because());
    }

    @Test
    void deleteIsDoneOnceTheDocumentIsGoneAndNoCopiesRemain() {
        assertTrue(evaluate(TaskType.DELETE_INDEX, null, empty()).satisfied());
    }

    @Test
    void deleteIsNotDoneWhileANodeStillHoldsACopy() {
        var verdict = evaluate(TaskType.DELETE_INDEX, null, holding(3041, ShardState.STARTED));
        assertFalse(verdict.satisfied());
        assertTrue(verdict.because().contains("still holds"), verdict.because());
    }

    @Test
    void deleteIsNotDoneWhileANodeIsStillAssignedAShardOtherThanZero() {
        // This asked about shard 0 and meant "any shard of this index", so an index whose only surviving entry
        // named shard 1 was read as assigned to nobody -- and the deletion reported success while a node was
        // still being told to hold it. Shard 1 rather than 0 is the entire point of the test.
        Assignment assignment = new Assignment(4, POOL, 1, NODE, 1L, Map.of(
            ACME, new Assignment.TenantAssignment(Set.of(NodeRole.DATA), Map.of(
                UUID, new Assignment.IndexAssignment("my-index", 3041, Map.of(1, ShardRole.PRIMARY))
            ))
        ));
        Observation.NodeObservation node = new Observation.NodeObservation(
            NODE, registration(), null, null, Liveness.LIVE, POOL, assignment, 0
        );

        var verdict = evaluate(TaskType.DELETE_INDEX, null, node);

        assertFalse(verdict.satisfied());
        assertTrue(verdict.because().contains("is assigned"), verdict.because());
    }

    @Test
    void closeIsDoneOnceTheIndexIsClosedAndItsCopiesAreGone() {
        assertFalse(evaluate(TaskType.CLOSE_INDEX, index(true, 1, 3041), empty()).satisfied());
        assertTrue(evaluate(TaskType.CLOSE_INDEX, index(false, 1, 3041), empty()).satisfied());
    }

    // ---------------------------------------------------------------- configuration updates

    @Test
    void anUpdateIsDoneOnceNoCopyReportsAnOlderConfigRevision() {
        assertFalse(evaluate(TaskType.UPDATE_MAPPING, index(true, 1, 3050), holding(3041, ShardState.STARTED)).satisfied());
        assertTrue(evaluate(TaskType.UPDATE_MAPPING, index(true, 1, 3050), holding(3050, ShardState.STARTED)).satisfied());
    }

    @Test
    void anUpdateIsDoneIfTheIndexHasGone() {
        assertTrue(evaluate(TaskType.UPDATE_SETTINGS, null, empty()).satisfied());
    }

    // ---------------------------------------------------------------- what a lost node does not block

    @Test
    void aLostNodesCopiesDoNotHoldADeleteOpen() {
        // Symmetric with the planner: a node given up for dead contributes nothing. Otherwise a delete task
        // would stay open for ever because a machine that is never coming back still claims to hold the index.
        Observation.NodeObservation lost = new Observation.NodeObservation(
            NODE, registration(), null, holding(3041, ShardState.STARTED).inventory(),
            Liveness.LOST, POOL, null, 0
        );
        assertTrue(evaluate(TaskType.DELETE_INDEX, null, lost).satisfied());
    }

    // ---------------------------------------------------------------- unknown types

    @Test
    void anUnrecognisedTypeIsNeitherDoneNorFailed() {
        // Completing it would report success for work this build cannot name; failing it would blame the task
        // for the supervisor's age.
        var verdict = evaluate(TaskType.UNKNOWN, index(true, 1, 3041), empty());
        assertFalse(verdict.satisfied());
        assertTrue(verdict.because().contains("not recognised"), verdict.because());
    }

    // ---------------------------------------------------------------- the record itself

    @Test
    void parsesTheRecordTheControlPlaneActuallyWrites() {
        // Copied from KEY_LAYOUT.md, which was generated by exercising a real control plane.
        String json = """
            {
              "task_id": "NmnqMpOCtk1I5aW_zVzSkA",
              "type": "create-index",
              "index": ".ds-logs-app-000001",
              "index_uuid": "JqyK2fDEK5RrEZWGo4SPDA",
              "state": "pending",
              "submitted_at": 1788969977936,
              "sequence": 2
            }
            """;
        TaskRecord record = TaskRecord.from(json.getBytes(StandardCharsets.UTF_8));

        assertEquals("NmnqMpOCtk1I5aW_zVzSkA", record.taskId());
        assertEquals(TaskType.CREATE_INDEX, record.type(), "hyphenated on the wire, underscored in Java");
        assertEquals(".ds-logs-app-000001", record.index());
        assertEquals(TaskState.PENDING, record.state());
        assertEquals(1788969977936L, record.submittedAt());
        assertEquals(0, record.attempts(), "the control plane writes no attempt count");
    }

    @Test
    void everyTypeTheControlPlaneWritesIsRecognised() {
        for (String type : List.of("create-index", "delete-index", "update-mapping",
            "update-settings", "close-index", "open-index")) {
            assertFalse(TaskType.from(type) == TaskType.UNKNOWN, type + " should be recognised");
            assertEquals(type, TaskType.from(type).wireName(), "wire name must round-trip");
        }
    }

    @Test
    void anUnknownTypeSurvivesReadingAndCannotBeWritten() {
        assertEquals(TaskType.UNKNOWN, TaskType.from("rollover"));
        org.junit.jupiter.api.Assertions.assertThrows(IllegalStateException.class, TaskType.UNKNOWN::wireName);
    }

    @Test
    void theStableOrderIsByTimeThenIdRatherThanBySequence() {
        // R4.5: sequence restarts per control plane process and submitted_at ties constantly, so neither is a
        // global ordering. This one is arbitrary but stable, which is all fairness needs.
        TaskRecord first = new TaskRecord("aaa", TaskType.CREATE_INDEX, "i", UUID, TaskState.PENDING,
            1000, 99, null, 0, 0, 0, null, 0, null);
        TaskRecord second = new TaskRecord("bbb", TaskType.CREATE_INDEX, "i", UUID, TaskState.PENDING,
            1000, 1, null, 0, 0, 0, null, 0, null);

        assertTrue(TaskRecord.STABLE_ORDER.compare(first, second) < 0,
            "same millisecond, so the id decides -- not the sequence");
    }

    // ---------------------------------------------------- pausing and resuming ingestion (control plane, new)

    @Test
    void pausingIsDoneWhenEveryCopyIsRunningTheCurrentConfiguration() {
        // Whether a stream consumes lives in the index document, so the change reaches a node exactly as a mapping
        // does. Same condition as UPDATE_SETTINGS, for the same reason -- and before this it parsed as UNKNOWN,
        // never completed, and turned into a permanent TASK_STUCK finding.
        var current = TaskCompletion.evaluate(task(TaskType.PAUSE_INGESTION), ACME,
            observe(index(true, 1, 3041), holding(3041, ShardState.STARTED)));
        assertTrue(current.satisfied(), current.because());

        var behind = TaskCompletion.evaluate(task(TaskType.PAUSE_INGESTION), ACME,
            observe(index(true, 1, 3041), holding(3040, ShardState.STARTED)));
        assertFalse(behind.satisfied(), "a copy still on the previous revision has not seen the pause");
    }

    @Test
    void resumingWithNoResetsIsJustAMetadataChange() {
        var verdict = TaskCompletion.evaluate(task(TaskType.RESUME_INGESTION), ACME,
            observe(index(true, 1, 3041), holding(3041, ShardState.STARTED)));

        assertTrue(verdict.satisfied(), verdict.because());
    }

    @Test
    void aPausedIndexWhoseUuidIsGoneIsDoneRatherThanPending() {
        // A consumer cannot be running for an index that does not exist. Same shape as every other task type: the
        // work is not outstanding, it is unwanted.
        var verdict = TaskCompletion.evaluate(task(TaskType.PAUSE_INGESTION), ACME,
            observe(null, holding(3041, ShardState.STARTED)));

        assertTrue(verdict.satisfied(), verdict.because());
    }

    @Test
    void aResetIsDoneOnlyWhenItsPrimaryHolderAcknowledgesTheVersionCarryingIt() {
        TaskRecord task = resumeWithResets(new TaskRecord.IngestionReset(0, ResetMode.OFFSET, "1000"));

        // Told at version 9, reporting 8: it has not read the document carrying the reset yet.
        var behind = evaluateResume(task,
            Map.of(NODE, primaryReporting(NODE, 0, 9, 8, AppliedOutcome.APPLIED)));
        assertFalse(behind.satisfied(), behind.because());
        assertTrue(behind.because().contains("assignment version 8"), behind.because());

        // Current version, but still working: the contract has a node hold `applying` while a reset is outstanding,
        // so a matching version alone would say only that it had read the document.
        var applying = evaluateResume(task,
            Map.of(NODE, primaryReporting(NODE, 0, 9, 9, AppliedOutcome.APPLYING)));
        assertFalse(applying.satisfied(), applying.because());

        // Refused. Never acknowledged, so the task stays open and visible rather than completing on a lie.
        var failed = evaluateResume(task,
            Map.of(NODE, primaryReporting(NODE, 0, 9, 9, AppliedOutcome.FAILED)));
        assertFalse(failed.satisfied(), failed.because());

        var done = evaluateResume(task,
            Map.of(NODE, primaryReporting(NODE, 0, 9, 9, AppliedOutcome.APPLIED)));
        assertTrue(done.satisfied(), done.because());
        assertTrue(done.because().contains("position cannot distinguish"), done.because());
    }

    @Test
    void aNodeUpToDateOnAnAssignmentThatDoesNotCarryTheResetHasNotAcknowledged() {
        // The bug this rule replaced. The task queue runs after the applier but reads the observation taken before
        // it, so on the pass a reset is first written the store holds version N+1 while the observation still shows
        // N -- and a node reporting N looked, by version alone, perfectly acknowledged. It completed the task
        // before the instruction had been written at all.
        TaskRecord task = resumeWithResets(new TaskRecord.IngestionReset(0, ResetMode.OFFSET, "1000"));

        var verdict = evaluateResume(task, Map.of(
            NODE, primaryReporting(NODE, 0, 9, 9, AppliedOutcome.APPLIED, null)
        ));

        assertFalse(verdict.satisfied(), verdict.because());
        assertTrue(verdict.because().contains("does not carry this reset yet"), verdict.because());
    }

    @Test
    void anAssignmentRewrittenForAnUnrelatedReasonStillAcknowledges() {
        // Which is why the id is the identity rather than the version: the version moves when an unrelated shard is
        // placed in the same document, the node skips re-applying by id as the contract requires, and the
        // acknowledgement still has to hold at the newer version.
        TaskRecord task = resumeWithResets(new TaskRecord.IngestionReset(0, ResetMode.OFFSET, "1000"));

        var verdict = evaluateResume(task, Map.of(
            NODE, primaryReporting(NODE, 0, 17, 17, AppliedOutcome.APPLIED, "task-1")
        ));

        assertTrue(verdict.satisfied(), verdict.because());
    }

    @Test
    void everyShardNamedByAResumeHasToAcknowledge() {
        // A reset is per shard and an assignment is per node, so two shards on two nodes need both. Completing on
        // the first would report a pointer moved on a shard whose holder had not heard of it.
        NodeId other = NodeId.of("nodeBBBBBBBBBBBBBBBBBB");
        TaskRecord task = resumeWithResets(
            new TaskRecord.IngestionReset(0, ResetMode.OFFSET, "1000"),
            new TaskRecord.IngestionReset(1, ResetMode.TIMESTAMP, "1789000000000")
        );

        var oneShort = evaluateResume(task, Map.of(
            NODE, primaryReporting(NODE, 0, 9, 9, AppliedOutcome.APPLIED),
            other, primaryReporting(other, 1, 4, 3, AppliedOutcome.APPLIED)
        ));
        assertFalse(oneShort.satisfied(), oneShort.because());
        assertTrue(oneShort.because().contains("shard 1"), oneShort.because());
        assertFalse(oneShort.because().contains("shard 0 "), "shard 0 is settled: " + oneShort.because());

        var both = evaluateResume(task, Map.of(
            NODE, primaryReporting(NODE, 0, 9, 9, AppliedOutcome.APPLIED),
            other, primaryReporting(other, 1, 4, 4, AppliedOutcome.APPLIED)
        ));
        assertTrue(both.satisfied(), both.because());
    }

    @Test
    void aResetForAShardWithNoPrimaryStaysOpenRatherThanCompletingOrFailing() {
        // The shard's missing primary is a placement problem the diff already reports; blaming the resume for it
        // would hide both. Same patience every other kind of work here gets.
        TaskRecord task = resumeWithResets(new TaskRecord.IngestionReset(1, ResetMode.OFFSET, "1000"));

        var verdict = evaluateResume(task,
            Map.of(NODE, primaryReporting(NODE, 0, 9, 9, AppliedOutcome.APPLIED)));

        assertFalse(verdict.satisfied(), verdict.because());
        assertTrue(verdict.because().contains("shard 1 has no primary to reset"), verdict.because());
    }

    @Test
    void theUnpauseHasToLandTooBeforeAResumeIsDone() {
        // A resume is an unpause as well as a reset. A node still on an older index revision has not seen the
        // unpause, however cleanly it acknowledged the reset -- and the metadata half is reported first because it
        // is the half an operator can act on.
        TaskRecord task = resumeWithResets(new TaskRecord.IngestionReset(0, ResetMode.OFFSET, "1000"));
        Observation.NodeObservation stale = primaryReporting(NODE, 0, 9, 9, AppliedOutcome.APPLIED);
        // Same node, but its copy was built against an older index document.
        ShardInventory older = new ShardInventory(NODE, "eph", 1L, Map.of(
            ACME, new ShardInventory.TenantShards(Map.of(
                UUID, new ShardInventory.IndexShards("my-index", 3040, Map.of(
                    0, ShardInventory.ShardCopy.of(ShardRole.PRIMARY, ShardState.STARTED)
                ))
            ))
        ));
        Observation.NodeObservation behindOnConfig = new Observation.NodeObservation(
            NODE, stale.registration(), stale.heartbeat(), older, Liveness.LIVE, POOL, stale.assignment(), 5
        );

        var verdict = evaluateResume(task, Map.of(NODE, behindOnConfig));

        assertFalse(verdict.satisfied(), verdict.because());
        assertFalse(verdict.because().contains("consumer-pointer reset(s) requested"),
            "the metadata half is the one to report: " + verdict.because());
    }

    @Test
    void bothIngestionTypesAreRecognisedRatherThanUnknown() {
        // They used to parse as UNKNOWN, which never completes and becomes TASK_STUCK. The wire names are the
        // control plane's, kebab-cased.
        assertEquals(TaskType.PAUSE_INGESTION, TaskType.from("pause-ingestion"));
        assertEquals(TaskType.RESUME_INGESTION, TaskType.from("resume-ingestion"));
        assertEquals("pause-ingestion", TaskType.PAUSE_INGESTION.wireName());
        assertEquals("resume-ingestion", TaskType.RESUME_INGESTION.wireName());
    }

    @Test
    void aResetModeThisBuildDoesNotKnowReadsAsUnknownRatherThanFailing() {
        assertEquals(ResetMode.OFFSET, ResetMode.from("offset"));
        assertEquals(ResetMode.TIMESTAMP, ResetMode.from("timestamp"));
        assertEquals(ResetMode.UNKNOWN, ResetMode.from("sequence-number"));
    }
}
