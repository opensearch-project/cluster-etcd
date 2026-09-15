package org.opensearch.supervisor.model.task;

import org.junit.jupiter.api.Test;
import org.opensearch.supervisor.model.NodeId;
import org.opensearch.supervisor.model.PoolId;
import org.opensearch.supervisor.model.TenantId;
import org.opensearch.supervisor.model.index.IndexConfig;
import org.opensearch.supervisor.model.node.Assignment;
import org.opensearch.supervisor.model.node.NodeRole;
import org.opensearch.supervisor.model.node.Registration;
import org.opensearch.supervisor.model.node.ShardInventory;
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
            1000, 1, null, 0, 0, 0, null, 0);
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
            1000, 99, null, 0, 0, 0, null, 0);
        TaskRecord second = new TaskRecord("bbb", TaskType.CREATE_INDEX, "i", UUID, TaskState.PENDING,
            1000, 1, null, 0, 0, 0, null, 0);

        assertTrue(TaskRecord.STABLE_ORDER.compare(first, second) < 0,
            "same millisecond, so the id decides -- not the sequence");
    }
}
