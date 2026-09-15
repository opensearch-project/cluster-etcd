package org.opensearch.supervisor.core;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.opensearch.controlplane.store.InMemoryKeyValueStore;
import org.opensearch.supervisor.fakenode.FakeNode;
import org.opensearch.supervisor.model.Keys;
import org.opensearch.supervisor.model.NodeId;
import org.opensearch.supervisor.model.PoolId;
import org.opensearch.supervisor.model.TenantId;
import org.opensearch.supervisor.model.diff.FindingKind;
import org.opensearch.supervisor.model.index.IndexConfig;
import org.opensearch.supervisor.model.node.NodeRole;
import org.opensearch.supervisor.model.node.Registration;
import org.opensearch.supervisor.model.plan.Planner;
import org.opensearch.supervisor.model.pool.PoolBinding;
import org.opensearch.supervisor.model.pool.PoolDefinition;
import org.opensearch.supervisor.model.pool.PoolKind;
import org.opensearch.supervisor.model.status.Replication;
import org.opensearch.supervisor.model.task.TaskRecord;
import org.opensearch.supervisor.model.task.TaskState;
import org.opensearch.supervisor.model.task.TaskType;

import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Draining the control plane's queue, and the two properties that matter more than the draining does: that
 * reconciliation works with the queue empty, and that a lost or duplicated task changes nothing.
 */
class TaskQueueTests {

    private static final PoolId POOL = PoolId.of("shared-a");
    private static final TenantId ACME = TenantId.of("acme");
    private static final NodeId NODE = NodeId.of("nodeAAAAAAAAAAAAAAAAAA");
    private static final String UUID = "uzKxLWZu4nPSrTknEcDpoA";
    private static final Duration TTL = Duration.ofSeconds(15);

    private InMemoryKeyValueStore store;
    private MutableClock clock;

    private static final class MutableClock extends Clock {
        private Instant now = Instant.ofEpochMilli(1_000_000);

        @Override
        public ZoneId getZone() {
            return ZoneOffset.UTC;
        }

        @Override
        public Clock withZone(ZoneId zone) {
            return this;
        }

        @Override
        public Instant instant() {
            return now;
        }

        void advance(Duration by) {
            now = now.plus(by);
        }
    }

    @BeforeEach
    void setUp() {
        store = new InMemoryKeyValueStore();
        clock = new MutableClock();
        store.put(Keys.poolDefinition(POOL), new PoolDefinition(POOL, PoolKind.SHARED, false, null, null, java.util.List.of(), false).toBytes());
        store.put(Keys.poolBinding(ACME), PoolBinding.to(POOL).toBytes());
        store.put(Keys.tenantRegistration(ACME), "{\"tenant\":\"acme\"}".getBytes(StandardCharsets.UTF_8));
    }

    private FakeNode dataNode() {
        return new FakeNode(store, new Registration(
            NODE, "placeholder", "data-1", POOL, EnumSet.of(NodeRole.DATA),
            "10.0.0.1", 9300, "3.7.0", Map.of("zone", "z1"), 5000, 0L
        ), clock, "eph-1");
    }

    private Supervisor supervisor(String id) {
        return new Supervisor(store, clock, id, Set.of(), 1.0, TTL, Balancer.Config.defaults(),
            new Planner.Policy(200, 5, 2), false);
    }

    private void createIndex(int shards) {
        store.put(Keys.indicesPrefix(ACME) + "my-index", IndexConfig.toDocument(
            "my-index", UUID, true, IndexConfig.settingsOf(shards, 0, Replication.SEGMENT)
        ));
    }

    /** Writes what the control plane writes: state pending, no supervisor fields at all. */
    private void submitTask(String taskId, TaskType type) {
        store.put(Keys.tasksPrefix(ACME) + taskId, new TaskRecord(
            taskId, type, "my-index", UUID, TaskState.PENDING, clock.millis(), 1, null, 0, 0, 0, null, 0
        ).toBytes());
    }

    private Optional<TaskRecord> taskOf(String taskId) {
        return store.get(Keys.tasksPrefix(ACME) + taskId).map(v -> TaskRecord.from(v.value()));
    }

    private void run(Supervisor supervisor, FakeNode node, int passes) {
        for (int pass = 0; pass < passes; pass++) {
            supervisor.tick();
            if (node != null) {
                node.tick();
            }
            clock.advance(Duration.ofSeconds(2));
        }
    }

    // ---------------------------------------------------------------- draining

    @Test
    void aTaskIsClaimedAndThenCompletedAsTheWorkGetsDone() {
        FakeNode node = dataNode();
        node.start();
        createIndex(1);
        submitTask("task-1", TaskType.CREATE_INDEX);
        Supervisor supervisor = supervisor("s1");

        run(supervisor, node, 8);

        TaskRecord task = taskOf("task-1").orElseThrow();
        assertEquals(TaskState.COMPLETED, task.state());
        assertTrue(task.attempts() >= 1, "completing without ever claiming would lose the attempt count");
    }

    @Test
    void pendingTasksStopBeingPendingAsSoonAsTheyAreClaimed() {
        // cluster.pending_tasks reports only state=pending, so the pile the control plane has been growing
        // starts draining the moment claiming works -- before completion is even involved.
        createIndex(3);
        submitTask("task-1", TaskType.CREATE_INDEX);
        Supervisor supervisor = supervisor("s1");

        run(supervisor, null, 3);

        assertFalse(taskOf("task-1").orElseThrow().state() == TaskState.PENDING);
    }

    @Test
    void aCompletedTaskIsReapedOnceItsRetentionWindowHasPassed() {
        FakeNode node = dataNode();
        node.start();
        createIndex(1);
        submitTask("task-1", TaskType.CREATE_INDEX);
        Supervisor supervisor = supervisor("s1");

        run(supervisor, node, 8);
        assertEquals(TaskState.COMPLETED, taskOf("task-1").orElseThrow().state());

        // The record exists at all only so an operator has a moment to see it finished.
        clock.advance(TaskQueue.Policy.defaults().retention().plusMinutes(1));
        run(supervisor, node, 2);

        assertTrue(taskOf("task-1").isEmpty(), "the queue must actually drain, not just change state");
    }

    @Test
    void severalTasksAllDrain() {
        FakeNode node = dataNode();
        node.start();
        createIndex(1);
        for (int i = 0; i < 20; i++) {
            submitTask("task-" + i, TaskType.CREATE_INDEX);
        }
        Supervisor supervisor = supervisor("s1");

        run(supervisor, node, 10);

        long outstanding = store.scanPrefix(Keys.tasksPrefix(ACME))
            .values()
            .stream()
            .map(v -> TaskRecord.from(v.value()))
            .filter(task -> task.state() != TaskState.COMPLETED)
            .count();
        assertEquals(0, outstanding);
    }

    @Test
    void aTaskThatCanNeverCompleteEventuallyFailsRatherThanLoopingForEver() {
        // R4.2. A delete task for an index the control plane has not removed can never be satisfied, and the
        // interesting question is what happens after a while -- silence, or a record somebody can find.
        createIndex(1);
        submitTask("task-1", TaskType.DELETE_INDEX);
        Supervisor supervisor = supervisor("s1");

        // Each pass past the claim lease is another attempt, so time has to pass, not just passes.
        for (int pass = 0; pass < 40; pass++) {
            supervisor.tick();
            clock.advance(TaskQueue.Policy.defaults().claimLease().plusSeconds(1));
        }

        TaskRecord task = taskOf("task-1").orElseThrow();
        assertEquals(TaskState.FAILED, task.state());
        assertEquals(TaskQueue.Policy.defaults().maxAttempts(), task.attempts());
        assertTrue(task.lastError().contains("still in desired state"), task.lastError());
    }

    @Test
    void aFailedTaskIsNeverReapedSoTheFailureStaysVisible() {
        createIndex(1);
        submitTask("task-1", TaskType.DELETE_INDEX);
        Supervisor supervisor = supervisor("s1");

        for (int pass = 0; pass < 40; pass++) {
            supervisor.tick();
            clock.advance(TaskQueue.Policy.defaults().claimLease().plusSeconds(1));
        }
        assertEquals(TaskState.FAILED, taskOf("task-1").orElseThrow().state());

        clock.advance(Duration.ofDays(7));
        run(supervisor, null, 3);

        assertTrue(taskOf("task-1").isPresent(), "a deleted failure is an invisible failure");
    }

    @Test
    void aFailedTaskIsReportedInTheDiff() {
        createIndex(1);
        submitTask("task-1", TaskType.DELETE_INDEX);
        Supervisor supervisor = supervisor("s1");

        for (int pass = 0; pass < 40; pass++) {
            supervisor.tick();
            clock.advance(TaskQueue.Policy.defaults().claimLease().plusSeconds(1));
        }
        supervisor.tick();

        assertFalse(supervisor.lastDiffOf(POOL).of(FindingKind.TASK_STUCK).isEmpty(),
            "a failed task must show up where an operator already looks");
    }

    @Test
    void anUnrecognisedTaskTypeIsLeftAloneAndReported() {
        // A newer control plane writing a task an older supervisor cannot serve is a real deployment state.
        // Completing it would report success for work it cannot name; claiming it would take responsibility
        // for the same.
        createIndex(1);
        store.put(Keys.tasksPrefix(ACME) + "task-1",
            ("{\"task_id\":\"task-1\",\"type\":\"rollover\",\"index\":\"my-index\",\"index_uuid\":\"" + UUID
                + "\",\"state\":\"pending\",\"submitted_at\":1000,\"sequence\":1}").getBytes(StandardCharsets.UTF_8));
        Supervisor supervisor = supervisor("s1");

        run(supervisor, null, 6);

        assertEquals(TaskState.PENDING, taskOf("task-1").orElseThrow().state(), "left for a newer supervisor");
        assertFalse(supervisor.lastDiffOf(POOL).of(FindingKind.TASK_STUCK).isEmpty());
    }

    @Test
    void anUnreadableRecordDoesNotStopTheRestDraining() {
        FakeNode node = dataNode();
        node.start();
        createIndex(1);
        submitTask("task-good", TaskType.CREATE_INDEX);
        store.put(Keys.tasksPrefix(ACME) + "task-bad", "{ not json".getBytes(StandardCharsets.UTF_8));
        Supervisor supervisor = supervisor("s1");

        run(supervisor, node, 8);

        assertEquals(TaskState.COMPLETED, taskOf("task-good").orElseThrow().state());
    }

    @Test
    void aSlowlyConvergingTaskRenewsItsClaimRatherThanSpendingAttempts() {
        // The hazard created by defining complete as converged: an index big enough to take longer to recover
        // than the attempt budget allows would be marked failed for making steady progress -- blaming the task
        // for the size of the index. Progress is what tells slow from stuck.
        FakeNode node = dataNode();
        node.start();
        createIndex(6);
        submitTask("task-1", TaskType.CREATE_INDEX);
        Supervisor supervisor = supervisor("s1");

        // Far more elapsed claim leases than the attempt budget, with the node converging one step at a time.
        for (int pass = 0; pass < 30; pass++) {
            supervisor.tick();
            node.tick();
            clock.advance(TaskQueue.Policy.defaults().claimLease().plusSeconds(1));
        }

        TaskRecord task = taskOf("task-1").orElseThrow();
        assertEquals(TaskState.COMPLETED, task.state(),
            "a task that kept getting somewhere must not be failed: " + task.lastError());
        assertTrue(task.attempts() <= TaskQueue.Policy.defaults().maxAttempts(),
            "attempts should track stalls, not passes: " + task.attempts());
    }

    // ---------------------------------------------------------------- claims

    @Test
    void anExpiredClaimCanBeTakenOverAndCountsAsAFreshAttempt() {
        createIndex(1);
        submitTask("task-1", TaskType.DELETE_INDEX);

        // A supervisor that claimed and then died, leaving its claim behind.
        store.put(Keys.tasksPrefix(ACME) + "task-1", new TaskRecord(
            "task-1", TaskType.DELETE_INDEX, "my-index", UUID, TaskState.CLAIMED,
            1000, 1, "dead-supervisor", 1, clock.millis() - 1, 1, null, 0
        ).toBytes());

        Supervisor supervisor = supervisor("s1");
        run(supervisor, null, 3);

        TaskRecord task = taskOf("task-1").orElseThrow();
        assertEquals("s1", task.holder());
        assertEquals(2, task.attempts(), "taking over a dead claim is the second attempt, not the first");
    }

    @Test
    void aLiveClaimHeldBySomebodyElseIsLeftAlone() {
        createIndex(1);
        store.put(Keys.tasksPrefix(ACME) + "task-1", new TaskRecord(
            "task-1", TaskType.DELETE_INDEX, "my-index", UUID, TaskState.CLAIMED,
            1000, 1, "other-supervisor", 1, clock.millis() + 600_000, 1, null, 0
        ).toBytes());

        Supervisor supervisor = supervisor("s1");
        run(supervisor, null, 4);

        TaskRecord task = taskOf("task-1").orElseThrow();
        assertEquals("other-supervisor", task.holder(), "somebody else's live claim is the point of the lease");
        assertEquals(1, task.attempts());
    }

    @Test
    void aFleetDoesNotDoubleCountAttemptsOnTheSameTask() {
        // Three supervisors, one lease on the pool. Only the holder touches the queue, and the CAS on the
        // record is what makes that true rather than any agreement between them.
        FakeNode node = dataNode();
        node.start();
        createIndex(1);
        submitTask("task-1", TaskType.CREATE_INDEX);

        var fleet = List.of(supervisor("s1"), supervisor("s2"), supervisor("s3"));
        for (int pass = 0; pass < 10; pass++) {
            fleet.forEach(Supervisor::tick);
            node.tick();
            clock.advance(Duration.ofSeconds(2));
        }

        TaskRecord task = taskOf("task-1").orElseThrow();
        assertEquals(TaskState.COMPLETED, task.state());
        assertTrue(task.attempts() <= 2, "attempts should not grow once per supervisor: " + task.attempts());
    }

    // ---------------------------------------------------------------- the properties that matter

    @Test
    void reconciliationWorksWithTheQueueCompletelyEmpty() {
        // R2.1, and the most important test in this file. A supervisor that only acted on tasks would never
        // converge state that predates it, or state whose task it consumed before crashing. So: create an
        // index, submit no task at all, and the shard must still be placed.
        FakeNode node = dataNode();
        node.start();
        createIndex(2);
        Supervisor supervisor = supervisor("s1");

        run(supervisor, node, 8);

        assertTrue(store.scanPrefix(Keys.tasksPrefix(ACME)).isEmpty(), "no task was ever written");
        assertEquals(2, node.inventory().copyCount(), "and the work happened anyway");
        assertTrue(supervisor.lastDiffOf(POOL).isEmpty());
    }

    @Test
    void aDuplicatedTaskIsHarmless() {
        // R4.4: correctness must not depend on a task being seen exactly once. Two records for the same work
        // both complete, because completion is a question about the world rather than about having acted.
        FakeNode node = dataNode();
        node.start();
        createIndex(1);
        submitTask("task-1", TaskType.CREATE_INDEX);
        submitTask("task-2", TaskType.CREATE_INDEX);
        Supervisor supervisor = supervisor("s1");

        run(supervisor, node, 8);

        assertEquals(TaskState.COMPLETED, taskOf("task-1").orElseThrow().state());
        assertEquals(TaskState.COMPLETED, taskOf("task-2").orElseThrow().state());
        assertEquals(1, node.inventory().copyCount(), "and no duplicate shard came of it");
    }

    @Test
    void aTaskThatArrivesBeforeTheIndexDocumentIsNotMistakenForCompletedWork() {
        // KEY_LAYOUT.md warns that a task can arrive before the index document it refers to. The task looks
        // like "nothing to create", which is the same shape as "the index was deleted" -- and both are
        // genuinely complete, because in neither case is there work outstanding. What must not happen is that
        // the index arriving later is then ignored.
        submitTask("task-1", TaskType.CREATE_INDEX);
        FakeNode node = dataNode();
        node.start();
        Supervisor supervisor = supervisor("s1");
        run(supervisor, node, 4);

        assertEquals(TaskState.COMPLETED, taskOf("task-1").orElseThrow().state());

        createIndex(1);
        run(supervisor, node, 8);

        assertEquals(1, node.inventory().copyCount(), "the index is placed from its document, not its task");
    }

    @Test
    void aReadOnlySupervisorTouchesNoTasks() {
        createIndex(1);
        submitTask("task-1", TaskType.CREATE_INDEX);

        Supervisor readOnly = new Supervisor(store, clock, "s1", Set.of(), 1.0, TTL,
            Balancer.Config.defaults(), Planner.Policy.defaults(), true);
        run(readOnly, null, 6);

        assertEquals(TaskState.PENDING, taskOf("task-1").orElseThrow().state());
    }

    @Test
    void tasksForATenantInAnotherPoolAreNotTouched() {
        // P2 gives this subtree a single writer: the owner of the tenant's pool. Draining another pool's
        // tenant's queue would break that, and nothing would notice until two supervisors disagreed.
        createIndex(1);
        submitTask("task-1", TaskType.CREATE_INDEX);
        store.put(Keys.poolBinding(ACME), PoolBinding.to(PoolId.of("elsewhere")).toBytes());

        Supervisor supervisor = supervisor("s1");
        run(supervisor, null, 6);

        assertEquals(TaskState.PENDING, taskOf("task-1").orElseThrow().state());
    }
}
