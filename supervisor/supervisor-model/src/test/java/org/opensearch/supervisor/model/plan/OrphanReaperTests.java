package org.opensearch.supervisor.model.plan;

import org.junit.jupiter.api.Test;
import org.opensearch.supervisor.model.NodeId;
import org.opensearch.supervisor.model.PoolId;
import org.opensearch.supervisor.model.TenantId;
import org.opensearch.supervisor.model.diff.Diff;
import org.opensearch.supervisor.model.diff.Differ;
import org.opensearch.supervisor.model.diff.FindingAges;
import org.opensearch.supervisor.model.diff.FindingKind;
import org.opensearch.supervisor.model.index.IndexConfig;
import org.opensearch.supervisor.model.node.Assignment;
import org.opensearch.supervisor.model.node.Heartbeat;
import org.opensearch.supervisor.model.node.NodeRole;
import org.opensearch.supervisor.model.node.Registration;
import org.opensearch.supervisor.model.node.ShardInventory;
import org.opensearch.supervisor.model.node.ShardRole;
import org.opensearch.supervisor.model.node.ShardState;
import org.opensearch.supervisor.model.observe.Observation;
import org.opensearch.supervisor.model.settings.AllocationEnable;
import org.opensearch.supervisor.model.status.Liveness;
import org.opensearch.supervisor.model.status.Replication;
import org.opensearch.supervisor.model.task.TaskRecord;
import org.opensearch.supervisor.model.task.TaskState;
import org.opensearch.supervisor.model.task.TaskType;

import java.time.Instant;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Letting go of a copy nobody desires — the only rule in this service that destroys data.
 *
 * <p>So the cases that matter most here are the ones where it must <em>not</em> fire, and they are not
 * hypothetical: each corresponds to a way the supervisor can be wrong about what exists rather than a way a
 * cluster can be unusual. The ages are produced by running the real differ over advancing passes, because the
 * whole rule is about age and a fixture that stamped the ages in would be testing the assertion, not the code.
 */
class OrphanReaperTests {

    private static final PoolId POOL = PoolId.of("shared-a");
    private static final TenantId ACME = TenantId.of("acme");
    private static final String GONE = "uzKxLWZu4nPSrTknEcDpoA";
    private static final String LIVING = "vBxKLWZu4nPSrTknEcDpoA";
    private static final NodeId NODE = NodeId.of("nodeAAAAAAAAAAAAAAAAAA");

    private static Registration registration() {
        return new Registration(
            NODE, "eph", NODE.value(), POOL, EnumSet.of(NodeRole.DATA),
            "10.0.0.1", 9300, "3.7.0", Map.of(), 5000, 1L
        );
    }

    private static Heartbeat heartbeat() {
        return new Heartbeat(NODE, "eph", 1L, Heartbeat.Applied.NONE, null, 0,
            new Heartbeat.Capacity(1000, 500, 10, 10, 0.1));
    }

    private static IndexConfig index(String uuid, String name) {
        return new IndexConfig(name, uuid, true, 1, 0, Replication.SEGMENT, false, AllocationEnable.UNKNOWN, 3041);
    }

    private static ShardInventory holding(String uuid) {
        return new ShardInventory(NODE, "eph", 1L, Map.of(
            ACME, new ShardInventory.TenantShards(Map.of(
                uuid, new ShardInventory.IndexShards("my-index", 3041, Map.of(
                    0, ShardInventory.ShardCopy.of(ShardRole.PRIMARY, ShardState.STARTED)
                ))
            ))
        ));
    }

    private static Assignment assigned(String uuid) {
        return new Assignment(1, POOL, 1, NODE, 1L, Map.of(
            ACME, new Assignment.TenantAssignment(Set.of(NodeRole.DATA), Map.of(
                uuid, new Assignment.IndexAssignment("my-index", 3041, Map.of(0, ShardRole.PRIMARY))
            ))
        ));
    }

    private static TaskRecord deletionOf(String uuid, TaskState state) {
        return new TaskRecord("task-1", TaskType.DELETE_INDEX, "my-index", uuid, state,
            1000, 1, null, 0, 0, 0, null, 0, null);
    }

    /** A node that both holds and is assigned one copy of {@code GONE}, which is what a deleted index leaves. */
    private static Observation observe(long pass, Map<String, IndexConfig> indices, List<TaskRecord> tasks) {
        return observe(pass, indices, tasks, holding(GONE), assigned(GONE));
    }

    private static Observation observe(
        long pass,
        Map<String, IndexConfig> indices,
        List<TaskRecord> tasks,
        ShardInventory inventory,
        Assignment assignment
    ) {
        Observation.NodeObservation node = new Observation.NodeObservation(
            NODE, registration(), heartbeat(), inventory, Liveness.LIVE, POOL, assignment, 7
        );
        return new Observation(pass, Instant.EPOCH, POOL,
            Map.of(ACME, new Observation.DesiredState(indices, tasks)),
            Map.of(NODE, node), List.of());
    }

    /** An observation with no tenant bound to this pool at all. */
    private static Observation observeUnbound(long pass) {
        Observation.NodeObservation node = new Observation.NodeObservation(
            NODE, registration(), heartbeat(), holding(GONE), Liveness.LIVE, POOL, assigned(GONE), 7
        );
        return new Observation(pass, Instant.EPOCH, POOL, Map.of(), Map.of(NODE, node), List.of());
    }

    private static OrphanReaper.Outcome reapAfter(int passes, Observation observation) {
        FindingAges ages = new FindingAges();
        Diff diff = null;
        for (int pass = 1; pass <= passes; pass++) {
            diff = Differ.diff(new Observation(
                pass, observation.observedAt(), observation.pool(),
                observation.tenants(), observation.nodes(), observation.unreadable()
            ), ages);
        }
        return OrphanReaper.reap(observation, diff, 2);
    }

    private static String refusal(OrphanReaper.Outcome outcome) {
        assertEquals(1, outcome.refusals().size(), "one orphan, so one reason: " + outcome.refusals());
        return outcome.refusals().values().iterator().next();
    }

    // ---------------------------------------------------------------- acting on it

    @Test
    void aDeletionTaskIsEnoughEvidenceToWithdrawTheCopyPromptly() {
        // The loop this closes: the task cannot finish while the copy is held, and the copy was never dropped
        // because nothing consumed the finding. Two passes of patience is right here because the evidence is
        // positive -- the control plane asked for this index to be deleted -- rather than an absence.
        OrphanReaper.Outcome outcome = reapAfter(2, observe(2,
            Map.of(LIVING, index(LIVING, "other-index")),
            List.of(deletionOf(GONE, TaskState.PENDING))
        ));

        assertEquals(1, outcome.removals().size());
        OrphanReaper.Removal removal = outcome.removals().get(0);
        assertEquals(GONE, removal.indexUuid());
        assertEquals(NODE, removal.node());
        assertEquals(0, removal.shard());
        assertTrue(removal.because().contains("task-1"), "the reason must name the evidence: " + removal.because());
    }

    @Test
    void aDeletionTaskGivenUpOnIsStillEvidenceThatTheIndexWasDeleted() {
        // A task's state is bookkeeping about this service's own progress. Its existence is a fact about the
        // control plane, and that is what is being read. Worth a test because the failing state is exactly what
        // a pool full of unreapable orphans produces: the task retries until it is given up on, and requiring a
        // live task would make the cleanup impossible in precisely the case that needs it.
        OrphanReaper.Outcome outcome = reapAfter(2, observe(2,
            Map.of(LIVING, index(LIVING, "other-index")),
            List.of(deletionOf(GONE, TaskState.FAILED))
        ));

        assertEquals(1, outcome.removals().size());
    }

    @Test
    void anAbsenceAloneIsEventuallyEnough() {
        OrphanReaper.Outcome outcome = reapAfter(
            (int) OrphanReaper.PATIENCE_WITHOUT_A_DELETION_TASK,
            observe(OrphanReaper.PATIENCE_WITHOUT_A_DELETION_TASK,
                Map.of(LIVING, index(LIVING, "other-index")), List.of())
        );

        assertEquals(1, outcome.removals().size());
        assertTrue(outcome.removals().get(0).because().contains("no deletion task"),
            "the reason must say the evidence was weaker: " + outcome.removals().get(0).because());
    }

    @Test
    void anAssignmentEntryIsWithdrawnEvenWhereTheNodeHoldsNothing() {
        // The half that is easy to miss, and the expensive one. A node defers an entry naming an index it cannot
        // resolve and reports a deferred assignment as failed -- so the entry alone stops this node ever
        // reporting an assignment as applied, which is the acknowledgement a consumer-pointer reset completes
        // on. One deleted index silently disables pointer resets on the node for ever.
        OrphanReaper.Outcome outcome = reapAfter(2, observe(2,
            Map.of(LIVING, index(LIVING, "other-index")),
            List.of(deletionOf(GONE, TaskState.PENDING)),
            null,
            assigned(GONE)
        ));

        assertEquals(1, outcome.removals().size());
        assertEquals(GONE, outcome.removals().get(0).indexUuid());
    }

    @Test
    void aCopyBothHeldAndAssignedIsWithdrawnOnce() {
        OrphanReaper.Outcome outcome = reapAfter(2, observe(2,
            Map.of(LIVING, index(LIVING, "other-index")),
            List.of(deletionOf(GONE, TaskState.PENDING))
        ));

        assertEquals(1, outcome.removals().size(), "held and assigned are one copy, not two");
    }

    // ---------------------------------------------------------------- refusing to act on it

    @Test
    void anAbsenceWithNoDeletionTaskWaitsLongerThanATornReadCouldLast() {
        // Two passes is calibrated for a torn read: R2.5 promises the supervisor will see an index without its
        // alias, and the next pass settles it. A mistake in *reading* desired state is not a race and does not
        // settle, so at two passes of patience a control plane whose key layout moved would take the pool's data
        // with it about ten seconds later.
        OrphanReaper.Outcome outcome = reapAfter(5, observe(5,
            Map.of(LIVING, index(LIVING, "other-index")), List.of()));

        assertEquals(List.of(), outcome.removals());
        assertTrue(refusal(outcome).contains("only evidence"),
            "a thirty-pass wait with no reason beside it reads as nothing happening: " + refusal(outcome));
    }

    @Test
    void aTenantWithNoIndexDocumentsAtAllIsNotReadAsATenantWithNothingToKeep() {
        // The unrecoverable one. An empty index map is what a tenant whose indices were all deleted looks like,
        // and also what a failed read of its index prefix looks like, and the two want opposite actions. Kept
        // for ever rather than deleted once, because only one of those two mistakes can be undone.
        OrphanReaper.Outcome outcome = reapAfter(200, observe(200, Map.of(), List.of()));

        assertEquals(List.of(), outcome.removals());
        assertTrue(refusal(outcome).contains("failed read"),
            "the reason has to name the ambiguity: " + refusal(outcome));
    }

    @Test
    void aTenantWithNoIndexDocumentsIsStillCleanedUpWhenATaskSaysTheIndexWasDeleted() {
        // And the escape hatch from the rule above: a deletion task naming the uuid is positive evidence, so an
        // empty index map is no longer the only thing being read. Without this, deleting a tenant's last index
        // would leave a copy nothing could ever remove.
        OrphanReaper.Outcome outcome = reapAfter(2, observe(2, Map.of(),
            List.of(deletionOf(GONE, TaskState.PENDING))));

        assertEquals(1, outcome.removals().size());
    }

    @Test
    void aTenantThisPoolDoesNotOwnKeepsItsData() {
        // A tenant rebound to another pool looks exactly like this from here: its indices are absent because
        // this supervisor no longer reads them, not because they stopped existing. The copies are that tenant's
        // data, being served by whichever pool owns it now.
        OrphanReaper.Outcome outcome = reapAfter(200, observeUnbound(200));

        assertEquals(List.of(), outcome.removals());
        assertTrue(refusal(outcome).contains("not bound to this pool"), refusal(outcome));
    }

    @Test
    void aCopyHeldWithNoAssignmentEntryIsNotThisServicesToWithdraw() {
        // Nothing left to say: the assignment already omits the index, which is the strongest statement this
        // service can make. Planning a removal anyway would write no change and report a copy dropped.
        OrphanReaper.Outcome outcome = reapAfter(2, observe(2,
            Map.of(LIVING, index(LIVING, "other-index")),
            List.of(deletionOf(GONE, TaskState.PENDING)),
            holding(GONE),
            null
        ));

        assertEquals(List.of(), outcome.removals());
        assertTrue(refusal(outcome).contains("nothing left to withdraw"), refusal(outcome));
    }

    @Test
    void anIndexStillInDesiredStateIsNoOrphanEvenWithADeletionTaskAgainstIt() {
        // The ordering that makes reading a task safe. A task naming a live index raises no finding, so the
        // evidence cannot be applied to an index the control plane has not finished removing.
        Observation observation = observe(2, Map.of(GONE, index(GONE, "my-index")),
            List.of(deletionOf(GONE, TaskState.PENDING)));

        FindingAges ages = new FindingAges();
        Diff diff = Differ.diff(observation, ages);

        assertEquals(List.of(), diff.of(FindingKind.ORPHANED_COPY));
        assertEquals(List.of(), OrphanReaper.reap(observation, diff, 2).removals());
    }

    // ---------------------------------------------------------------- through the planner

    @Test
    void thePlannerTurnsAnAgedOrphanIntoARemoval() {
        Observation observation = observe(2, Map.of(LIVING, index(LIVING, "other-index")),
            List.of(deletionOf(GONE, TaskState.PENDING)));
        FindingAges ages = new FindingAges();
        Diff diff = null;
        for (int pass = 1; pass <= 2; pass++) {
            diff = Differ.diff(new Observation(pass, observation.observedAt(), observation.pool(),
                observation.tenants(), observation.nodes(), observation.unreadable()), ages);
        }

        Plan plan = Planner.plan(observation, diff, 7, Planner.Policy.defaults());

        Instruction.RemoveCopy remove = plan.instructions().stream()
            .filter(Instruction.RemoveCopy.class::isInstance)
            .map(Instruction.RemoveCopy.class::cast)
            .filter(instruction -> GONE.equals(instruction.indexUuid()))
            .findFirst()
            .orElse(null);
        assertNotNull(remove, "the orphan has to reach the plan, or none of this is connected: "
            + plan.instructions());
        assertEquals(NODE, remove.node());
    }

    @Test
    void aRefusalToReapIsReadableFromThePlanAgainstItsOwnFinding() {
        // Same channel as a refusal to place, so that "why is this still here" has one answer in one place.
        Observation observation = observe(200, Map.of(), List.of());
        FindingAges ages = new FindingAges();
        Diff diff = null;
        for (int pass = 1; pass <= 200; pass++) {
            diff = Differ.diff(new Observation(pass, observation.observedAt(), observation.pool(),
                observation.tenants(), observation.nodes(), observation.unreadable()), ages);
        }

        Plan plan = Planner.plan(observation, diff, 7, Planner.Policy.defaults());

        assertEquals(List.of(), plan.instructions());
        String refusal = plan.refusalFor(diff.of(FindingKind.ORPHANED_COPY).get(0));
        assertNotNull(refusal, "an orphan this declines to remove has to explain itself where a copy it "
            + "declines to place does");
        assertTrue(refusal.contains("failed read"), refusal);
    }
}
