package org.opensearch.supervisor.model.plan;

import org.opensearch.supervisor.model.NodeId;
import org.opensearch.supervisor.model.TenantId;
import org.opensearch.supervisor.model.diff.Diff;
import org.opensearch.supervisor.model.diff.Finding;
import org.opensearch.supervisor.model.diff.FindingKind;
import org.opensearch.supervisor.model.observe.Observation;
import org.opensearch.supervisor.model.task.TaskRecord;
import org.opensearch.supervisor.model.task.TaskType;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Lets go of a copy whose index is gone — the other half of {@code ORPHANED_COPY}, which for a long time was
 * raised and consumed by nothing.
 *
 * <p>What that cost was not obvious from the finding. A copy nobody desires is not merely waste:
 *
 * <ul>
 *   <li>its {@code DELETE_INDEX} task can never finish, because {@code TaskCompletion} quite rightly refuses
 *       while any node still holds or is assigned the index — so the queue accumulates a stuck task per
 *       deleted index;
 *   <li>and the node holding it can never report {@code applied} again, because the contract has it defer an
 *       assignment entry it cannot resolve and report anything deferred as a failure. That is the expensive
 *       one: the supervisor completes a consumer-pointer reset only on an {@code APPLIED} acknowledgement, so
 *       one deleted index silently disables pointer resets on that node for ever.
 * </ul>
 *
 * <p><b>Deleting data on the strength of an absence is the most dangerous thing this service does</b>, and
 * every rule below exists because the obvious version of it destroys a pool in some plausible circumstance.
 * The question is never "is this copy wanted?" — it is "do I know enough to be sure nobody wants it?", and
 * absence of evidence answers the first question while looking like an answer to the second.
 */
public final class OrphanReaper {

    /**
     * How long an orphan must hold when nothing but its absence says it was deleted.
     *
     * <p>Much longer than {@code minimumPasses}, and deliberately not that setting. Two passes of patience is
     * calibrated for a torn read — R2.5 promises the supervisor will observe an index without its alias, and one
     * more pass settles it. This is calibrated for something else: a mistake in <em>reading</em> desired state,
     * which does not resolve on the next pass because it is not a race. A prefix that moved, a key layout the
     * control plane changed, a deserialisation that started throwing — each makes every index look deleted at
     * once, and at two passes of patience the pool is gone about ten seconds later.
     *
     * <p>Thirty passes does not make that safe, and nothing here could. What it buys is that the operator who
     * notices a control plane behaving oddly has minutes rather than seconds, and that the diff shows a wall of
     * {@code ORPHANED_COPY} findings with a refusal explaining the wait before anything is destroyed.
     */
    public static final long PATIENCE_WITHOUT_A_DELETION_TASK = 30;

    /** A copy to stop asking a node to hold. */
    public record Removal(TenantId tenant, String indexUuid, int shard, NodeId node, String because) {}

    /**
     * @param refusals keyed by {@link Finding#refusalKey()}, so an orphan this declines to remove explains
     *     itself in the same place a copy this declines to place does. Several of the rules below never
     *     resolve on their own, and an unexplained finding that sits for ever is how the last version of
     *     this was discovered by someone reading a status document in confusion.
     */
    public record Outcome(List<Removal> removals, Map<String, String> refusals) {}

    private OrphanReaper() {}

    public static Outcome reap(Observation observation, Diff diff, long minimumPasses) {
        List<Removal> removals = new ArrayList<>();
        Map<String, String> refusals = new LinkedHashMap<>();

        for (Finding finding : diff.of(FindingKind.ORPHANED_COPY)) {
            TenantId tenant = finding.subject().tenant();
            String indexUuid = finding.subject().indexUuid();
            Integer shard = finding.subject().shard();
            NodeId node = finding.subject().node();
            if (tenant == null || indexUuid == null || shard == null || node == null) {
                continue;
            }

            Observation.DesiredState desired = observation.tenants().get(tenant);
            if (desired == null) {
                // The tenant is not bound to this pool, so this supervisor did not read its index documents and
                // cannot tell a deleted index from one it simply has no business knowing about. A tenant rebound
                // to another pool looks exactly like this, and the copies on these nodes are that tenant's data,
                // being served by whoever owns it now.
                refusals.put(finding.refusalKey(), "tenant [" + tenant.value() + "] is not bound to this pool, so"
                    + " this supervisor never read its index documents and will not delete a copy on the strength"
                    + " of an index it was never in a position to see");
                continue;
            }

            TaskRecord deletion = deletionTaskFor(desired, indexUuid);
            if (deletion == null && desired.indicesByUuid().isEmpty()) {
                // A tenant with no index documents at all is indistinguishable from a failed read of its index
                // prefix, and the two want opposite actions: delete everything, or touch nothing. So the copies
                // are kept and the finding is left to age, which is the only one of the two that is recoverable.
                //
                // This does mean a tenant whose last index was deleted, and whose deletion task the control plane
                // has since pruned, holds that copy indefinitely. Accepted: it costs disk and a standing finding,
                // against a rule whose failure costs a tenant's entire dataset.
                refusals.put(finding.refusalKey(), "tenant [" + tenant.value() + "] has no index documents at all,"
                    + " which is indistinguishable from a failed read of its index prefix; a deletion task naming"
                    + " index uuid " + indexUuid + " would settle it, and none does");
                continue;
            }

            long patience = deletion == null ? PATIENCE_WITHOUT_A_DELETION_TASK : minimumPasses;
            if (finding.passes() < patience) {
                if (deletion == null) {
                    // Only worth saying on the long path. A copy two passes from removal needs no explanation;
                    // one thirty passes from it, sitting in a diff with no reason beside it, invites exactly the
                    // conclusion that nothing is going to happen -- which was true until now.
                    refusals.put(finding.refusalKey(), "no deletion task names index uuid " + indexUuid + ", so its"
                        + " absence from desired state is the only evidence it was deleted; waiting "
                        + patience + " passes before deleting data on that alone, and this has held for "
                        + finding.passes());
                }
                continue;
            }

            if (observation.nodes().get(node) == null
                || observation.nodes().get(node).isAssigned(tenant, indexUuid, shard) == false) {
                // The node holds a copy its assignment does not name, so there is nothing here to withdraw: the
                // document already says "do not hold this". Planning a removal anyway would write no change and
                // report that a copy had been dropped, which is the kind of log line that costs an hour later.
                refusals.put(finding.refusalKey(), "node [" + node.value() + "] holds this copy but its assignment"
                    + " does not name it, so this service has nothing left to withdraw; dropping it is the node's"
                    + " to do");
                continue;
            }

            removals.add(new Removal(tenant, indexUuid, shard, node, because(finding, indexUuid, deletion)));
        }

        return new Outcome(removals, refusals);
    }

    /**
     * A task asking for this index to be deleted, in any state.
     *
     * <p><b>Positive evidence, and the state of the task deliberately does not matter.</b> What is being looked
     * for is that a deletion was <em>requested</em> for this uuid, which is a fact about the control plane; a
     * task's state is bookkeeping about the supervisor's own progress, and a task given up on after five attempts
     * — which is precisely what a pool full of unreapable orphans produces — is no less evidence that the index
     * was deleted than a pending one.
     *
     * <p>This cannot mistake a live index for a deleted one, because a task naming an index that is still in
     * desired state raises no {@code ORPHANED_COPY} to act on in the first place.
     */
    private static TaskRecord deletionTaskFor(Observation.DesiredState desired, String indexUuid) {
        for (TaskRecord task : desired.tasks()) {
            if (task.type() == TaskType.DELETE_INDEX && indexUuid.equals(task.indexUuid())) {
                return task;
            }
        }
        return null;
    }

    private static String because(Finding finding, String indexUuid, TaskRecord deletion) {
        String evidence = deletion == null
            ? "absent from desired state for " + finding.passes() + " passes with no deletion task naming it"
            : "absent from desired state and task [" + deletion.taskId() + "] asks for it to be deleted";
        return "index uuid " + indexUuid + " is " + evidence
            + ", so this copy is withdrawn; until it is, the deletion cannot finish and this node cannot report"
            + " an assignment as applied";
    }
}
