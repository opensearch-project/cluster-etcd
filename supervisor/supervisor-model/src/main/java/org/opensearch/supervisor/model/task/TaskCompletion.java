package org.opensearch.supervisor.model.task;

import org.opensearch.supervisor.model.TenantId;
import org.opensearch.supervisor.model.index.IndexConfig;
import org.opensearch.supervisor.model.node.ShardInventory;
import org.opensearch.supervisor.model.observe.Observation;

import java.util.Optional;

/**
 * Whether a task's work is done, decided from the observation rather than from anything the task itself says.
 *
 * <p>That direction matters. A task carries no information the index documents do not — R2.1 — so completion
 * cannot mean "the supervisor did something in response to this record". It means <b>the state the record
 * describes has been reached</b>, which is a question about the world and is therefore answerable without any
 * memory of having acted. A task that arrives twice completes twice, harmlessly; one that never arrives leaves
 * the same work to be found by the ordinary diff.
 *
 * <p>The recurring case is the one worth stating: <b>a task whose subject no longer exists is complete, not
 * failed.</b> A {@code create-index} task for an index that has since been deleted describes work nobody
 * wants any more. Failing it would leave a permanent record of something that never went wrong; completing it
 * drains the queue, which is the whole point of the exercise.
 */
public final class TaskCompletion {

    /**
     * Why a task is or is not finished — carried into the record, or into a finding.
     *
     * @param progress a monotonic measure of how far the work has got, compared against the value stored on
     *     the record to tell "slow" from "stuck". Without it, a task whose shards take longer to recover than
     *     the attempt budget allows would be marked failed for making steady progress — blaming the task for
     *     the size of the index. Meaningless in absolute terms; only its direction is used.
     */
    public record Verdict(boolean satisfied, String because, int progress) {
        public static Verdict done(String because) {
            return new Verdict(true, because, Integer.MAX_VALUE);
        }

        public static Verdict notYet(String because) {
            return new Verdict(false, because, 0);
        }

        public static Verdict notYet(String because, int progress) {
            return new Verdict(false, because, progress);
        }
    }

    private TaskCompletion() {}

    public static Verdict evaluate(TaskRecord task, TenantId tenant, Observation observation) {
        Optional<IndexConfig> index = indexOf(task, tenant, observation);

        return switch (task.type()) {
            case CREATE_INDEX, OPEN_INDEX -> index
                .map(config -> config.open() ? allShardsAccountedFor(config, tenant, observation) : Verdict.done(
                    "index [" + config.name() + "] is closed in desired state, so there is nothing open to do"
                ))
                .orElseGet(() -> Verdict.done(
                    "index uuid " + task.indexUuid() + " is no longer in desired state, so there is nothing"
                        + " to create; the work is not outstanding, it is unwanted"
                ));

            case DELETE_INDEX -> index
                .<Verdict>map(config -> Verdict.notYet(
                    "index [" + config.name() + "] is still in desired state, so the control plane has not"
                        + " finished removing it"
                ))
                .orElseGet(() -> noCopiesRemain(task, tenant, observation));

            case CLOSE_INDEX -> index
                .map(config -> config.open()
                    ? Verdict.notYet("index [" + config.name() + "] is still open in desired state")
                    : noCopiesRemain(task, tenant, observation))
                .orElseGet(() -> Verdict.done("index uuid " + task.indexUuid() + " is gone, which closes it"));

            case UPDATE_MAPPING, UPDATE_SETTINGS -> index
                .map(config -> noCopyIsStale(config, tenant, observation))
                .orElseGet(() -> Verdict.done(
                    "index uuid " + task.indexUuid() + " is no longer in desired state, so its configuration"
                        + " cannot be out of date"
                ));

            // Pausing is a metadata change and nothing more: whether a stream is consuming lives in the index
            // document, so it reaches a node exactly as a mapping does and is done when every copy is running the
            // current configuration. Same condition as UPDATE_SETTINGS, for the same reason.
            case PAUSE_INGESTION -> index
                .map(config -> noCopyIsStale(config, tenant, observation))
                .orElseGet(() -> Verdict.done(
                    "index uuid " + task.indexUuid() + " is no longer in desired state, so its consumer cannot"
                        + " be running"
                ));

            case RESUME_INGESTION -> index
                .map(config -> resumeVerdict(task, config, tenant, observation))
                .orElseGet(() -> Verdict.done(
                    "index uuid " + task.indexUuid() + " is no longer in desired state, so there is no consumer"
                        + " to resume"
                ));

            // Not completed and not failed. Completing it would report success for work this build cannot
            // even name; failing it would blame the task for this supervisor's age.
            case UNKNOWN -> Verdict.notYet(
                "task type is not recognised by this build, so whether it is done cannot be decided here"
            );
        };
    }

    /**
     * A resume is a metadata change plus, sometimes, a one-shot instruction — and the second is why this is not
     * simply {@code noCopyIsStale}.
     *
     * <p>A resume may carry <b>consumer-pointer resets</b>: per shard, move the consumer to this offset or
     * timestamp. That is not desired state. Desired state is a value a reconciler can keep re-applying for ever,
     * and re-applying "start from offset 1000" for ever means a stream that never advances. So the reset rides on
     * the task, and the supervisor has to hand it over exactly once and then stop.
     *
     * <p>Which is not implemented yet, and the honest thing is to say so rather than complete the task. Completing
     * it would report that a pointer had been moved when nothing had moved it — and the failure would be silent,
     * because a consumer left where it was looks exactly like one that was reset and then caught up. That
     * indistinguishability is the whole difficulty; {@code DESIGN.md} §21 has the design, which turns on
     * acknowledging the <em>instruction</em> rather than observing the effect.
     */
    private static Verdict resumeVerdict(
        TaskRecord task,
        IndexConfig config,
        TenantId tenant,
        Observation observation
    ) {
        if (task.details() == null || task.details().hasResets() == false) {
            return noCopyIsStale(config, tenant, observation);
        }
        return Verdict.notYet(
            "index [" + config.name() + "] resumes with " + task.details().resetSettings().size()
                + " consumer-pointer reset(s), and handing a reset to a node is not implemented (DESIGN.md §21)."
                + " The metadata half may well have landed; this task stays open because completing it would"
                + " report a pointer as moved when nothing moved it, and a consumer left alone is"
                + " indistinguishable afterwards from one that was reset and caught up"
        );
    }

    private static Optional<IndexConfig> indexOf(TaskRecord task, TenantId tenant, Observation observation) {
        Observation.DesiredState desired = observation.tenants().get(tenant);
        if (desired == null || task.indexUuid() == null) {
            return Optional.empty();
        }
        return Optional.ofNullable(desired.indicesByUuid().get(task.indexUuid()));
    }

    /**
     * Every shard of the index has a primary that is actually serving.
     *
     * <p><b>Convergence, not "the supervisor has done its part."</b> An earlier version completed as soon as
     * every shard was placed or assigned, on the grounds that a slow recovery would otherwise hold a task open
     * for as long as it takes. That was optimising for the queue draining over the queue telling the truth: an
     * index whose shards are assigned but not yet recovered is not usable, so a create-index task for it is
     * not done. A task that stays open while a large index recovers is the queue being accurate.
     *
     * <p>The hazard that creates is handled by {@code progress} rather than by weakening the definition:
     * a task making steady progress renews its claim instead of spending an attempt.
     *
     * <p><b>Replicas are deliberately excluded, and this is the line to move.</b> Requiring every desired
     * copy would be the stronger and more obviously correct reading — but the planner cannot place replicas
     * yet (milestone 5 acts on primaries only), so every create-index task for an index with replicas would
     * fail, blaming the task for an unimplemented feature. Missing replicas are already reported as
     * {@code UNASSIGNED_REPLICA}, so nothing is hidden. When replica placement lands, this predicate should
     * tighten to the full desired set and this paragraph should go.
     */
    private static Verdict allShardsAccountedFor(IndexConfig index, TenantId tenant, Observation observation) {
        int serving = 0;
        int firstMissing = -1;
        for (int shard = 0; shard < index.numberOfShards(); shard++) {
            if (hasServingPrimary(tenant, index.uuid(), shard, observation)) {
                serving++;
            } else if (firstMissing < 0) {
                firstMissing = shard;
            }
        }

        if (serving == index.numberOfShards()) {
            return Verdict.done(
                "every shard of [" + index.name() + "] has a serving primary, so the index is usable"
            );
        }
        return Verdict.notYet(
            "index [" + index.name() + "] has " + serving + " of " + index.numberOfShards()
                + " shards serving; shard " + firstMissing + " has no started primary yet",
            serving
        );
    }

    private static boolean hasServingPrimary(
        TenantId tenant,
        String indexUuid,
        int shard,
        Observation observation
    ) {
        for (Observation.NodeObservation node : observation.nodes().values()) {
            if (node.copiesCount() == false || node.inventory() == null) {
                continue;
            }
            var tenantShards = node.inventory().tenants().get(tenant);
            if (tenantShards == null) {
                continue;
            }
            var held = tenantShards.indices().get(indexUuid);
            if (held == null) {
                continue;
            }
            var copy = held.shards().get(shard);
            if (copy != null && copy.isActivePrimary()) {
                return true;
            }
        }
        return false;
    }

    private static Verdict noCopiesRemain(TaskRecord task, TenantId tenant, Observation observation) {
        for (Observation.NodeObservation node : observation.nodes().values()) {
            if (node.copiesCount() == false) {
                continue;
            }
            if (node.isAssigned(tenant, task.indexUuid(), 0) || holdsAnything(node, tenant, task.indexUuid())) {
                return Verdict.notYet(
                    "node [" + node.nodeId() + "] still holds or is assigned a copy of index uuid "
                        + task.indexUuid()
                );
            }
        }
        return Verdict.done("no node in this pool holds or is assigned a copy of index uuid " + task.indexUuid());
    }

    private static boolean holdsAnything(Observation.NodeObservation node, TenantId tenant, String indexUuid) {
        ShardInventory inventory = node.inventory();
        if (inventory == null) {
            return false;
        }
        var tenantShards = inventory.tenants().get(tenant);
        if (tenantShards == null) {
            return false;
        }
        var index = tenantShards.indices().get(indexUuid);
        return index != null && index.shards().isEmpty() == false;
    }

    /**
     * No copy was built against a different revision of the index document than the current one.
     *
     * <p>Revisions are opaque and compare only for equality, which is all this needs: "the same" or "not the
     * same" is the whole question, and neither side ever orders them.
     */
    private static Verdict noCopyIsStale(IndexConfig index, TenantId tenant, Observation observation) {
        for (Observation.NodeObservation node : observation.nodes().values()) {
            if (node.copiesCount() == false || node.inventory() == null) {
                continue;
            }
            var tenantShards = node.inventory().tenants().get(tenant);
            if (tenantShards == null) {
                continue;
            }
            var held = tenantShards.indices().get(index.uuid());
            if (held != null && held.configRevision() != index.configRevision()) {
                return Verdict.notYet(
                    "node [" + node.nodeId() + "] still reports [" + index.name() + "] at config revision "
                        + held.configRevision() + " where desired state is at " + index.configRevision()
                );
            }
        }
        return Verdict.done(
            "every copy of [" + index.name() + "] reports the current config revision "
                + index.configRevision()
        );
    }
}
