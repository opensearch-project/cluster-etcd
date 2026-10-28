package org.opensearch.supervisor.model.task;

import org.opensearch.supervisor.model.TenantId;
import org.opensearch.supervisor.model.index.IndexConfig;
import org.opensearch.supervisor.model.node.ShardInventory;
import org.opensearch.supervisor.model.observe.Observation;

import org.opensearch.supervisor.model.node.AppliedOutcome;
import org.opensearch.supervisor.model.node.Heartbeat;
import org.opensearch.supervisor.model.node.ShardRole;
import java.util.ArrayList;
import java.util.List;
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

            // Not noCopiesRemain: a closed index keeps its assignment entries, because withdrawing them would tell
            // the node to delete data that must survive a reopen (SurplusReaper leaves them dormant). So the entries
            // never go, and waiting for them failed every close of an index that had copies. A node reports a closed
            // index as holding no copies (NODE_CONTRACT.md), so that is what is waited for.
            case CLOSE_INDEX -> index
                .map(config -> config.open()
                    ? Verdict.notYet("index [" + config.name() + "] is still open in desired state")
                    : noCopiesReported(task, tenant, observation))
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

        // The metadata half first: a resume is an unpause as well as a reset, and a node still on an older index
        // revision has not seen the unpause however cleanly it acknowledged the reset.
        Verdict metadata = noCopyIsStale(config, tenant, observation);
        if (metadata.satisfied() == false) {
            return metadata;
        }

        List<String> waiting = new ArrayList<>();
        for (TaskRecord.IngestionReset reset : task.details().resetSettings()) {
            Optional<Observation.NodeObservation> holder =
                primaryHolder(config, reset.shard(), tenant, observation);
            if (holder.isEmpty()) {
                // Not completed and not failed. The shard's missing primary is a placement problem the diff is
                // already reporting; blaming the resume for it would hide both.
                waiting.add("shard " + reset.shard() + " has no primary to reset");
                continue;
            }
            if (acknowledged(holder.get(), task, config, tenant) == false) {
                waiting.add("shard " + reset.shard() + " is held by node [" + holder.get().nodeId()
                    + "], which reports " + describeApplied(holder.get(), task, config, tenant));
            }
        }

        if (waiting.isEmpty()) {
            return Verdict.done(
                "every shard named by this resume is held by a node reporting it applied the assignment carrying"
                    + " the reset, which is the only evidence available: a consumer that was reset starts advancing"
                    + " immediately, so its position cannot distinguish a reset that happened from one that did not"
            );
        }
        return Verdict.notYet(
            task.details().resetSettings().size() + " consumer-pointer reset(s) requested; "
                + waiting.size() + " not yet acknowledged: " + String.join("; ", waiting)
        );
    }

    /**
     * The node holding this shard's primary, by what it <em>reports</em> rather than what it was told.
     *
     * <p>An assignment naming a primary is the instruction; an inventory reporting one is the fact. A reset is
     * applied by a running poller, and only a node that has the copy has a poller.
     */
    private static Optional<Observation.NodeObservation> primaryHolder(
        IndexConfig config,
        int shard,
        TenantId tenant,
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
            var index = tenantShards.indices().get(config.uuid());
            if (index == null) {
                continue;
            }
            var copy = index.shards().get(shard);
            if (copy != null && copy.role() == ShardRole.PRIMARY) {
                return Optional.of(node);
            }
        }
        return Optional.empty();
    }

    /**
     * Whether this node has applied an assignment that <b>contained this reset</b>.
     *
     * <p>Not "is this node up to date", which is what the first version of this compared and which completed the
     * task before the reset had been written. The task queue runs after the applier but reads the observation taken
     * before it, so on the pass the reset is first written the store holds version N+1 while the observation still
     * shows N — and a node reporting N looked, by version alone, perfectly acknowledged.
     *
     * <p>The fix is to ask the precise question. The observed assignment either carries a reset with this task's id
     * or it does not; if it does and the node reports having applied <em>that</em> version, then the node applied a
     * document containing this instruction. Nothing about it depends on the store and the observation agreeing.
     *
     * <p>It also makes the id earn its keep on this side as well as the node's: an assignment rewritten for an
     * unrelated reason keeps the same reset id, so a node that skipped re-applying it — as the contract requires —
     * still acknowledges correctly at the newer version.
     *
     * <p>{@code APPLIED} is required rather than merely a matching version, because the contract has a node hold
     * {@code applying} while a reset is outstanding and report {@code failed} when one is refused. Without that the
     * version alone would say only that the node had read the document.
     */
    private static boolean acknowledged(
        Observation.NodeObservation node,
        TaskRecord task,
        IndexConfig config,
        TenantId tenant
    ) {
        if (node.assignment() == null || node.heartbeat() == null) {
            return false;
        }
        var tenantAssignment = node.assignment().tenants().get(tenant);
        if (tenantAssignment == null) {
            return false;
        }
        var index = tenantAssignment.indices().get(config.uuid());
        if (index == null
            || index.ingestionReset() == null
            || task.taskId().equals(index.ingestionReset().id()) == false) {
            // The reset this task asked for is not in the document this node was observed holding, so whatever it
            // has applied, it has not applied this.
            return false;
        }

        Heartbeat.Applied applied = node.heartbeat().applied();
        return applied != null
            && applied.outcome() == AppliedOutcome.APPLIED
            && applied.assignmentVersion() == node.assignment().version();
    }

    private static String describeApplied(
        Observation.NodeObservation node,
        TaskRecord task,
        IndexConfig config,
        TenantId tenant
    ) {
        var tenantAssignment = node.assignment() == null ? null : node.assignment().tenants().get(tenant);
        var index = tenantAssignment == null ? null : tenantAssignment.indices().get(config.uuid());
        if (index == null || index.ingestionReset() == null
            || task.taskId().equals(index.ingestionReset().id()) == false) {
            return "an assignment that does not carry this reset yet";
        }
        if (node.heartbeat() == null || node.heartbeat().applied() == null) {
            return "no applied state at all";
        }
        Heartbeat.Applied applied = node.heartbeat().applied();
        return "assignment version " + applied.assignmentVersion() + " as " + applied.outcome()
            + " against the observed version " + node.assignment().version() + " that carries it";
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
            if (node.isAssignedAnyShardOf(tenant, task.indexUuid()) || holdsAnything(node, tenant, task.indexUuid())) {
                return Verdict.notYet(
                    "node [" + node.nodeId() + "] still holds or is assigned a copy of index uuid "
                        + task.indexUuid()
                );
            }
        }
        return Verdict.done("no node in this pool holds or is assigned a copy of index uuid " + task.indexUuid());
    }

    /** No node reports holding a copy, whatever it is still assigned — which is how a closed index is reported. */
    private static Verdict noCopiesReported(TaskRecord task, TenantId tenant, Observation observation) {
        for (Observation.NodeObservation node : observation.nodes().values()) {
            if (node.copiesCount() && holdsAnything(node, tenant, task.indexUuid())) {
                return Verdict.notYet(
                    "node [" + node.nodeId() + "] still reports a copy of index uuid " + task.indexUuid()
                        + ", so it has not closed it yet"
                );
            }
        }
        return Verdict.done("no node in this pool reports a copy of index uuid " + task.indexUuid()
            + ", which is how a node reports an index closed");
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
