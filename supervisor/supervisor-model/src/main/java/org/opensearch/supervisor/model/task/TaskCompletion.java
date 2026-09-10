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

    /** Why a task is or is not finished — carried into the record, or into a finding. */
    public record Verdict(boolean satisfied, String because) {
        public static Verdict done(String because) {
            return new Verdict(true, because);
        }

        public static Verdict notYet(String because) {
            return new Verdict(false, because);
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

            // Not completed and not failed. Completing it would report success for work this build cannot
            // even name; failing it would blame the task for this supervisor's age.
            case UNKNOWN -> Verdict.notYet(
                "task type is not recognised by this build, so whether it is done cannot be decided here"
            );
        };
    }

    private static Optional<IndexConfig> indexOf(TaskRecord task, TenantId tenant, Observation observation) {
        Observation.DesiredState desired = observation.tenants().get(tenant);
        if (desired == null || task.indexUuid() == null) {
            return Optional.empty();
        }
        return Optional.ofNullable(desired.indicesByUuid().get(task.indexUuid()));
    }

    /**
     * Every shard has a copy somewhere, or has been told to have one.
     *
     * <p>Assignments count, not only inventories. A create-index task should complete once the supervisor has
     * placed the shards, not once every node has finished recovering them — otherwise a slow recovery holds a
     * task open for as long as it takes, and the queue stops being a queue.
     */
    private static Verdict allShardsAccountedFor(IndexConfig index, TenantId tenant, Observation observation) {
        for (int shard = 0; shard < index.numberOfShards(); shard++) {
            if (accountedFor(tenant, index.uuid(), shard, observation) == false) {
                return Verdict.notYet(
                    "index [" + index.name() + "] shard " + shard + " has neither a copy nor an assignment"
                        + " anywhere in this pool yet"
                );
            }
        }
        return Verdict.done(
            "every shard of [" + index.name() + "] is placed or assigned; recovery finishing is the node's"
                + " business and is already visible in the diff"
        );
    }

    private static boolean accountedFor(
        TenantId tenant,
        String indexUuid,
        int shard,
        Observation observation
    ) {
        for (Observation.NodeObservation node : observation.nodes().values()) {
            if (node.copiesCount() == false) {
                continue;
            }
            if (node.isAssigned(tenant, indexUuid, shard)) {
                return true;
            }
            ShardInventory inventory = node.inventory();
            if (inventory == null) {
                continue;
            }
            var tenantShards = inventory.tenants().get(tenant);
            if (tenantShards == null) {
                continue;
            }
            var held = tenantShards.indices().get(indexUuid);
            if (held != null && held.shards().containsKey(shard)) {
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
