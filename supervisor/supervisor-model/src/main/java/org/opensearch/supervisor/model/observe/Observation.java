package org.opensearch.supervisor.model.observe;

import org.opensearch.supervisor.model.NodeId;
import org.opensearch.supervisor.model.PoolId;
import org.opensearch.supervisor.model.TenantId;
import org.opensearch.supervisor.model.index.IndexConfig;
import org.opensearch.supervisor.model.node.Assignment;
import org.opensearch.supervisor.model.node.Heartbeat;
import org.opensearch.supervisor.model.node.Registration;
import org.opensearch.supervisor.model.node.ShardInventory;
import org.opensearch.supervisor.model.settings.ClusterSettings;
import org.opensearch.supervisor.model.status.Liveness;
import org.opensearch.supervisor.model.task.TaskRecord;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * Everything one pass saw, as data.
 *
 * <p>{@code WORKING_NOTES.md} asks for this directly — "take observations as data, not as a store handle" —
 * and the sandbox makes it more than a preference: with no sockets and no containers, the only code
 * testable there is the code that touches nothing, and that happens to be the code carrying the risk. A
 * differ handed one of these cannot read a key it forgot to observe and cannot be non-deterministic.
 *
 * <p><b>An observation is not a snapshot.</b> The store has no cross-key transaction and {@code scanPrefix}
 * is not atomic on every backend, so these reads were taken at different instants and can disagree with
 * each other. {@code DESIGN.md} §12 audits what that can cost; the short version is that every field here
 * may be stale relative to every other, and the differ must never treat disagreement between two of them
 * as a fact worth destroying data over.
 */
public record Observation(
    long pass,
    Instant observedAt,
    PoolId pool,
    Map<TenantId, DesiredState> tenants,
    Map<NodeId, NodeObservation> nodes,
    List<String> unreadable
) {

    public Observation {
        Objects.requireNonNull(pool, "pool");
        Objects.requireNonNull(observedAt, "observedAt");
        tenants = tenants == null ? Map.of() : Map.copyOf(tenants);
        nodes = nodes == null ? Map.of() : Map.copyOf(nodes);
        unreadable = unreadable == null ? List.of() : List.copyOf(unreadable);
    }

    /** Nodes that can be placed on: registered, in this pool, and not presumed gone. */
    public Map<NodeId, NodeObservation> placeableNodes() {
        return nodes.entrySet()
            .stream()
            .filter(entry -> entry.getValue().isPlaceable())
            .collect(java.util.stream.Collectors.toUnmodifiableMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    /**
     * What the control plane says should exist for one tenant.
     *
     * @param tasks the tenant's work queue. Part of the observation because the queue lives in the tenant's
     *     own subtree and because the diff reports on it — but <b>never a source of intent</b>: the indices
     *     are what say what should exist, and R2.1 requires the same answer with this list empty.
     */
    public record DesiredState(
        Map<String, IndexConfig> indicesByUuid,
        List<TaskRecord> tasks,
        ClusterSettings settings
    ) {
        public DesiredState {
            indicesByUuid = indicesByUuid == null ? Map.of() : Map.copyOf(indicesByUuid);
            tasks = tasks == null ? List.of() : List.copyOf(tasks);
            settings = settings == null ? ClusterSettings.EMPTY : settings;
        }

        public DesiredState(Map<String, IndexConfig> indicesByUuid) {
            this(indicesByUuid, List.of(), ClusterSettings.EMPTY);
        }

        public DesiredState(Map<String, IndexConfig> indicesByUuid, List<TaskRecord> tasks) {
            this(indicesByUuid, tasks, ClusterSettings.EMPTY);
        }

        public static DesiredState empty() {
            return new DesiredState(Map.of(), List.of(), ClusterSettings.EMPTY);
        }
    }

    /**
     * One node's three documents plus the verdict on whether it is alive.
     *
     * @param liveness computed at observation time rather than inside the differ, because it depends on the
     *     supervisor's own memory of when it last saw this node's heartbeat key change revision. Keeping it
     *     out is what keeps the differ pure.
     * @param assignment what this node has most recently been <em>told</em> to hold, as distinct from the
     *     inventory, which is what it has. Observed for two reasons: an assignment is a complete desired set,
     *     so adding one shard means rewriting the rest, and a shard already named here must not be placed
     *     again while the node is still acting on it.
     * @param assignmentRevision the revision the assignment was read at, which every write
     *     compare-and-sets against. <b>Zero when there is no assignment yet</b>, which is the store's
     *     "must not exist" revision, so create-if-absent falls out of the same code path as an update. Not
     *     named as a constant here on purpose: this module must not reference the store, and the module graph
     *     check enforces that textually — see {@code DESIGN.md} §1.
     */
    public record NodeObservation(
        NodeId nodeId,
        Registration registration,
        Heartbeat heartbeat,
        ShardInventory inventory,
        Liveness liveness,
        PoolId pool,
        Assignment assignment,
        long assignmentRevision
    ) {

        public NodeObservation {
            Objects.requireNonNull(nodeId, "nodeId");
            liveness = liveness == null ? Liveness.UNKNOWN : liveness;
        }

        public Optional<Assignment> assignmentIfPresent() {
            return Optional.ofNullable(assignment);
        }

        /** Whether this node has already been told to hold a copy of a shard. */
        public boolean isAssigned(TenantId tenant, String indexUuid, int shard) {
            if (assignment == null) {
                return false;
            }
            var tenantAssignment = assignment.tenants().get(tenant);
            if (tenantAssignment == null) {
                return false;
            }
            var index = tenantAssignment.indices().get(indexUuid);
            return index != null && index.shards().containsKey(shard);
        }

        public Optional<Registration> registrationIfPresent() {
            return Optional.ofNullable(registration);
        }

        public Optional<ShardInventory> inventoryIfPresent() {
            return Optional.ofNullable(inventory);
        }

        /**
         * Whether new copies may be placed here.
         *
         * <p>Unregistered is deliberately not placeable but is <em>not</em> a reason to move anything off:
         * the node keeps what it has. A heartbeat without a registration is far more likely to be a torn
         * read than a broken node.
         */
        public boolean isPlaceable() {
            return registration != null && pool != null && liveness == Liveness.LIVE;
        }

        /**
         * Whether the copies this node reports should still be counted as existing.
         *
         * <p>{@code STALE} copies count. The gap between stale and lost is the system's tolerance for a GC
         * pause, and treating a paused node's shards as gone is how a reconciler turns a hiccup into a
         * cluster-wide rebuild.
         */
        public boolean copiesCount() {
            return liveness != Liveness.LOST;
        }

        /**
         * Whether an assignment written for this node is evidence that a copy exists or is on its way.
         *
         * <p><b>A distinct question from {@link #copiesCount()}, and conflating them deadlocked a real pool.</b>
         * An inventory is the node's own statement about what it has; an assignment is the <em>supervisor's</em>
         * statement about what it should have. The second is only evidence of a copy if the node is in a position
         * to be acting on it, and a node that is not reporting at all has told us nothing — it may never have seen
         * the instruction.
         *
         * <p>The bug this closes: {@code Differ} counts a copy only when a node reports one, while the planner also
         * counted a copy a node had merely been assigned. A node handed a primary that died before reporting it
         * therefore satisfied the planner and not the differ, so the shard was announced unassigned every pass and
         * skipped before the allocator ran — no placement, and not even a refusal to read. It showed up as a pool
         * that would not place a shard on its one live node, after enough data planes had been started and stopped
         * to leave a graveyard of them.
         *
         * <p>Requiring a heartbeat rather than {@code LIVE} on purpose. A node that has heartbeated and gone quiet
         * keeps its claim through the stale window, which is the tolerance for a GC pause that the whole liveness
         * ladder exists to provide — placing a second primary while the first may still be running is the one
         * mistake here that costs more than a pass. What does not survive is a node that never spoke at all.
         */
        public boolean actsOnItsAssignment() {
            return copiesCount() && heartbeat != null;
        }
    }
}
