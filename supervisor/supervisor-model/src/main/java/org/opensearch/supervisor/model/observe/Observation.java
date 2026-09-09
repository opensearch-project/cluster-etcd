package org.opensearch.supervisor.model.observe;

import org.opensearch.supervisor.model.NodeId;
import org.opensearch.supervisor.model.PoolId;
import org.opensearch.supervisor.model.TenantId;
import org.opensearch.supervisor.model.index.IndexConfig;
import org.opensearch.supervisor.model.node.Heartbeat;
import org.opensearch.supervisor.model.node.Registration;
import org.opensearch.supervisor.model.node.ShardInventory;
import org.opensearch.supervisor.model.status.Liveness;

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

    /** What the control plane says should exist for one tenant. */
    public record DesiredState(Map<String, IndexConfig> indicesByUuid) {
        public DesiredState {
            indicesByUuid = indicesByUuid == null ? Map.of() : Map.copyOf(indicesByUuid);
        }

        public static DesiredState empty() {
            return new DesiredState(Map.of());
        }
    }

    /**
     * One node's three documents plus the verdict on whether it is alive.
     *
     * @param liveness computed at observation time rather than inside the differ, because it depends on the
     *     supervisor's own memory of when it last saw this node's heartbeat key change revision. Keeping it
     *     out is what keeps the differ pure.
     */
    public record NodeObservation(
        NodeId nodeId,
        Registration registration,
        Heartbeat heartbeat,
        ShardInventory inventory,
        Liveness liveness,
        PoolId pool
    ) {

        public NodeObservation {
            Objects.requireNonNull(nodeId, "nodeId");
            liveness = liveness == null ? Liveness.UNKNOWN : liveness;
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
    }
}
