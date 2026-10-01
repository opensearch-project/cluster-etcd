package org.opensearch.supervisor.model.status;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.opensearch.supervisor.model.Json;
import org.opensearch.supervisor.model.NodeId;
import org.opensearch.supervisor.model.node.ShardRole;
import org.opensearch.supervisor.model.node.ShardState;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * {@code tenant/<t>/status/indices/<index-uuid>} — per-index health and shard placement.
 *
 * <p>The most-read document the supervisor writes, with four consumers: {@code cluster.health} per index,
 * {@code cluster.state}, coordinator routing, and a document-replication data node resolving the peers it
 * must replicate to or from. They get one document rather than four because it is one computation.
 *
 * <p><b>Every observed copy is listed, including copies that are not yet serving.</b> That is required
 * rather than incidental: a docrep replica reads this to find the primary it must replicate from, and a
 * projection filtered to serving copies would deadlock — the replica could never see the primary it is
 * waiting for.
 */
public record IndexStatus(
    String index,
    String indexUuid,
    long configRevision,
    HealthStatus status,
    Replication replication,
    int numberOfShards,
    int numberOfReplicas,
    int numberOfSearchReplicas,
    boolean ingestionPaused,
    Map<Integer, List<ShardPlacement>> shards,
    long observedAt,
    long observedPass
) {

    public IndexStatus {
        Objects.requireNonNull(indexUuid, "indexUuid");
        shards = shards == null ? Map.of() : Map.copyOf(shards);
    }

    /**
     * The counts {@code cluster.health} renders, <b>derived rather than stored</b>.
     *
     * <p>{@code SUPERVISOR_KEYS.md} shows them on the wire and the control plane reads them directly, but a
     * stored count is a count that can disagree with the placements it summarises — and a health document that
     * contradicts itself is worse than one that makes a reader do arithmetic. Annotated so they are written,
     * computed so they cannot drift, and ignored on the way back in.
     */
    @JsonProperty("active_shards")
    public int activeShards() {
        return (int) shards.values()
            .stream()
            .flatMap(List::stream)
            .filter(placement -> placement.state() == ShardState.STARTED)
            .count();
    }

    @JsonProperty("active_primary_shards")
    public int activePrimaryShards() {
        return (int) shards.values()
            .stream()
            .filter(placements -> placements.stream().anyMatch(
                placement -> placement.role() == ShardRole.PRIMARY && placement.state() == ShardState.STARTED))
            .count();
    }

    @JsonProperty("initializing_shards")
    public int initializingShards() {
        return (int) shards.values()
            .stream()
            .flatMap(List::stream)
            .filter(placement -> placement.state() == ShardState.INITIALIZING)
            .count();
    }

    /** Desired copies that exist nowhere. Zero for a closed index, which wants none. */
    @JsonProperty("unassigned_shards")
    public int unassignedShards() {
        // Every kind of copy the index asked for, counted per shard. Leaving search replicas out let an index report
        // green with a requested copy missing, and an index-wide total let a surplus copy of one shard cancel a
        // missing copy of another.
        int desiredPerShard = 1 + numberOfReplicas + numberOfSearchReplicas;
        return shards.values().stream().mapToInt(placements -> Math.max(0, desiredPerShard - placements.size())).sum();
    }

    /** Where one copy of one shard is, and how it is getting on. */
    public record ShardPlacement(NodeId nodeId, ShardRole role, ShardState state) {
        public ShardPlacement {
            Objects.requireNonNull(nodeId, "nodeId");
            Objects.requireNonNull(role, "role");
            Objects.requireNonNull(state, "state");
        }

        /**
         * Whether this copy can serve a request, on the wire as {@code serving}.
         *
         * <p><b>Annotated because it is load-bearing, not because it is convenient.</b> It began as an accident —
         * {@code isServing()} is named the way Jackson detects a property — and by the time anyone noticed, the
         * data plane's coordinator routing was already reading it. Removing it as an unintended field, which is
         * the correct fix for two others in this codebase, would have routed queries to nothing in another
         * repository with no test failing in this one.
         *
         * <p>Redundant with {@code state}, and kept anyway: which states serve is this service's rule, and making
         * every reader reimplement it is how two implementations of that rule come to disagree.
         */
        @JsonProperty("serving")
        public boolean isServing() {
            return state.isServing();
        }
    }

    /**
     * The serving primary of a shard, if there is one.
     *
     * <p>This is what a docrep replica waits for. Empty is the normal state during recovery and means "not
     * yet", never "never" — the caller stays {@code INITIALIZING} and asks again next pass.
     */
    public Optional<ShardPlacement> servingPrimary(int shard) {
        return shards.getOrDefault(shard, List.of())
            .stream()
            .filter(placement -> placement.role() == ShardRole.PRIMARY && placement.isServing())
            .findFirst();
    }

    /** The copies a docrep primary must fan writes out to: every non-primary copy of the shard. */
    public List<ShardPlacement> replicasOf(int shard) {
        return shards.getOrDefault(shard, List.of())
            .stream()
            .filter(placement -> placement.role() != ShardRole.PRIMARY)
            .toList();
    }

    public byte[] toBytes() {
        return Json.write(this, "index status");
    }

    public static IndexStatus from(byte[] bytes) {
        return Json.read(bytes, IndexStatus.class, "index status");
    }
}
