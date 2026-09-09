package org.opensearch.supervisor.model.status;

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
    Map<Integer, List<ShardPlacement>> shards,
    long observedAt,
    long observedPass
) {

    public IndexStatus {
        Objects.requireNonNull(indexUuid, "indexUuid");
        shards = shards == null ? Map.of() : Map.copyOf(shards);
    }

    /** Where one copy of one shard is, and how it is getting on. */
    public record ShardPlacement(NodeId nodeId, ShardRole role, ShardState state) {
        public ShardPlacement {
            Objects.requireNonNull(nodeId, "nodeId");
            Objects.requireNonNull(role, "role");
            Objects.requireNonNull(state, "state");
        }

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
