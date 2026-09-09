package org.opensearch.supervisor.model.node;

import org.opensearch.supervisor.model.NodeId;
import org.opensearch.supervisor.model.TenantId;

import java.util.Map;
import java.util.Objects;

/**
 * {@code nodes/shards/<node-id>} — ground truth for actual state: what this node <b>has</b>, not what it
 * was told.
 *
 * <p>Written only when the inventory changes, which is what keeps it off the every-few-seconds write path.
 * Keyed by index uuid rather than name throughout, because {@code KEY_LAYOUT.md} is explicit that names are
 * reused and a reconciler keyed on one will confuse an index with its deleted predecessor.
 */
public record ShardInventory(
    NodeId nodeId,
    String ephemeralId,
    long generatedAt,
    Map<TenantId, TenantShards> tenants
) {

    public ShardInventory {
        Objects.requireNonNull(nodeId, "nodeId");
        Objects.requireNonNull(ephemeralId, "ephemeralId");
        tenants = tenants == null ? Map.of() : Map.copyOf(tenants);
    }

    public static ShardInventory empty(NodeId nodeId, String ephemeralId, long generatedAt) {
        return new ShardInventory(nodeId, ephemeralId, generatedAt, Map.of());
    }

    /** Total copies across every tenant, which is what the heartbeat reports as {@code shard_copies}. */
    public int copyCount() {
        int total = 0;
        for (TenantShards tenant : tenants.values()) {
            for (IndexShards index : tenant.indices().values()) {
                total += index.shards().size();
            }
        }
        return total;
    }

    public record TenantShards(Map<String, IndexShards> indices) {
        public TenantShards {
            indices = indices == null ? Map.of() : Map.copyOf(indices);
        }
    }

    /**
     * @param name the index name, for humans. The uuid is the identity.
     * @param configRevision the store revision of the {@code tenant/<t>/indices/<name>} document this copy
     *     was built from. Revisions are opaque and <b>comparable only for equality</b> — neither side may
     *     order them.
     */
    public record IndexShards(String name, long configRevision, Map<Integer, ShardCopy> shards) {
        public IndexShards {
            shards = shards == null ? Map.of() : Map.copyOf(shards);
        }
    }

    /**
     * @param sizeBytes what a disk-aware decider needs; {@code null} while initializing
     * @param reason why a {@code FAILED} copy failed, bounded like the heartbeat's detail
     */
    public record ShardCopy(ShardState state, Long sizeBytes, String reason) {

        public static final int MAX_REASON_LENGTH = 512;

        public ShardCopy {
            Objects.requireNonNull(state, "state");
            if (reason != null && reason.length() > MAX_REASON_LENGTH) {
                reason = reason.substring(0, MAX_REASON_LENGTH);
            }
        }

        public static ShardCopy of(ShardState state) {
            return new ShardCopy(state, null, null);
        }

        public static ShardCopy of(ShardState state, long sizeBytes) {
            return new ShardCopy(state, sizeBytes, null);
        }
    }
}
