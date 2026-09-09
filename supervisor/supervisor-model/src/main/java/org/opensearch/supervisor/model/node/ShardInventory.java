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
     * One shard copy on this node.
     *
     * @param role what the copy is; @param state how it is getting on. Two dimensions, not one — a primary
     *     that is still initializing is a thing that exists, and health cannot be computed without being
     *     able to say so.
     * @param sizeBytes what a disk-aware decider needs; {@code null} while initializing
     * @param reason why a {@code FAILED} copy failed, bounded like the heartbeat's detail
     *
     * <p><b>Deliberately absent: OpenSearch's allocation id.</b> It is a real per-copy identity and it was
     *     tried here, because it distinguishes a copy from its own predecessor after a rebuild on the same
     *     node, which nothing else in this document can do. Measured cost: 135 bytes per copy instead of
     *     72, which is 37% of the inventory's size budget spent on a field the supervisor has no use for —
     *     it places copies, it does not track in-sync sets. The node needs allocation ids internally and
     *     has them on local disk; it does not need to publish them for the supervisor's benefit. Add it
     *     when something actually reads it, and raise the ceiling on copies-per-node at the same time.
     */
    public record ShardCopy(ShardRole role, ShardState state, Long sizeBytes, String reason) {

        public static final int MAX_REASON_LENGTH = 512;

        public ShardCopy {
            Objects.requireNonNull(role, "role");
            Objects.requireNonNull(state, "state");
            if (reason != null && reason.length() > MAX_REASON_LENGTH) {
                reason = reason.substring(0, MAX_REASON_LENGTH);
            }
        }

        /** Whether this copy is a started primary, which is what {@code active_primary_shards} counts. */
        public boolean isActivePrimary() {
            return role == ShardRole.PRIMARY && state == ShardState.STARTED;
        }

        public static ShardCopy of(ShardRole role, ShardState state) {
            return new ShardCopy(role, state, null, null);
        }

        public static ShardCopy of(ShardRole role, ShardState state, long sizeBytes) {
            return new ShardCopy(role, state, sizeBytes, null);
        }
    }
}
