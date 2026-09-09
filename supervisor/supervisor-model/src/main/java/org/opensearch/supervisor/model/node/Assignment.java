package org.opensearch.supervisor.model.node;

import org.opensearch.supervisor.model.NodeId;
import org.opensearch.supervisor.model.PoolId;
import org.opensearch.supervisor.model.TenantId;

import java.util.Map;
import java.util.Objects;

/**
 * {@code nodes/assignment/<node-id>} — the node's full desired state, written by the owner of its pool.
 *
 * <p><b>The complete desired set, never a delta.</b> A shard copy the node holds and this document does not
 * name must be removed. That is R2.1 pushed all the way down: a data plane applying deltas would have the
 * same edge-triggered bug one layer lower, where it is harder to see.
 *
 * <p>The single writer is the owner of this node's pool, which is one supervisor by construction
 * ({@code DESIGN.md} §2). Nothing here is merged from several sources.
 *
 * @param version monotonic <b>per node</b>, and meaningless across nodes. Echoed back in the heartbeat.
 * @param pool the pool the writer believed this node was in
 * @param epoch that pool's lease epoch, so an assignment written by a supervisor which had already lost the
 *     pool is identifiable after the fact. The node ignores both this and {@code pool}.
 */
public record Assignment(
    long version,
    PoolId pool,
    long epoch,
    NodeId nodeId,
    long generatedAt,
    Map<TenantId, TenantAssignment> tenants
) {

    public Assignment {
        Objects.requireNonNull(nodeId, "nodeId");
        tenants = tenants == null ? Map.of() : Map.copyOf(tenants);
        if (version < 1) {
            throw new IllegalArgumentException(
                "assignment version must start at 1, was " + version
                    + "; 0 is reserved for \"nothing applied yet\" in the heartbeat"
            );
        }
    }

    /** Total copies this node is being asked to hold. */
    public int copyCount() {
        int total = 0;
        for (TenantAssignment tenant : tenants.values()) {
            for (IndexAssignment index : tenant.indices().values()) {
                total += index.shards().size();
            }
        }
        return total;
    }

    /**
     * @param role {@code COORDINATOR} with no indices means "serve queries for this tenant, hold no data".
     *     Where such a node gets its routing table is {@code tenant/<t>/status/indices/<uuid>}, not here —
     *     the table is identical for every coordinator of a tenant, so copying it into each assignment
     *     would store it N times and rewrite all N whenever one shard moved.
     */
    public record TenantAssignment(NodeRole role, Map<String, IndexAssignment> indices) {
        public TenantAssignment {
            Objects.requireNonNull(role, "role");
            indices = indices == null ? Map.of() : Map.copyOf(indices);
        }

        public static TenantAssignment coordinator() {
            return new TenantAssignment(NodeRole.COORDINATOR, Map.of());
        }
    }

    /**
     * @param configRevision which revision of the index document to build against. If the node reads the
     *     index document and finds a different revision, this assignment is stale by one pass and it should
     *     apply what it read — the supervisor will re-issue.
     */
    public record IndexAssignment(String name, long configRevision, Map<Integer, ShardState> shards) {
        public IndexAssignment {
            shards = shards == null ? Map.of() : Map.copyOf(shards);
            shards.forEach((shard, state) -> {
                if (state != ShardState.PRIMARY && state != ShardState.REPLICA) {
                    throw new IllegalArgumentException(
                        "assignment for index [" + name + "] shard " + shard + " asks for " + state
                            + "; only PRIMARY and REPLICA can be assigned. INITIALIZING and FAILED are things"
                            + " a node reports, not things it can be told to be"
                    );
                }
            });
        }
    }
}
