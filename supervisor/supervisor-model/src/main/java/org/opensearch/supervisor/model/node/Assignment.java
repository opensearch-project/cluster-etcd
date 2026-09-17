package org.opensearch.supervisor.model.node;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.opensearch.supervisor.model.NodeId;
import org.opensearch.supervisor.model.PoolId;
import org.opensearch.supervisor.model.TenantId;

import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;

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
    /**
     * What this node does for one tenant.
     *
     * <p><b>A set, not a single role</b>, because both at once is a real configuration and the natural one for a
     * small deployment: OpenSearch's own out-of-the-box behaviour is a coordinator on every node, which is a poor
     * idea at scale and exactly right for three nodes on a laptop. A node holding copies of an index and
     * coordinating for its tenant gets one routing table containing its own copies and everybody else's, and it
     * could not be told to do that while this was one value.
     *
     * <p>Chosen over the alternative — letting {@code DATA} imply the capability, and having a node coordinate for
     * every tenant whose registration allows it — deliberately. That would make a node's behaviour depend partly on
     * its own configuration rather than wholly on the instruction written for it, which cuts against the grain of
     * everything else here: a node should be able to learn everything it does from its own assignment document.
     *
     * @param roles never empty. An entry that means nothing is an entry that should not exist, and the applier
     *     prunes one rather than writing it.
     */
    public record TenantAssignment(Set<NodeRole> roles, Map<String, IndexAssignment> indices) {
        public TenantAssignment {
            Objects.requireNonNull(roles, "roles");
            if (roles.isEmpty()) {
                throw new IllegalArgumentException(
                    "a tenant entry with no roles says nothing; drop the entry instead");
            }
            // Sorted, so that two supervisors writing the same desired state produce the same bytes and the
            // digest handshake in NODE_CONTRACT.md does not report a change that is only an ordering.
            roles = Collections.unmodifiableSet(new TreeSet<>(roles));
            indices = indices == null ? Map.of() : Map.copyOf(indices);
        }

        /**
         * Reads both the current {@code roles} array and the {@code role} string that preceded it.
         *
         * <p>Assignments outlive the process that wrote them — they sit in the store — so a supervisor that has
         * just been upgraded reads documents its predecessor wrote. Refusing them would report every node's
         * assignment as unreadable for one pass, which is survivable and needlessly alarming, and it would do it
         * during an upgrade, which is when nobody wants novel findings. Only the plural form is ever written, so
         * the tolerance costs one pass and then nothing.
         */
        @JsonCreator
        static TenantAssignment fromJson(
            @JsonProperty("roles") Set<NodeRole> roles,
            @JsonProperty("role") NodeRole legacyRole,
            @JsonProperty("indices") Map<String, IndexAssignment> indices
        ) {
            Set<NodeRole> resolved = roles != null && roles.isEmpty() == false
                ? roles
                : Set.of(legacyRole == null ? NodeRole.DATA : legacyRole);
            return new TenantAssignment(resolved, indices);
        }

        public static TenantAssignment coordinator() {
            return new TenantAssignment(Set.of(NodeRole.COORDINATOR), Map.of());
        }

        public static TenantAssignment data(Map<String, IndexAssignment> indices) {
            return new TenantAssignment(Set.of(NodeRole.DATA), indices);
        }

        public boolean holdsData() {
            return roles.contains(NodeRole.DATA);
        }

        public boolean coordinates() {
            return roles.contains(NodeRole.COORDINATOR);
        }

        /** The same entry with a role added, or itself if it already has it. */
        public TenantAssignment with(NodeRole role) {
            if (roles.contains(role)) {
                return this;
            }
            Set<NodeRole> next = new TreeSet<>(roles);
            next.add(role);
            return new TenantAssignment(next, indices);
        }

        /** The same entry without a role, or empty if that leaves it meaningless. */
        public Optional<TenantAssignment> without(NodeRole role) {
            if (roles.contains(role) == false) {
                return Optional.of(this);
            }
            Set<NodeRole> next = new TreeSet<>(roles);
            next.remove(role);
            return next.isEmpty() ? Optional.empty() : Optional.of(new TenantAssignment(next, indices));
        }

        public TenantAssignment withIndices(Map<String, IndexAssignment> replacement) {
            return new TenantAssignment(roles, replacement);
        }
    }

    /**
     * @param configRevision which revision of the index document to build against. If the node reads the
     *     index document and finds a different revision, this assignment is stale by one pass and it should
     *     apply what it read — the supervisor will re-issue.
     */
    /**
     * A one-shot instruction to move consumers, carried on the assignment because it is not desired state.
     *
     * <p>"Start from offset 1000" re-applied for ever is a stream that never advances, so this cannot live in the
     * index document. It rides here instead, identified by the id of the task that asked for it, and is removed once
     * acknowledged — see {@code DESIGN.md} §21.
     *
     * @param id the resume task's id. <b>The identity a node keys idempotence on</b>, never the assignment version:
     *     the version moves when an unrelated shard is placed in the same document, and re-applying a reset for that
     *     reason is the rewind-for-ever failure arriving by another route.
     * @param shards by shard number. Only shards this node holds the <em>primary</em> of ever appear — a search
     *     replica has no consumer of its own, and the data plane refuses one it was not made primary for rather
     *     than trusting that promise.
     */
    public record IngestionReset(String id, Map<Integer, ShardReset> shards) {

        public IngestionReset {
            Objects.requireNonNull(id, "id");
            shards = shards == null ? Map.of() : Map.copyOf(shards);
        }

        /**
         * Deliberately not called {@code isEmpty}. Jackson reads an {@code isX()} method on a record as another
         * property and wrote {@code "empty": false} into the assignment document — a field no contract mentions,
         * invented by a convenience method. Caught by serialising one and looking at the bytes.
         */
        public boolean hasNoShards() {
            return shards.isEmpty();
        }
    }

    /** Where one shard's consumer should be moved to. {@code value} is an offset, or epoch millis for a timestamp. */
    public record ShardReset(ResetMode mode, String value) {

        public ShardReset {
            Objects.requireNonNull(mode, "mode");
        }
    }

    /**
     * @param ingestionReset an outstanding consumer-pointer reset, or null. Absent on almost every assignment ever
     *     written, and a reader must treat it as optional.
     */
    public record IndexAssignment(
        String name,
        long configRevision,
        Map<Integer, ShardRole> shards,
        IngestionReset ingestionReset
    ) {

        public IndexAssignment {
            shards = shards == null ? Map.of() : Map.copyOf(shards);
            shards.forEach((shard, role) -> {
                if (role == null || role == ShardRole.UNKNOWN) {
                    throw new IllegalArgumentException(
                        "assignment for index [" + name + "] shard " + shard + " asks for role " + role
                            + "; a role this build cannot name is a role it cannot ask a node to take"
                    );
                }
            });
        }

        /** Without a reset, which is every assignment but the handful that carry one. */
        public IndexAssignment(String name, long configRevision, Map<Integer, ShardRole> shards) {
            this(name, configRevision, shards, null);
        }

        public IndexAssignment withIngestionReset(IngestionReset reset) {
            return new IndexAssignment(name, configRevision, shards, reset);
        }
    }

}
