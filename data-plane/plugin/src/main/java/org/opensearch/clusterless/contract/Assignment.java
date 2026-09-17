/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.clusterless.contract;

import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;

/**
 * {@code nodes/assignment/<node-id>} — this node's full desired state, written by the owner of its pool.
 *
 * <p><b>The complete desired set, never a delta.</b> A shard copy this node holds and this document does not
 * name must be removed. That is level-triggering pushed all the way down: a data plane applying deltas would
 * have the same edge-triggered bug as an event-driven supervisor, one layer lower and harder to see.
 *
 * <p>Three rules about reading it are worth having in one place, because each has a plausible wrong answer:
 *
 * <ul>
 *   <li><b>An absent assignment is not an empty one.</b> Absent means <em>no instruction</em>: keep what you
 *       hold and keep reporting the last version applied. A supervisor that has not written one yet, a pool
 *       between owners, and a lost key all look identical, and reading any of them as "drop everything" would
 *       empty a node. Only an assignment that is present and names nothing drops everything. This is why
 *       {@link #read} returns a value and the caller distinguishes absence — the type cannot express it.
 *   <li><b>Versions start at 1.</b> Zero is the heartbeat's "nothing applied yet", so an assignment numbered
 *       zero would be indistinguishable from never having been given one, and is rejected.
 *   <li><b>{@code pool} and {@code epoch} are ignored.</b> They exist so that an assignment written by a
 *       supervisor which had already lost the pool is identifiable after the fact. That is the supervisor's
 *       business; a node acting on them would be second-guessing which supervisor owns it, which is exactly
 *       the decision it must not make.
 * </ul>
 *
 * @param version monotonic per node, meaningless across nodes, echoed back on the heartbeat
 */
public record Assignment(long version, long generatedAt, Map<TenantId, TenantAssignment> tenants) {

    /** Well clear of ZooKeeper's 1 MiB default and etcd's 1.5 MiB. */
    public static final int BUDGET_BYTES = 512 * 1024;

    public Assignment {
        tenants = tenants == null ? Map.of() : Collections.unmodifiableMap(new TreeMap<>(tenants));
        if (version < 1) {
            throw new IllegalArgumentException(
                "assignment version must start at 1, was "
                    + version
                    + "; 0 is reserved for \"nothing applied yet\" in the heartbeat"
            );
        }
    }

    /**
     * What one tenant asks of this node.
     *
     * <p><b>A set, not one role.</b> It was one, and one cannot say the thing a small deployment most wants to
     * say: that a node holds copies of a tenant's indices <em>and</em> serves queries for it. Both at once is a
     * real configuration — OpenSearch's own default is that every node can coordinate — and it is the only one
     * testable on two or three nodes, which is why the data plane's {@code CoordinatorState} decorates the data
     * path rather than replacing it.
     *
     * @param roles never empty. An entry that asks for nothing is an entry that should not exist, and treating it
     *     as "no data and no queries" would silently drop a tenant the supervisor meant to say something about.
     */
    public record TenantAssignment(Set<NodeRole> roles, Map<String, IndexAssignment> indices) {

        public TenantAssignment {
            Objects.requireNonNull(roles, "roles");
            roles = roles.isEmpty() ? Set.of(NodeRole.UNKNOWN) : Collections.unmodifiableSet(EnumSet.copyOf(roles));
            indices = indices == null ? Map.of() : Collections.unmodifiableMap(new TreeMap<>(indices));
        }

        /** Whether this node is to hold copies of this tenant's indices. */
        public boolean holdsData() {
            return roles.contains(NodeRole.DATA);
        }

        /** Whether this node is to serve queries for this tenant, whether or not it also holds data. */
        public boolean coordinates() {
            return roles.contains(NodeRole.COORDINATOR);
        }

        /** Whether this build understood any of what it was asked to be. */
        public boolean understood() {
            return holdsData() || coordinates();
        }
    }

    /**
     * One index's shards, keyed by shard number.
     *
     * @param name the index's name within its tenant, which is how the index document is keyed
     * @param configRevision which revision of that document to build against. <b>Opaque</b>: comparable only
     *     for equality. If the node reads a different revision, the assignment is stale by one pass and it
     *     should apply what it read — the supervisor will re-issue.
     * @param reset a consumer-pointer reset to carry out once, or null. The one instruction in a document that
     *     is otherwise entirely desired state — see {@link IngestionReset} for why it could not live in the
     *     index document with the rest of the ingestion configuration.
     */
    public record IndexAssignment(String name, long configRevision, Map<Integer, ShardRole> shards, IngestionReset reset) {

        public IndexAssignment {
            Objects.requireNonNull(name, "name");
            shards = shards == null ? Map.of() : Collections.unmodifiableMap(new TreeMap<>(shards));
        }

        /** An index with no reset outstanding, which is every index almost all of the time. */
        public IndexAssignment(String name, long configRevision, Map<Integer, ShardRole> shards) {
            this(name, configRevision, shards, null);
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
     * Tenants this node is to serve queries for, whether or not it also holds their data.
     *
     * <p>Not "without holding data", which is what this meant while a tenant entry carried one role. A node that
     * does both routes to its own copies and to everyone else's, from one routing table.
     */
    public List<TenantId> coordinatorTenants() {
        List<TenantId> coordinating = new ArrayList<>();
        tenants.forEach((tenant, assignment) -> {
            if (assignment.coordinates()) {
                coordinating.add(tenant);
            }
        });
        return List.copyOf(coordinating);
    }

    /**
     * Shard roles this build could not understand, as {@code tenant/index-uuid/shard} strings.
     *
     * <p>Kept rather than discarded so the node can say what it did not act on. An unknown role is left out of
     * the desired state — a copy whose role this node cannot classify is one it must not create, since it
     * cannot tell whether the supervisor meant something that takes writes — and reporting it is the
     * difference between a visible finding and a silently missing shard.
     */
    public List<String> unreadableRoles() {
        List<String> unreadable = new ArrayList<>();
        tenants.forEach((tenant, tenantAssignment) -> tenantAssignment.indices().forEach((uuid, index) -> {
            index.shards().forEach((shard, role) -> {
                if (role == ShardRole.UNKNOWN) {
                    unreadable.add(tenant + "/" + uuid + "/" + shard);
                }
            });
        }));
        return List.copyOf(unreadable);
    }

    // ---------------------------------------------------------------- reading

    /**
     * @throws MalformedDocumentException if the document cannot be understood. The caller must treat that as
     *     <em>no instruction</em> rather than as an empty assignment.
     */
    public static Assignment read(byte[] bytes) {
        String what = "assignment";
        if (bytes.length > BUDGET_BYTES) {
            // The supervisor is required to write nothing rather than truncate, so an oversized document
            // means something else has gone wrong. Refusing it is safer than parsing half a desired state.
            throw new MalformedDocumentException(what, "it is " + bytes.length + " bytes, over the budget of " + BUDGET_BYTES);
        }
        Map<String, Object> document = Documents.parse(bytes, what);

        long version = Documents.number(document, "version", 0, what);
        long generatedAt = Documents.number(document, "generated_at", 0, what);

        Map<TenantId, TenantAssignment> tenants = new LinkedHashMap<>();
        Documents.object(document, "tenants").forEach((tenantId, value) -> {
            if (value instanceof Map == false) {
                return;
            }
            @SuppressWarnings("unchecked")
            Map<String, Object> tenantDocument = (Map<String, Object>) value;
            tenants.put(tenantOf(tenantId, what), readTenant(tenantDocument, what));
        });

        try {
            return new Assignment(version, generatedAt, tenants);
        } catch (IllegalArgumentException e) {
            // The version rule. Rejected here rather than carried, because a caller that acted on version 0
            // would report an ack indistinguishable from never having been assigned anything.
            throw new MalformedDocumentException(what, e.getMessage());
        }
    }

    private static TenantId tenantOf(String value, String what) {
        try {
            return TenantId.of(value);
        } catch (IllegalArgumentException e) {
            throw new MalformedDocumentException(what, e.getMessage());
        }
    }

    private static TenantAssignment readTenant(Map<String, Object> document, String what) {
        Set<NodeRole> roles = rolesOf(document);

        Map<String, IndexAssignment> indices = new LinkedHashMap<>();
        Documents.object(document, "indices").forEach((uuid, value) -> {
            if (value instanceof Map == false) {
                return;
            }
            @SuppressWarnings("unchecked")
            Map<String, Object> indexDocument = (Map<String, Object>) value;
            indices.put(uuid, readIndex(indexDocument, what));
        });

        return new TenantAssignment(roles, indices);
    }

    /**
     * The roles a tenant entry asks for, from either spelling.
     *
     * <p>{@code roles} is the current one and {@code role} is what preceded it. Reading both is not politeness: a
     * supervisor and a data plane are deployed and upgraded separately, so for some window one is older than the
     * other — and a node that understood only the newer spelling treated every tenant of an older supervisor as
     * unintelligible. Which is exactly what happened: a coordinator read {@code roles: ["coordinator"]}, found no
     * {@code role}, and reported that the role was not one it understood while doing nothing at all.
     */
    private static Set<NodeRole> rolesOf(Map<String, Object> document) {
        Set<NodeRole> roles = EnumSet.noneOf(NodeRole.class);
        Object declared = document.get("roles");
        if (declared instanceof Iterable<?> values) {
            for (Object value : values) {
                roles.add(NodeRole.from(value == null ? null : String.valueOf(value)));
            }
        }
        if (roles.isEmpty()) {
            roles.add(NodeRole.from(Documents.string(document, "role", null)));
        }
        return roles;
    }

    private static IndexAssignment readIndex(Map<String, Object> document, String what) {
        String name = Documents.requireString(document, "name", what);
        long configRevision = Documents.number(document, "config_revision", 0, what);

        Map<Integer, ShardRole> shards = new TreeMap<>();
        Documents.object(document, "shards").forEach((shard, role) -> {
            int shardId;
            try {
                shardId = Integer.parseInt(shard);
            } catch (NumberFormatException e) {
                throw new MalformedDocumentException(what, "[" + shard + "] is not a shard number");
            }
            // An unrecognised role becomes the sentinel rather than failing the document. See ShardRole.
            shards.put(shardId, ShardRole.from(role == null ? null : String.valueOf(role)));
        });

        return new IndexAssignment(name, configRevision, shards, readReset(document, what));
    }

    /**
     * The reset an index entry carries, or null.
     *
     * <p>A malformed one is dropped rather than failing the document, on the same reasoning as every other
     * tolerant read here — one bad field must not cost a node the rest of its desired state. It is not
     * <em>silently</em> dropped: a reset with no shards, or with an unusable id, is a reset nothing can
     * acknowledge, and the supervisor's task stays incomplete with the reason that it is not being acted on.
     * That is the visible outcome. An unrecognised <em>mode</em> is different and is kept — see
     * {@link IngestionReset.Mode#UNKNOWN}, which this node reports rather than skips.
     */
    private static IngestionReset readReset(Map<String, Object> document, String what) {
        Map<String, Object> reset = Documents.object(document, "ingestion_reset");
        if (reset.isEmpty()) {
            return null;
        }
        String id = Documents.string(reset, "id", null);
        if (id == null || id.isEmpty()) {
            return null;
        }

        Map<Integer, IngestionReset.Pointer> shards = new TreeMap<>();
        Documents.object(reset, "shards").forEach((shard, value) -> {
            if (value instanceof Map == false) {
                return;
            }
            int shardId;
            try {
                shardId = Integer.parseInt(shard);
            } catch (NumberFormatException e) {
                throw new MalformedDocumentException(what, "[" + shard + "] is not a shard number");
            }
            @SuppressWarnings("unchecked")
            Map<String, Object> pointer = (Map<String, Object>) value;
            String pointerValue = Documents.string(pointer, "value", null);
            if (pointerValue == null) {
                // Nothing to move the consumer to. Kept out rather than defaulted, because every default here
                // is a position, and a position nobody asked for is the one thing worse than no reset at all.
                return;
            }
            shards.put(shardId, new IngestionReset.Pointer(IngestionReset.Mode.from(Documents.string(pointer, "mode", null)), pointerValue));
        });

        if (shards.isEmpty()) {
            return null;
        }
        return new IngestionReset(id, shards);
    }
}
