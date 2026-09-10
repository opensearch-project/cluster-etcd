package org.opensearch.supervisor.model;

import java.util.Objects;

/**
 * Every key this service reads or writes, in one place.
 *
 * <p>{@code KEY_LAYOUT.md}, {@code NODE_CONTRACT.md} and {@code SUPERVISOR_KEYS.md} are contracts with two
 * other services, and a contract in prose drifts. This class is those documents made executable: if a key
 * is built here and nowhere else, then changing a layout is one edit and a failing test rather than a
 * search for string concatenation.
 *
 * <p>Two rules the layout depends on, both enforced by the identifier types rather than here:
 *
 * <ul>
 *   <li><b>No identifier may contain {@code '/'}.</b> Keys are built by concatenation, so an id with a
 *       slash in it would escape its subtree and land in someone else's — a tenant able to write
 *       {@code ../nodes/assignment/x} would be a tenant able to move another tenant's shards.
 *   <li><b>One key segment decides the writer.</b> Under {@code nodes/} it is the second segment; under
 *       {@code tenant/<t>/} it is whether the segment is {@code status}. Ownership has to be decidable
 *       from the key alone, which is why health lives at {@code tenant/<t>/status/cluster} and not at
 *       {@code tenant/<t>/cluster/health} next to the control plane's own {@code cluster/} keys.
 * </ul>
 *
 * <p>{@code scanPrefix} is specified over key prefixes, not path boundaries, so every prefix constant
 * here ends in {@code '/'} on purpose: {@code "tenant/default/i"} is a legal prefix that would match
 * {@code indices/} and {@code ingest/} together.
 */
public final class Keys {

    private Keys() {}

    // ---------------------------------------------------------------- the tenant registry

    /** Root registry of tenants that exist, written by the control plane on a tenant's first write. */
    public static final String TENANTS_PREFIX = "tenants/";

    public static String tenantRegistration(TenantId tenant) {
        return TENANTS_PREFIX + tenant.value();
    }

    // ---------------------------------------------------------------- the control plane's keys

    /** The subtree owned by one tenant. The control plane writes here; the supervisor mostly reads. */
    public static String tenantPrefix(TenantId tenant) {
        return "tenant/" + tenant.value() + "/";
    }

    public static String indicesPrefix(TenantId tenant) {
        return tenantPrefix(tenant) + "indices/";
    }

    public static String aliasesPrefix(TenantId tenant) {
        return tenantPrefix(tenant) + "aliases/";
    }

    public static String dataStreamsPrefix(TenantId tenant) {
        return tenantPrefix(tenant) + "data_streams/";
    }

    /**
     * Cluster-wide settings, written by {@code cluster.put_settings}. The supervisor reads one of them: the
     * allocation brake. Control-plane-owned, and never written here.
     */
    public static String clusterSettings(TenantId tenant) {
        return tenantPrefix(tenant) + "cluster/settings";
    }

    /**
     * The work queue. Jointly owned: the control plane creates records, the supervisor claims, completes
     * and reaps them, and the control plane never revisits one. See {@code SUPERVISOR_KEYS.md} §2 — this
     * is the one place the supervisor writes a key the control plane also writes, and it is deliberate.
     */
    public static String tasksPrefix(TenantId tenant) {
        return tenantPrefix(tenant) + "tasks/index-management/";
    }

    // ---------------------------------------------------------------- the supervisor's projections

    /**
     * The supervisor-owned subtree <em>inside</em> the tenant prefix.
     *
     * <p>It has to be inside it. The control plane's handlers see a store rooted at
     * {@code tenant/<id>/}, so a projection anywhere else is not merely undiscovered by them, it is
     * unreachable — which is why R5.5's "own prefixes only" is kept one level down instead of literally.
     */
    public static String statusPrefix(TenantId tenant) {
        return tenantPrefix(tenant) + "status/";
    }

    public static String clusterStatus(TenantId tenant) {
        return statusPrefix(tenant) + "cluster";
    }

    public static String indexStatusPrefix(TenantId tenant) {
        return statusPrefix(tenant) + "indices/";
    }

    /**
     * Per-index health and shard placement. Four readers: the control plane for {@code cluster.health}
     * and {@code cluster.state}, coordinators for routing, and docrep data nodes for peer resolution.
     */
    public static String indexStatus(TenantId tenant, String indexUuid) {
        return indexStatusPrefix(tenant) + require(indexUuid, "indexUuid");
    }

    public static String nodeStatusPrefix(TenantId tenant) {
        return statusPrefix(tenant) + "nodes/";
    }

    public static String nodeStatus(TenantId tenant, NodeId node) {
        return nodeStatusPrefix(tenant) + node.value();
    }

    public static String awarenessStatus(TenantId tenant) {
        return statusPrefix(tenant) + "awareness";
    }

    // ---------------------------------------------------------------- the data plane seam

    /**
     * Grouped by kind rather than by node, because the hot path is "read every heartbeat, read almost no
     * inventories" and {@code scanPrefix} cannot skip a middle segment. Under {@code nodes/<id>/heartbeat}
     * one scan of {@code nodes/} would return every shard inventory in the cluster on every pass.
     */
    public static final String NODE_REGISTRATION_PREFIX = "nodes/registration/";

    public static final String NODE_HEARTBEAT_PREFIX = "nodes/heartbeat/";

    public static final String NODE_SHARDS_PREFIX = "nodes/shards/";

    public static final String NODE_ASSIGNMENT_PREFIX = "nodes/assignment/";

    public static String nodeRegistration(NodeId node) {
        return NODE_REGISTRATION_PREFIX + node.value();
    }

    public static String nodeHeartbeat(NodeId node) {
        return NODE_HEARTBEAT_PREFIX + node.value();
    }

    public static String nodeShards(NodeId node) {
        return NODE_SHARDS_PREFIX + node.value();
    }

    /** Written by the owner of this node's pool, which is one supervisor by construction. */
    public static String nodeAssignment(NodeId node) {
        return NODE_ASSIGNMENT_PREFIX + node.value();
    }

    // ---------------------------------------------------------------- pools

    public static final String POOL_DEFINITIONS_PREFIX = "pools/definitions/";

    public static final String POOL_BINDINGS_PREFIX = "pools/bindings/";

    public static final String POOL_NODES_PREFIX = "pools/nodes/";

    public static String poolDefinition(PoolId pool) {
        return POOL_DEFINITIONS_PREFIX + pool.value();
    }

    /**
     * One key per tenant, and that is the point: rebinding a tenant to another pool is then a single
     * compare-and-set. A membership list inside the pool document would make a move two writes to two
     * keys with no transaction, and the torn state is "this tenant is in both pools" — two supervisors
     * placing its shards, which is the one thing the partition model exists to prevent.
     */
    public static String poolBinding(TenantId tenant) {
        return POOL_BINDINGS_PREFIX + tenant.value();
    }

    /** Operator override of the pool a node declared for itself. Absent for most nodes. */
    public static String poolNode(NodeId node) {
        return POOL_NODES_PREFIX + node.value();
    }

    // ---------------------------------------------------------------- supervisor bookkeeping

    public static final String MEMBERS_PREFIX = "supervisor/members/";

    public static final String POOL_LEASES_PREFIX = "supervisor/pools/";

    /** Chores that belong to no pool: unbound tenants, unpooled nodes, unowned pools, dead members. */
    public static final String HOUSEKEEPING_LEASE = "supervisor/leases/housekeeping";

    public static String member(String supervisorId) {
        return MEMBERS_PREFIX + require(supervisorId, "supervisorId");
    }

    /** The only mutual-exclusion primitive in the system. Everything else has one writer by construction. */
    public static String poolLease(PoolId pool) {
        return POOL_LEASES_PREFIX + pool.value() + "/lease";
    }

    // ---------------------------------------------------------------- observability

    public static String poolConvergence(PoolId pool) {
        return "supervisor/status/pools/" + pool.value() + "/convergence";
    }

    public static String poolOscillation(PoolId pool) {
        return "supervisor/status/pools/" + pool.value() + "/oscillation";
    }

    /**
     * Keyed by tenant rather than by pool on purpose: a rebind changes a tenant's pool, and an operator's
     * bookmark for "what is wrong with acme" should not move when it does.
     */
    public static String tenantDiff(TenantId tenant) {
        return "supervisor/status/diff/" + tenant.value();
    }

    public static String memberStatus(String supervisorId) {
        return "supervisor/status/members/" + require(supervisorId, "supervisorId");
    }

    public static final String HOUSEKEEPING_STATUS = "supervisor/status/housekeeping";

    // ---------------------------------------------------------------- helpers

    /**
     * Extracts the trailing identifier from a key returned by a prefix scan.
     *
     * @throws IllegalArgumentException if the key does not start with the prefix, which would mean the
     *     store returned something the scan did not ask for
     */
    public static String idAfter(String prefix, String key) {
        Objects.requireNonNull(prefix, "prefix");
        Objects.requireNonNull(key, "key");
        if (key.startsWith(prefix) == false) {
            throw new IllegalArgumentException("key [" + key + "] does not start with prefix [" + prefix + "]");
        }
        return key.substring(prefix.length());
    }

    private static String require(String value, String what) {
        Objects.requireNonNull(value, what);
        if (value.isEmpty()) {
            throw new IllegalArgumentException(what + " must not be empty");
        }
        if (value.indexOf('/') >= 0) {
            throw new IllegalArgumentException(what + " [" + value + "] must not contain '/'");
        }
        return value;
    }
}
