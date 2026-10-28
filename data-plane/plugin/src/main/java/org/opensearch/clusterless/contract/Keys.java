/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.clusterless.contract;

/**
 * Every key this plugin reads or writes, in one place.
 *
 * <p>{@code NODE_CONTRACT.md} and {@code KEY_LAYOUT.md} are contracts with two other services, and a
 * contract in prose drifts. Building a key here and nowhere else makes a layout change one edit and a
 * failing test rather than a search for string concatenation.
 *
 * <p>Two properties of the layout are worth restating where they can be seen:
 *
 * <ul>
 *   <li><b>Node documents are grouped by kind, not by node.</b> {@code nodes/heartbeat/<id>} rather than
 *       {@code nodes/<id>/heartbeat}, because the supervisor's hot path is "read every heartbeat, read
 *       almost no inventories" and {@code scanPrefix} cannot skip a middle segment. The cost lands here:
 *       a node cannot watch one prefix covering all of its own keys. It does not need to — the only key
 *       it reads is its own assignment.
 *   <li><b>The second segment under {@code nodes/} decides the writer.</b> This plugin writes
 *       {@code registration}, {@code heartbeat} and {@code shards} for its own node id, and nothing else
 *       anywhere. A violation is detectable by reading a key, which is the point of arranging it this way.
 * </ul>
 *
 * <p>No identifier may contain {@code '/'}, which the identifier types enforce rather than this class:
 * keys are built by concatenation, so an id with a slash would escape its subtree.
 */
public final class Keys {

    private Keys() {}

    // ---------------------------------------------------------------- what this node writes

    public static String nodeRegistration(NodeId node) {
        return "nodes/registration/" + node.value();
    }

    public static String nodeHeartbeat(NodeId node) {
        return "nodes/heartbeat/" + node.value();
    }

    public static String nodeShards(NodeId node) {
        return "nodes/shards/" + node.value();
    }

    // ---------------------------------------------------------------- what this node reads

    /**
     * Its own, and no other node's (D4.1). A node reading another node's assignment would be a second
     * reader of a single-writer document and would couple the two.
     */
    public static String nodeAssignment(NodeId node) {
        return "nodes/assignment/" + node.value();
    }

    /**
     * Index settings and mappings, written by the control plane. Keyed by <b>name</b> while everything
     * else is keyed by uuid, which is why the contract requires the reader to check the {@code uuid}
     * inside against the uuid its assignment named: an index deleted and recreated under the same name
     * gives a perfectly valid document describing a different index.
     */
    public static String indexConfig(TenantId tenant, String indexName) {
        return tenantPrefix(tenant) + "indices/" + require(indexName, "indexName");
    }

    /** Health and placement for one index. Four readers; this plugin is two of them. */
    public static String indexStatus(TenantId tenant, String indexUuid) {
        return tenantPrefix(tenant) + "status/indices/" + require(indexUuid, "indexUuid");
    }

    /**
     * Every index projection for a tenant, for a coordinator that has to route to all of them.
     *
     * <p>A prefix scan rather than a read per index, because a coordinator is told which tenants it serves and not
     * which indices they have — the assignment deliberately carries no index list for a coordinated tenant, since
     * that list is identical for every coordinator of the tenant and changes when any index is created.
     */
    public static String indexStatusPrefix(TenantId tenant) {
        return tenantPrefix(tenant) + "status/indices/";
    }

    /**
     * Where a peer node is. Addresses come from here and never from another node's heartbeat: names are
     * reused while ids are not, so a peer resolved by name eventually points at a node's successor.
     */
    public static String nodeStatus(TenantId tenant, NodeId node) {
        return tenantPrefix(tenant) + "status/nodes/" + node.value();
    }

    private static String tenantPrefix(TenantId tenant) {
        return "tenant/" + tenant.value() + "/";
    }

    private static String require(String value, String what) {
        if (value == null || value.isEmpty()) {
            throw new IllegalArgumentException(what + " must not be empty");
        }
        if (value.indexOf('/') >= 0) {
            throw new IllegalArgumentException(what + " [" + value + "] must not contain '/'");
        }
        return value;
    }
}
