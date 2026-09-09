package org.opensearch.supervisor.model.status;

import org.opensearch.supervisor.model.Json;
import org.opensearch.supervisor.model.NodeId;
import org.opensearch.supervisor.model.node.NodeRole;

import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * {@code tenant/<t>/status/nodes/<node-id>} — a tenant-scoped view of one node in that tenant's pool.
 *
 * <p>Only the slow-changing facts. No load or capacity figures, so that a node's disk filling up does not
 * rewrite the document; that data is in {@code nodes/heartbeat/<id>} for anyone entitled to read the root.
 *
 * <p>Two readers: {@code cluster.state}, and a document-replication data node turning a peer's node id from
 * {@link IndexStatus} into an address it can actually connect to.
 */
public record NodeStatus(
    NodeId nodeId,
    String name,
    Set<NodeRole> roles,
    String address,
    int port,
    String version,
    Map<String, String> attributes,
    Liveness liveness,
    long observedAt
) {

    public NodeStatus {
        Objects.requireNonNull(nodeId, "nodeId");
        roles = roles == null ? Set.of() : Set.copyOf(roles);
        attributes = attributes == null ? Map.of() : Map.copyOf(attributes);
    }

    /** Whether this node is worth trying to reach. A {@code LOST} peer is not a recovery source. */
    public boolean isReachable() {
        return liveness == Liveness.LIVE || liveness == Liveness.STALE;
    }

    public byte[] toBytes() {
        return Json.write(this, "node status");
    }

    public static NodeStatus from(byte[] bytes) {
        return Json.read(bytes, NodeStatus.class, "node status");
    }
}
