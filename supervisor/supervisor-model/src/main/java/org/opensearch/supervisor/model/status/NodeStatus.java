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
 *
 * <p><b>The ephemeral id is here because an address is necessary and not sufficient.</b> Found by a real
 * coordinator failing to connect to a node it had correctly located: {@code DiscoveryNode.equals} compares
 * <em>only</em> the ephemeral id, and {@code TransportService}'s connection validator handshakes and then refuses
 * the connection unless the node it was handed equals the node the remote reports itself to be. So a reader that
 * knows where a peer is, but not which incarnation of it, builds a {@code DiscoveryNode} that is unequal to the
 * real one by construction and is refused. There is no workaround on the reading side — a fabricated ephemeral id
 * cannot match — which is what makes this the projection's problem.
 *
 * <p>It is the one fast-ish field in a document of slow-changing facts, and it belongs anyway: an ephemeral id
 * identifies an <em>incarnation</em> of a node, which is as slow-changing as the node's identity and changes on
 * exactly the same trigger as everything else here — a restart. It is not a licence to inline load or capacity,
 * which stay in {@code nodes/heartbeat/<id>}.
 */
public record NodeStatus(
    NodeId nodeId,
    String ephemeralId,
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
