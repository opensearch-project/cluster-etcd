package org.opensearch.supervisor.fakenode;

import org.opensearch.controlplane.store.KeyValueStore;
import org.opensearch.supervisor.model.Keys;
import org.opensearch.supervisor.model.NodeId;
import org.opensearch.supervisor.model.TenantId;
import org.opensearch.supervisor.model.node.ShardRole;
import org.opensearch.supervisor.model.status.IndexStatus;
import org.opensearch.supervisor.model.status.NodeStatus;
import org.opensearch.supervisor.model.status.Replication;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * Turns "I hold shard 1 of this index as a REPLICA" into "and here is the address of the node I must
 * replicate from", for document replication only.
 *
 * <p>The assignment deliberately carries no peer information — {@code NODE_CONTRACT.md} explains why — so a
 * node resolves peers from two supervisor-written projections it can already read:
 * {@code tenant/<t>/status/indices/<uuid>} says which node holds which copy, and
 * {@code tenant/<t>/status/nodes/<id>} says where that node is.
 *
 * <p>Reading a supervisor projection rather than another node's heartbeat is the deliberate difference from
 * {@code ../cluster-etcd}: those documents have one writer and a schema meant to be read, where a
 * heartbeat's author never has to think about a second class of reader.
 *
 * <p><b>Not finding a peer is normal.</b> During recovery the primary is placed in the same pass as the
 * replica and is not serving yet, so {@link Unresolved} is a "not yet" that the caller retries next tick,
 * never a failure.
 */
public final class PeerResolver {

    private final KeyValueStore store;

    public PeerResolver(KeyValueStore store) {
        this.store = Objects.requireNonNull(store, "store");
    }

    /** A peer this node must talk to, with enough detail to actually reach it. */
    public record Peer(NodeId nodeId, ShardRole role, String address, int port) {}

    /** Why no peers could be resolved. All of these are ordinary states during convergence. */
    public enum Unresolved {
        /** Segment replication: copies pull from the remote store and have no peers by design. */
        NOT_REQUIRED,
        /** The supervisor has not projected this index yet. */
        NO_PROJECTION,
        /** No primary is serving yet. The caller stays INITIALIZING and asks again. */
        NO_SERVING_PRIMARY,
        /** A peer is named but its node document is missing or says the node is gone. */
        PEER_UNREACHABLE
    }

    /** Either the peers to talk to, or the reason there are none yet. */
    public record Resolution(List<Peer> peers, Unresolved unresolved) {
        public static Resolution of(List<Peer> peers) {
            return new Resolution(List.copyOf(peers), null);
        }

        public static Resolution none(Unresolved reason) {
            return new Resolution(List.of(), reason);
        }

        public boolean isResolved() {
            return unresolved == null;
        }
    }

    /**
     * @param role the role <em>this</em> node holds. A replica resolves the one serving primary; a primary
     *     resolves the replicas it must fan writes out to.
     */
    public Resolution resolve(TenantId tenant, String indexUuid, int shard, ShardRole role) {
        Optional<IndexStatus> projection = store.get(Keys.indexStatus(tenant, indexUuid))
            .map(versioned -> IndexStatus.from(versioned.value()));
        if (projection.isEmpty()) {
            return Resolution.none(Unresolved.NO_PROJECTION);
        }
        IndexStatus status = projection.get();

        // Whether peers exist at all is a property of the index, not of the assignment.
        if (status.replication() != Replication.DOCUMENT) {
            return Resolution.none(Unresolved.NOT_REQUIRED);
        }

        List<IndexStatus.ShardPlacement> wanted = new ArrayList<>();
        if (role == ShardRole.PRIMARY) {
            wanted.addAll(status.replicasOf(shard));
        } else {
            Optional<IndexStatus.ShardPlacement> primary = status.servingPrimary(shard);
            if (primary.isEmpty()) {
                return Resolution.none(Unresolved.NO_SERVING_PRIMARY);
            }
            wanted.add(primary.get());
        }

        List<Peer> peers = new ArrayList<>();
        for (IndexStatus.ShardPlacement placement : wanted) {
            Optional<NodeStatus> node = store.get(Keys.nodeStatus(tenant, placement.nodeId()))
                .map(versioned -> NodeStatus.from(versioned.value()));
            if (node.isEmpty() || node.get().isReachable() == false) {
                // A primary we cannot reach is not a primary we can replicate from, so that is a hard "not
                // yet". A replica we cannot reach only costs the primary one fan-out target, so the rest
                // still resolve — a shard should not stop replicating because one of three replicas is down.
                if (role != ShardRole.PRIMARY) {
                    return Resolution.none(Unresolved.PEER_UNREACHABLE);
                }
                continue;
            }
            peers.add(new Peer(placement.nodeId(), placement.role(), node.get().address(), node.get().port()));
        }
        return Resolution.of(peers);
    }
}
