package org.opensearch.supervisor.fakenode;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.opensearch.controlplane.store.InMemoryKeyValueStore;
import org.opensearch.supervisor.model.Keys;
import org.opensearch.supervisor.model.NodeId;
import org.opensearch.supervisor.model.TenantId;
import org.opensearch.supervisor.model.node.NodeRole;
import org.opensearch.supervisor.model.node.ShardRole;
import org.opensearch.supervisor.model.node.ShardState;
import org.opensearch.supervisor.model.status.HealthStatus;
import org.opensearch.supervisor.model.status.IndexStatus;
import org.opensearch.supervisor.model.status.Liveness;
import org.opensearch.supervisor.model.status.NodeStatus;
import org.opensearch.supervisor.model.status.Replication;

import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Whether the documents this system publishes actually contain enough for a document-replication node to
 * find its peers.
 *
 * <p>That is the question the design answers on paper — the assignment carries no peer fields, so the
 * status projections must be sufficient — and these are what stop it from being an assertion. The
 * projections are written here by hand because the supervisor does not write them until milestone 7; the
 * point is that the shapes are enough, not that anything produces them yet.
 */
class PeerResolverTests {

    private static final TenantId ACME = TenantId.of("acme");
    private static final String INDEX_UUID = "uzKxLWZu4nPSrTknEcDpoA";
    private static final NodeId PRIMARY_NODE = NodeId.of("pv1sDMfZRxSTMiW5VrbN9w");
    private static final NodeId REPLICA_NODE = NodeId.of("72ZSKe7GSBq2ZRvwHYw9zw");

    private InMemoryKeyValueStore store;
    private PeerResolver resolver;

    @BeforeEach
    void setUp() {
        store = new InMemoryKeyValueStore();
        resolver = new PeerResolver(store);
    }

    private void projectIndex(Replication replication, List<IndexStatus.ShardPlacement> shard0) {
        IndexStatus status = new IndexStatus(
            "my-index", INDEX_UUID, 3041, HealthStatus.YELLOW, replication,
            1, 1, 0, false, Map.of(0, shard0), 1788969977933L, 10432
        );
        store.put(Keys.indexStatus(ACME, INDEX_UUID), status.toBytes());
    }

    private void projectNode(NodeId nodeId, String address, Liveness liveness) {
        NodeStatus status = new NodeStatus(
            nodeId, "eph-" + nodeId.value(), nodeId.value(), Set.of(NodeRole.DATA), address, 9300, "3.7.0",
            Map.of("zone", "z1"), liveness, 1788969977933L
        );
        store.put(Keys.nodeStatus(ACME, nodeId), status.toBytes());
    }

    @Test
    void aReplicaFindsItsServingPrimary() {
        projectIndex(Replication.DOCUMENT, List.of(
            new IndexStatus.ShardPlacement(PRIMARY_NODE, ShardRole.PRIMARY, ShardState.STARTED),
            new IndexStatus.ShardPlacement(REPLICA_NODE, ShardRole.REPLICA, ShardState.INITIALIZING)
        ));
        projectNode(PRIMARY_NODE, "10.0.3.14", Liveness.LIVE);

        PeerResolver.Resolution resolution = resolver.resolve(ACME, INDEX_UUID, 0, ShardRole.REPLICA);

        assertTrue(resolution.isResolved());
        assertEquals(1, resolution.peers().size());
        PeerResolver.Peer primary = resolution.peers().get(0);
        assertEquals(PRIMARY_NODE, primary.nodeId());
        assertEquals("10.0.3.14", primary.address());
        assertEquals(9300, primary.port());
    }

    @Test
    void aPrimaryFindsTheReplicasItMustFanOutTo() {
        NodeId secondReplica = NodeId.of("zF5QLAUpQXqIwaNOD5vDKQ");
        projectIndex(Replication.DOCUMENT, List.of(
            new IndexStatus.ShardPlacement(PRIMARY_NODE, ShardRole.PRIMARY, ShardState.STARTED),
            new IndexStatus.ShardPlacement(REPLICA_NODE, ShardRole.REPLICA, ShardState.STARTED),
            new IndexStatus.ShardPlacement(secondReplica, ShardRole.REPLICA, ShardState.INITIALIZING)
        ));
        projectNode(REPLICA_NODE, "10.0.3.15", Liveness.LIVE);
        projectNode(secondReplica, "10.0.3.16", Liveness.LIVE);

        PeerResolver.Resolution resolution = resolver.resolve(ACME, INDEX_UUID, 0, ShardRole.PRIMARY);

        assertTrue(resolution.isResolved());
        assertEquals(2, resolution.peers().size(), "an initializing replica is still a fan-out target");
    }

    @Test
    void segmentReplicationResolvesNoPeersAtAll() {
        // The whole reason the assignment can carry a bare role string: a segrep copy pulls from the remote
        // store. Whether peers apply is a property of the index, not of the assignment.
        projectIndex(Replication.SEGMENT, List.of(
            new IndexStatus.ShardPlacement(PRIMARY_NODE, ShardRole.PRIMARY, ShardState.STARTED)
        ));

        PeerResolver.Resolution resolution = resolver.resolve(ACME, INDEX_UUID, 0, ShardRole.SEARCH_REPLICA);

        assertFalse(resolution.isResolved());
        assertEquals(PeerResolver.Unresolved.NOT_REQUIRED, resolution.unresolved());
    }

    @Test
    void aReplicaWaitsWhenThePrimaryIsNotServingYet() {
        // The convergence case, and the reason the projection must list copies that are not yet started: if
        // it were filtered to serving copies the replica could not even see the primary it is waiting for.
        // "Not yet" must be distinguishable from "never".
        projectIndex(Replication.DOCUMENT, List.of(
            new IndexStatus.ShardPlacement(PRIMARY_NODE, ShardRole.PRIMARY, ShardState.INITIALIZING),
            new IndexStatus.ShardPlacement(REPLICA_NODE, ShardRole.REPLICA, ShardState.INITIALIZING)
        ));
        projectNode(PRIMARY_NODE, "10.0.3.14", Liveness.LIVE);

        PeerResolver.Resolution resolution = resolver.resolve(ACME, INDEX_UUID, 0, ShardRole.REPLICA);

        assertFalse(resolution.isResolved());
        assertEquals(PeerResolver.Unresolved.NO_SERVING_PRIMARY, resolution.unresolved());
    }

    @Test
    void aReplicaWaitsWhenTheProjectionHasNotBeenWrittenYet() {
        PeerResolver.Resolution resolution = resolver.resolve(ACME, INDEX_UUID, 0, ShardRole.REPLICA);

        assertFalse(resolution.isResolved());
        assertEquals(PeerResolver.Unresolved.NO_PROJECTION, resolution.unresolved());
    }

    @Test
    void aLostPrimaryIsNotARecoverySource() {
        projectIndex(Replication.DOCUMENT, List.of(
            new IndexStatus.ShardPlacement(PRIMARY_NODE, ShardRole.PRIMARY, ShardState.STARTED)
        ));
        projectNode(PRIMARY_NODE, "10.0.3.14", Liveness.LOST);

        PeerResolver.Resolution resolution = resolver.resolve(ACME, INDEX_UUID, 0, ShardRole.REPLICA);

        assertFalse(resolution.isResolved());
        assertEquals(PeerResolver.Unresolved.PEER_UNREACHABLE, resolution.unresolved());
    }

    @Test
    void oneUnreachableReplicaDoesNotStopAPrimaryReplicatingToTheRest() {
        // Asymmetric on purpose. A primary we cannot reach means a replica has nothing to do; a replica we
        // cannot reach costs the primary one fan-out target, and a shard should not stop replicating
        // because one of three replicas is down.
        NodeId secondReplica = NodeId.of("zF5QLAUpQXqIwaNOD5vDKQ");
        projectIndex(Replication.DOCUMENT, List.of(
            new IndexStatus.ShardPlacement(PRIMARY_NODE, ShardRole.PRIMARY, ShardState.STARTED),
            new IndexStatus.ShardPlacement(REPLICA_NODE, ShardRole.REPLICA, ShardState.STARTED),
            new IndexStatus.ShardPlacement(secondReplica, ShardRole.REPLICA, ShardState.STARTED)
        ));
        projectNode(REPLICA_NODE, "10.0.3.15", Liveness.LIVE);
        projectNode(secondReplica, "10.0.3.16", Liveness.LOST);

        PeerResolver.Resolution resolution = resolver.resolve(ACME, INDEX_UUID, 0, ShardRole.PRIMARY);

        assertTrue(resolution.isResolved());
        assertEquals(List.of(REPLICA_NODE), resolution.peers().stream().map(PeerResolver.Peer::nodeId).toList());
    }

    @Test
    void aStaleNodeIsStillWorthTalkingTo() {
        // STALE is "we have not heard from it recently", not "it is gone". Refusing to replicate to a node
        // that is merely quiet would turn a missed heartbeat into an under-replicated shard.
        projectIndex(Replication.DOCUMENT, List.of(
            new IndexStatus.ShardPlacement(PRIMARY_NODE, ShardRole.PRIMARY, ShardState.STARTED)
        ));
        projectNode(PRIMARY_NODE, "10.0.3.14", Liveness.STALE);

        assertTrue(resolver.resolve(ACME, INDEX_UUID, 0, ShardRole.REPLICA).isResolved());
    }

    @Test
    void theProjectionsRoundTripThroughTheStore() {
        // These two documents are read by the control plane and by coordinators as well as here, so the
        // encoding has to survive a store round-trip rather than only an in-process one.
        projectIndex(Replication.DOCUMENT, List.of(
            new IndexStatus.ShardPlacement(PRIMARY_NODE, ShardRole.PRIMARY, ShardState.STARTED)
        ));
        projectNode(PRIMARY_NODE, "10.0.3.14", Liveness.LIVE);

        IndexStatus index = IndexStatus.from(store.get(Keys.indexStatus(ACME, INDEX_UUID)).orElseThrow().value());
        assertEquals(Replication.DOCUMENT, index.replication());
        assertEquals(HealthStatus.YELLOW, index.status());
        assertEquals(ShardRole.PRIMARY, index.shards().get(0).get(0).role());

        NodeStatus node = NodeStatus.from(store.get(Keys.nodeStatus(ACME, PRIMARY_NODE)).orElseThrow().value());
        assertEquals(Liveness.LIVE, node.liveness());
        assertEquals(Set.of(NodeRole.DATA), node.roles());
    }
}
