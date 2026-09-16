/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.clusterless.apply;

import org.opensearch.clusterless.contract.Keys;
import org.opensearch.clusterless.contract.NodeId;
import org.opensearch.clusterless.contract.ShardRole;
import org.opensearch.clusterless.contract.TenantId;
import org.opensearch.controlplane.store.InMemoryKeyValueStore;
import org.opensearch.test.OpenSearchTestCase;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * Which peers a coordinator will route to, and which it refuses.
 *
 * <p>Every test here is a peer being dropped, because that is where the interesting behaviour is: a store that has
 * been used by more than one run of a deployment accumulates projections for nodes that no longer exist, and two
 * of them can claim one endpoint. Sending a query to a node that is gone is the failure this prevents; dropping the
 * <em>live</em> node instead of the dead one is the worse failure it also prevents.
 */
public class CoordinatorViewSourceTests extends OpenSearchTestCase {

    private static final TenantId ACME = TenantId.of("acme");
    private static final String UUID = "uzKxLWZu4nPSrTknEcDpoA";

    private final InMemoryKeyValueStore store = new InMemoryKeyValueStore();

    private final IndexConfigSource configs = (tenant, name) -> Optional.of(TestFixtures.segmentReplicatedIndexConfig(UUID));

    public void testALiveAddressablePeerIsRoutedTo() {
        placement("AAAAAAAAAAAAAAAAAAAAAA");
        peer("AAAAAAAAAAAAAAAAAAAAAA", "eph-a", 9301, "live");

        CoordinatorView view = read();

        assertEquals(1, view.indices().size());
        assertEquals(1, view.peers().size());
        assertTrue(view.deferred().toString(), view.deferred().isEmpty());
    }

    public void testALostPeerIsNotRoutedTo() {
        // The supervisor has given up on it and is placing its copies elsewhere, so a query sent there waits for a
        // node that is not coming back.
        placement("AAAAAAAAAAAAAAAAAAAAAA");
        peer("AAAAAAAAAAAAAAAAAAAAAA", "eph-a", 9301, "lost");

        CoordinatorView view = read();

        assertTrue(view.peers().isEmpty());
        assertTrue(view.deferred().get(0), view.deferred().get(0).contains("given up on this node"));
    }

    public void testAStalePeerIsStillRoutedTo() {
        // A late heartbeat usually means a slow store rather than a dead node, and refusing to route to it would
        // turn one into an outage.
        placement("AAAAAAAAAAAAAAAAAAAAAA");
        peer("AAAAAAAAAAAAAAAAAAAAAA", "eph-a", 9301, "stale");

        assertEquals(1, read().peers().size());
    }

    public void testAPeerWithNoEphemeralIdIsNotRoutedTo() {
        // An older supervisor's projection. The address is there and it is not enough: DiscoveryNode equality is
        // the ephemeral id alone, so a peer built from this could never be connected to.
        placement("AAAAAAAAAAAAAAAAAAAAAA");
        store.put(
            Keys.nodeStatus(ACME, NodeId.of("AAAAAAAAAAAAAAAAAAAAAA")),
            "{\"node_id\":\"AAAAAAAAAAAAAAAAAAAAAA\",\"address\":\"127.0.0.1\",\"port\":9301,\"liveness\":\"live\"}"
                .getBytes(StandardCharsets.UTF_8)
        );

        CoordinatorView view = read();

        assertTrue(view.peers().isEmpty());
        assertTrue(view.deferred().get(0), view.deferred().get(0).contains("no ephemeral id"));
    }

    public void testWhenTwoNodesClaimOneEndpointTheLiveOneWins() {
        // The case a reused store produces, and the one that was failing. A node whose data directory was wiped
        // comes back with a new node id on the same port, and until the supervisor reaps the old projection both
        // claim 127.0.0.1:9301. Choosing deliberately matters because the alternative -- letting
        // DiscoveryNodes.Builder refuse whichever was added second -- could drop the live node's shards in favour
        // of a dead node's.
        placement("AAAAAAAAAAAAAAAAAAAAAA", "BBBBBBBBBBBBBBBBBBBBBB");
        peer("AAAAAAAAAAAAAAAAAAAAAA", "eph-a", 9301, "stale");
        peer("BBBBBBBBBBBBBBBBBBBBBB", "eph-b", 9301, "live");

        CoordinatorView view = read();

        assertEquals(1, view.peers().size());
        assertTrue("the live node is the one kept", view.peers().containsKey(NodeId.of("BBBBBBBBBBBBBBBBBBBBBB")));
        assertTrue(view.deferred().get(0), view.deferred().get(0).contains("already claimed by node"));
    }

    public void testTheChoiceBetweenEqualPeersIsStableAcrossPasses() {
        // An unstable choice would rebuild the routing table every pass, which is the mistake CoordinatorState
        // carries entries forward to avoid -- and it would be invisible, because each pass would look reasonable.
        placement("AAAAAAAAAAAAAAAAAAAAAA", "BBBBBBBBBBBBBBBBBBBBBB");
        peer("AAAAAAAAAAAAAAAAAAAAAA", "eph-a", 9301, "live");
        peer("BBBBBBBBBBBBBBBBBBBBBB", "eph-b", 9301, "live");

        assertEquals(read().peers().keySet(), read().peers().keySet());
        assertTrue(read().peers().containsKey(NodeId.of("AAAAAAAAAAAAAAAAAAAAAA")));
    }

    public void testDifferentPortsOnOneHostAreTwoPeers() {
        // The ordinary case, and the one an earlier version of this got wrong by treating a host as an endpoint.
        placement("AAAAAAAAAAAAAAAAAAAAAA", "BBBBBBBBBBBBBBBBBBBBBB");
        peer("AAAAAAAAAAAAAAAAAAAAAA", "eph-a", 9301, "live");
        peer("BBBBBBBBBBBBBBBBBBBBBB", "eph-b", 9302, "live");

        CoordinatorView view = read();

        assertEquals(2, view.peers().size());
        assertTrue(view.deferred().toString(), view.deferred().isEmpty());
    }

    public void testACopyOnANodeWithNoProjectionSaysSo() {
        placement("AAAAAAAAAAAAAAAAAAAAAA");

        CoordinatorView view = read();

        assertTrue(view.peers().isEmpty());
        assertTrue(view.deferred().get(0), view.deferred().get(0).contains("no node projection"));
    }

    public void testANodeWithNothingToRouteAndNothingToHoldReadsNothing() {
        CoordinatorView view = new CoordinatorViewSource.FromStore(store, configs).read(DesiredState.nothing());

        assertTrue(view.isEmpty());
        assertTrue(view.peers().isEmpty());
    }

    // ---------------------------------------------------------------- peer resolution, for a node that routes nothing

    public void testANodeHoldingACopyGetsThePeersOfItsOwnShard() {
        // No coordinator role anywhere in this test. The node holds shard 0 and needs the other copy of it, because
        // a primary publishes segment-replication checkpoints to the replicas in its own routing table and a search
        // replica whose primary is missing has to set the marker that disables its own polling.
        placement("AAAAAAAAAAAAAAAAAAAAAA", "BBBBBBBBBBBBBBBBBBBBBB");
        peer("AAAAAAAAAAAAAAAAAAAAAA", "eph-a", 9301, "live");
        peer("BBBBBBBBBBBBBBBBBBBBBB", "eph-b", 9302, "live");

        CoordinatorView view = readHolding(Map.of(0, ShardRole.PRIMARY));

        assertEquals(1, view.indices().size());
        assertEquals(Set.of(0), view.indices().get(0).shards());
        assertEquals("both copies' nodes are wanted", 2, view.peers().size());
    }

    public void testItTakesOnlyTheShardsThisNodeHolds() {
        // The difference from coordinating, and the reason peer resolution is the cheaper mechanism: a node holding
        // one shard of a large index has no business knowing where the other shards are.
        store.put(
            Keys.indexStatus(ACME, UUID),
            ("{\"index\":\"my-index\",\"index_uuid\":\"" + UUID + "\",\"number_of_shards\":2,\"shards\":{"
                + "\"0\":[{\"node_id\":\"AAAAAAAAAAAAAAAAAAAAAA\",\"role\":\"PRIMARY\",\"state\":\"STARTED\"}],"
                + "\"1\":[{\"node_id\":\"BBBBBBBBBBBBBBBBBBBBBB\",\"role\":\"PRIMARY\",\"state\":\"STARTED\"}]}}")
                .getBytes(StandardCharsets.UTF_8)
        );
        peer("AAAAAAAAAAAAAAAAAAAAAA", "eph-a", 9301, "live");
        peer("BBBBBBBBBBBBBBBBBBBBBB", "eph-b", 9302, "live");

        CoordinatorView view = readHolding(Map.of(0, ShardRole.PRIMARY));

        assertEquals(Set.of(0), view.indices().get(0).shards());
        assertEquals("only shard 0's node, not shard 1's", 1, view.peers().size());
        assertTrue(view.peers().containsKey(NodeId.of("AAAAAAAAAAAAAAAAAAAAAA")));
    }

    public void testAnIndexThisNodeNeitherHoldsNorCoordinatesIsIgnored() {
        // The property that keeps a data node's state small, and the reason the contract put this table in a
        // projection rather than in every node's assignment.
        store.put(
            Keys.indexStatus(ACME, "someOtherIndex12345678"),
            ("{\"index\":\"not-mine\",\"index_uuid\":\"someOtherIndex12345678\",\"number_of_shards\":1,\"shards\":{"
                + "\"0\":[{\"node_id\":\"BBBBBBBBBBBBBBBBBBBBBB\",\"role\":\"PRIMARY\",\"state\":\"STARTED\"}]}}")
                .getBytes(StandardCharsets.UTF_8)
        );
        placement("AAAAAAAAAAAAAAAAAAAAAA");
        peer("AAAAAAAAAAAAAAAAAAAAAA", "eph-a", 9301, "live");
        peer("BBBBBBBBBBBBBBBBBBBBBB", "eph-b", 9302, "live");

        CoordinatorView view = readHolding(Map.of(0, ShardRole.PRIMARY));

        assertEquals(1, view.indices().size());
        assertEquals(UUID, view.indices().get(0).uuid());
        assertEquals("and the other index's node is not wanted", 1, view.peers().size());
    }

    public void testHoldingAnIndexNeedsNoIndexDocumentRead() {
        // Its configuration was already read to build the copies, so the desired state has it. A reader that went
        // back to the store would be paying twice and could see a different revision than the copies were built
        // against.
        CoordinatorViewSource source = new CoordinatorViewSource.FromStore(store, (tenant, name) -> {
            throw new AssertionError("the index document must not be read for an index this node already holds");
        });
        placement("AAAAAAAAAAAAAAAAAAAAAA");
        peer("AAAAAAAAAAAAAAAAAAAAAA", "eph-a", 9301, "live");

        DesiredState.IndexKey key = new DesiredState.IndexKey(ACME, UUID);
        CoordinatorView view = source.read(
            new DesiredState(
                42,
                Map.of(key, new DesiredState.DesiredIndex(key, TestFixtures.segmentReplicatedIndexConfig(UUID), Map.of(0, ShardRole.PRIMARY))),
                List.of(),
                List.of()
            )
        );

        assertEquals(1, view.indices().size());
    }

    public void testCoordinatingAndHoldingTheSameTenantIsOneScanAndTheWholeIndex() {
        // A node that does both takes every shard, because coordinating subsumes peer resolution -- and it should
        // not read the tenant's projections twice to work that out.
        store.put(
            Keys.indexStatus(ACME, UUID),
            ("{\"index\":\"my-index\",\"index_uuid\":\"" + UUID + "\",\"number_of_shards\":2,\"shards\":{"
                + "\"0\":[{\"node_id\":\"AAAAAAAAAAAAAAAAAAAAAA\",\"role\":\"PRIMARY\",\"state\":\"STARTED\"}],"
                + "\"1\":[{\"node_id\":\"BBBBBBBBBBBBBBBBBBBBBB\",\"role\":\"PRIMARY\",\"state\":\"STARTED\"}]}}")
                .getBytes(StandardCharsets.UTF_8)
        );
        peer("AAAAAAAAAAAAAAAAAAAAAA", "eph-a", 9301, "live");
        peer("BBBBBBBBBBBBBBBBBBBBBB", "eph-b", 9302, "live");

        DesiredState.IndexKey key = new DesiredState.IndexKey(ACME, UUID);
        CoordinatorView view = new CoordinatorViewSource.FromStore(store, configs).read(
            new DesiredState(
                42,
                Map.of(key, new DesiredState.DesiredIndex(key, TestFixtures.segmentReplicatedIndexConfig(UUID), Map.of(0, ShardRole.PRIMARY))),
                List.of(ACME),
                List.of()
            )
        );

        assertEquals(1, view.indices().size());
        assertEquals("coordinating takes both shards, not just the held one", Set.of(0, 1), view.indices().get(0).shards());
    }

    // ---------------------------------------------------------------- what it reports about its own reach

    public void testAServingCopyOnAKeptPeerCounts() {
        placement("AAAAAAAAAAAAAAAAAAAAAA");
        peer("AAAAAAAAAAAAAAAAAAAAAA", "eph-a", 9301, "live");

        CoordinatorView view = read();

        assertEquals(1, view.servingCopies());
        assertTrue(view.indicesWithNothingServing().toString(), view.indicesWithNothingServing().isEmpty());
    }

    public void testAnIndexWhoseOnlyCopyIsOnALostNodeHasNothingServing() {
        // The state that otherwise looks like health: assignment applied, routing table built, nothing declined,
        // and a search returns nothing. It has to be reportable or it is invisible.
        placement("AAAAAAAAAAAAAAAAAAAAAA");
        peer("AAAAAAAAAAAAAAAAAAAAAA", "eph-a", 9301, "lost");

        CoordinatorView view = read();

        assertEquals(0, view.servingCopies());
        assertEquals(List.of("acme/my-index"), view.indicesWithNothingServing());
    }

    public void testAnInitializingCopyIsNotYetSomethingToAnswerFrom() {
        // It belongs in the routing table -- the state should say what the fleet is doing -- and a query cannot be
        // answered from it, so the two numbers differ on purpose.
        store.put(
            Keys.indexStatus(ACME, UUID),
            ("{\"index\":\"my-index\",\"index_uuid\":\"" + UUID + "\",\"number_of_shards\":1,\"shards\":{\"0\":["
                + "{\"node_id\":\"AAAAAAAAAAAAAAAAAAAAAA\",\"role\":\"PRIMARY\",\"state\":\"INITIALIZING\"}]}}")
                .getBytes(StandardCharsets.UTF_8)
        );
        peer("AAAAAAAAAAAAAAAAAAAAAA", "eph-a", 9301, "live");

        CoordinatorView view = read();

        assertEquals(1, view.peers().size());
        assertEquals(0, view.servingCopies());
        assertEquals(List.of("acme/my-index"), view.indicesWithNothingServing());
    }

    // ---------------------------------------------------------------- fixtures

    /** A node coordinating the tenant, which takes every shard of every one of its indices. */
    private CoordinatorView read() {
        return new CoordinatorViewSource.FromStore(store, configs)
            .read(new DesiredState(42, Map.of(), List.of(ACME), List.of()));
    }

    /**
     * A node holding one copy and coordinating nothing, which takes the peers of its own shards and no more.
     *
     * <p>The case peer resolution exists for: this node answers no queries and still needs the other copies of its
     * shard, because a primary publishes checkpoints to the replicas in its own routing table and a search replica
     * without its primary has to set the marker that stops its own polling.
     */
    private CoordinatorView readHolding(Map<Integer, ShardRole> shards) {
        DesiredState.IndexKey key = new DesiredState.IndexKey(ACME, UUID);
        DesiredState desired = new DesiredState(
            42,
            Map.of(key, new DesiredState.DesiredIndex(key, TestFixtures.segmentReplicatedIndexConfig(UUID), shards)),
            List.of(),
            List.of()
        );
        return new CoordinatorViewSource.FromStore(store, configs).read(desired);
    }

    private void placement(String... nodeIds) {
        StringBuilder copies = new StringBuilder();
        for (String nodeId : nodeIds) {
            if (copies.length() > 0) {
                copies.append(',');
            }
            copies.append("{\"node_id\":\"").append(nodeId).append("\",\"role\":\"PRIMARY\",\"state\":\"STARTED\"}");
        }
        store.put(
            Keys.indexStatus(ACME, UUID),
            ("{\"index\":\"my-index\",\"index_uuid\":\"" + UUID + "\",\"number_of_shards\":1,"
                + "\"shards\":{\"0\":[" + copies + "]}}").getBytes(StandardCharsets.UTF_8)
        );
    }

    private void peer(String nodeId, String ephemeralId, int port, String liveness) {
        store.put(
            Keys.nodeStatus(ACME, NodeId.of(nodeId)),
            ("{\"node_id\":\"" + nodeId + "\",\"ephemeral_id\":\"" + ephemeralId + "\",\"name\":\"data\","
                + "\"roles\":[\"data\"],\"address\":\"127.0.0.1\",\"port\":" + port + ",\"version\":\"3.9.0\","
                + "\"liveness\":\"" + liveness + "\"}").getBytes(StandardCharsets.UTF_8)
        );
    }
}
