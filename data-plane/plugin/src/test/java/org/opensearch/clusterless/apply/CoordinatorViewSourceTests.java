/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.clusterless.apply;

import org.opensearch.clusterless.contract.Keys;
import org.opensearch.clusterless.contract.NodeId;
import org.opensearch.clusterless.contract.TenantId;
import org.opensearch.controlplane.store.InMemoryKeyValueStore;
import org.opensearch.test.OpenSearchTestCase;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Optional;

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

    public void testANodeThatCoordinatesNothingReadsNothing() {
        CoordinatorView view = new CoordinatorViewSource.FromStore(store, configs).read(List.of());

        assertTrue(view.isEmpty());
        assertTrue(view.peers().isEmpty());
    }

    // ---------------------------------------------------------------- fixtures

    private CoordinatorView read() {
        return new CoordinatorViewSource.FromStore(store, configs).read(List.of(ACME));
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
