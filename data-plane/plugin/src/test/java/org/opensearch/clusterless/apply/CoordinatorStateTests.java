/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.clusterless.apply;

import org.opensearch.cluster.ClusterName;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.routing.IndexShardRoutingTable;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.clusterless.contract.IndexConfig;
import org.opensearch.clusterless.contract.IndexStatus;
import org.opensearch.clusterless.contract.NodeId;
import org.opensearch.clusterless.contract.NodeStatus;
import org.opensearch.clusterless.contract.ShardRole;
import org.opensearch.clusterless.contract.ShardState;
import org.opensearch.clusterless.contract.TenantId;
import org.opensearch.core.index.Index;
import org.opensearch.test.OpenSearchTestCase;

import java.util.EnumSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Routing to shards this node does not hold.
 *
 * <p>The interesting tests are about the union — a node that both holds copies of an index and coordinates for its
 * tenant — and about the two ways the fabrication can quietly ruin itself: disturbing this node's own routing
 * entries, and rebuilding other nodes' entries so that no two passes agree.
 */
public class CoordinatorStateTests extends OpenSearchTestCase {

    private static final ClusterName CLUSTER = new ClusterName("clusterless");
    private static final String LOCAL = "pv1sDMfZRxSTMiW5VrbN9w";
    private static final String PEER = "UntRkFd4QBihsxOaWA7Ijg";
    private static final String OTHER_PEER = "q0xuS-9oTkmb-pD1cfNKYA";
    private static final TenantId ACME = TenantId.of("acme");
    private static final String UUID = "uzKxLWZu4nPSrTknEcDpoA";

    // ---------------------------------------------------------------- a coordinator that holds nothing

    public void testAnIndexThisNodeHoldsNoPartOfIsStillRoutable() {
        ClusterState state = coordinate(
            ClusterState.EMPTY_STATE,
            copies(Map.of(0, List.of(copy(PEER, ShardRole.PRIMARY, ShardState.STARTED))))
        );

        IndexMetadata index = state.metadata().index(indexName());
        assertNotNull("the index has to be in the metadata to be routed to", index);
        assertEquals(1, state.routingTable().index(index.getIndex()).shards().size());

        ShardRouting routing = onlyCopy(state, 0);
        assertEquals(PEER, routing.currentNodeId());
        assertTrue(routing.started());
        assertTrue(routing.primary());
    }

    public void testThePeerIsInTheStatesNodesSoItsShardsCanBeAddressed() {
        // RoutingNodes looks up each shard's node and builds a RoutingNode around whatever it finds, so a routing
        // entry naming a node the state does not contain fails an invariant. Also the only way the transport can
        // reach it at all.
        ClusterState state = coordinate(
            ClusterState.EMPTY_STATE,
            copies(Map.of(0, List.of(copy(PEER, ShardRole.PRIMARY, ShardState.STARTED))))
        );

        assertEquals(2, state.nodes().getSize());
        assertNotNull(state.nodes().get(PEER));
        assertEquals("the ephemeral id is what makes the peer equal to the real one", "eph-" + PEER, state.nodes().get(PEER).getEphemeralId());
        assertEquals(peer(PEER).port(), state.nodes().get(PEER).getAddress().getPort());
        assertNotNull("and RoutingNodes must be constructible", state.getRoutingNodes().node(PEER));
    }

    public void testTheIdentitySettingsAreCarriedSoTheTenantsOwnNameResolves() {
        // Otherwise a coordinator would only answer to the fabricated name, which is the thing the name mapping
        // exists to hide. TenantIndexNameResolver reads exactly these two settings.
        ClusterState state = coordinate(
            ClusterState.EMPTY_STATE,
            copies(Map.of(0, List.of(copy(PEER, ShardRole.PRIMARY, ShardState.STARTED))))
        );

        IndexMetadata index = state.metadata().index(indexName());
        assertEquals("acme", org.opensearch.clusterless.ClusterlessIndexSettings.TENANT.get(index.getSettings()));
        assertEquals("my-index", org.opensearch.clusterless.ClusterlessIndexSettings.TENANT_INDEX_NAME.get(index.getSettings()));
    }

    public void testTheSearchReplicaCountDescribesTheFleetBecauseTheTableDoes() {
        // The mirror image of the rule on a data node, and the same rule: the count must describe the routing table
        // in the same state. Here that table holds every copy, so a search replica elsewhere counts -- and strict
        // search-replica routing then has somewhere to send a search.
        ClusterState state = coordinate(
            ClusterState.EMPTY_STATE,
            copies(
                Map.of(
                    0,
                    List.of(
                        copy(PEER, ShardRole.PRIMARY, ShardState.STARTED),
                        copy(OTHER_PEER, ShardRole.SEARCH_REPLICA, ShardState.STARTED)
                    )
                )
            )
        );

        assertEquals(1, state.metadata().index(indexName()).getNumberOfSearchOnlyReplicas());
        assertEquals(2, state.routingTable().index(new Index(indexName(), UUID)).shard(0).size());
    }

    // ---------------------------------------------------------------- the union, on a node that does both

    public void testACopyThisNodeHoldsIsKeptExactlyAsTheDataPathBuiltIt() {
        // D2.2, and the most expensive mistake available here: a rebuilt routing entry is a shard the server has
        // never heard of where a started one used to be, so it recovers it -- on every pass, for ever.
        ClusterState held = new DataNodeState(
            CLUSTER,
            TestFixtures.localNode(LOCAL),
            desiredHere(Map.of(0, ShardRole.PRIMARY)),
            MappingSource.WRAPPED,
            LocalShardStore.EMPTY
        ).buildClusterState(ClusterState.EMPTY_STATE);
        ShardRouting mine = onlyCopy(held, 0);

        // The supervisor's projection reports the same copy, a pass behind, plus a search replica elsewhere.
        ClusterState state = coordinate(
            held,
            copies(
                Map.of(
                    0,
                    List.of(
                        copy(LOCAL, ShardRole.PRIMARY, ShardState.INITIALIZING),
                        copy(PEER, ShardRole.SEARCH_REPLICA, ShardState.STARTED)
                    )
                )
            ),
            desiredHere(Map.of(0, ShardRole.PRIMARY))
        );

        IndexShardRoutingTable shard = state.routingTable().index(new Index(indexName(), UUID)).shard(0);
        assertEquals("both copies, and only two", 2, shard.size());
        ShardRouting stillMine = shard.shards().stream().filter(copy -> LOCAL.equals(copy.currentNodeId())).findFirst().orElseThrow();
        assertEquals("the allocation id must not move", mine.allocationId(), stillMine.allocationId());
        assertEquals(mine.state(), stillMine.state());
        assertSame("carried across untouched rather than rebuilt", mine, stillMine);
    }

    public void testTheMetadataOfAnIndexThisNodeHoldsIsNotReplaced() {
        // The data path decided its settings -- including the local search-replica count and anything the remote
        // store contributed -- and a coordinator has no business overwriting that.
        ClusterState held = new DataNodeState(
            CLUSTER,
            TestFixtures.localNode(LOCAL),
            desiredHere(Map.of(0, ShardRole.PRIMARY)),
            MappingSource.WRAPPED,
            LocalShardStore.EMPTY
        ).buildClusterState(ClusterState.EMPTY_STATE);
        IndexMetadata before = held.metadata().index(indexName());

        ClusterState state = coordinate(
            held,
            copies(Map.of(0, List.of(copy(LOCAL, ShardRole.PRIMARY, ShardState.STARTED)))),
            desiredHere(Map.of(0, ShardRole.PRIMARY))
        );

        assertEquals("the data path's metadata must survive untouched", before, state.metadata().index(indexName()));
        assertEquals("and its version must not move, or the server reloads it for nothing",
            before.getVersion(), state.metadata().index(indexName()).getVersion());
    }

    // ---------------------------------------------------------------- the fixed point

    public void testAsecondPassProducesTheSameRoutingForAPeersCopy() {
        // Not efficiency: initialize() mints an allocation id, so rebuilding a peer's entry every pass would make
        // the routing table differ from the last one for ever and this node would publish a state every poll
        // interval. That is the M5 bug in a new place, and StateEquivalence cannot see through it.
        CoordinatorView view = copies(Map.of(0, List.of(copy(PEER, ShardRole.PRIMARY, ShardState.STARTED))));

        ClusterState first = coordinate(ClusterState.EMPTY_STATE, view);
        ClusterState second = coordinate(first, view);

        assertEquals(onlyCopy(first, 0).allocationId(), onlyCopy(second, 0).allocationId());
        assertTrue("and the states must compare equal, or nothing converges", StateEquivalence.sameContent(first, second));
    }

    public void testACopyThatHasStartedSinceTheLastPassIsRebuiltRatherThanFrozen() {
        ClusterState first = coordinate(
            ClusterState.EMPTY_STATE,
            copies(Map.of(0, List.of(copy(PEER, ShardRole.PRIMARY, ShardState.INITIALIZING))))
        );
        assertTrue(onlyCopy(first, 0).initializing());

        ClusterState second = coordinate(
            first,
            copies(Map.of(0, List.of(copy(PEER, ShardRole.PRIMARY, ShardState.STARTED))))
        );

        assertTrue("the projection said it started, so the state must say so too", onlyCopy(second, 0).started());
    }

    // ---------------------------------------------------------------- what it declines to route to

    public void testACopyOnANodeWithNoProjectionIsNotRoutedTo() {
        // There is no address to reach it with, and inventing one would produce a shard the coordinator believes
        // in and cannot use.
        CoordinatorView view = new CoordinatorView(
            List.of(new CoordinatorView.RoutedIndex(ACME, config(), placement(Map.of(0, List.of(copy(PEER, ShardRole.PRIMARY, ShardState.STARTED)))))),
            Map.of(),
            List.of()
        );

        ClusterState state = new CoordinatorState(holdsNothing(), TestFixtures.localNode(LOCAL), view, MappingSource.WRAPPED)
            .buildClusterState(ClusterState.EMPTY_STATE);

        assertNull("no routable copy means no shard table at all", state.routingTable().index(new Index(indexName(), UUID)).shard(0));
        assertEquals("and no node added", 1, state.nodes().getSize());
    }

    public void testAFailedOrUnknownCopyIsNotRoutedTo() {
        ClusterState state = coordinate(
            ClusterState.EMPTY_STATE,
            copies(
                Map.of(
                    0,
                    List.of(copy(PEER, ShardRole.PRIMARY, ShardState.FAILED), copy(OTHER_PEER, ShardRole.UNKNOWN, ShardState.STARTED))
                )
            )
        );

        assertNull(
            "a failed copy occupies disk and cannot serve; an unrecognised role cannot be mapped at all",
            state.routingTable().index(new Index(indexName(), UUID)).shard(0)
        );
    }

    public void testANodeThatCoordinatesNothingIsUnchangedByThis() {
        ClusterState held = new DataNodeState(
            CLUSTER,
            TestFixtures.localNode(LOCAL),
            desiredHere(Map.of(0, ShardRole.PRIMARY)),
            MappingSource.WRAPPED,
            LocalShardStore.EMPTY
        ).buildClusterState(ClusterState.EMPTY_STATE);

        ClusterState state = new CoordinatorState(
            previous -> held,
            TestFixtures.localNode(LOCAL),
            CoordinatorView.nothing(),
            MappingSource.WRAPPED
        ).buildClusterState(ClusterState.EMPTY_STATE);

        assertSame(held, state);
    }

    // ---------------------------------------------------------------- fixtures

    private static String indexName() {
        return IndexNaming.indexName(ACME, UUID, "my-index");
    }

    private static ShardRouting onlyCopy(ClusterState state, int shard) {
        return state.routingTable().index(new Index(indexName(), UUID)).shard(shard).shards().get(0);
    }

    /**
     * A delegate that holds nothing, which is what a pure coordinator's data path is.
     *
     * <p>It contributes the local node and nothing else — not the previous state wholesale. That matters: the real
     * delegate rebuilds its routing table from the assignment each pass, so peers' entries reach this class only
     * through {@code previous}, and a fake that passed them through in {@code held} as well would hide a
     * double-add this class must not make.
     */
    private static NodeState holdsNothing() {
        return previous -> ClusterState.builder(CLUSTER)
            .version(previous.version() + 1)
            .nodes(
                org.opensearch.cluster.node.DiscoveryNodes.builder()
                    .localNodeId(LOCAL)
                    .add(TestFixtures.localNode(LOCAL))
                    .build()
            )
            .build();
    }

    private static ClusterState coordinate(ClusterState previous, CoordinatorView view) {
        return new CoordinatorState(holdsNothing(), TestFixtures.localNode(LOCAL), view, MappingSource.WRAPPED)
            .buildClusterState(previous);
    }

    private static ClusterState coordinate(ClusterState previous, CoordinatorView view, DesiredState alsoHeld) {
        NodeState data = new DataNodeState(
            CLUSTER,
            TestFixtures.localNode(LOCAL),
            alsoHeld,
            MappingSource.WRAPPED,
            LocalShardStore.EMPTY
        );
        return new CoordinatorState(data, TestFixtures.localNode(LOCAL), view, MappingSource.WRAPPED).buildClusterState(previous);
    }

    private static IndexStatus.Copy copy(String node, ShardRole role, ShardState state) {
        return new IndexStatus.Copy(NodeId.of(node), role, state, state == ShardState.STARTED);
    }

    private static CoordinatorView copies(Map<Integer, List<IndexStatus.Copy>> shards) {
        Map<NodeId, NodeStatus> peers = new LinkedHashMap<>();
        shards.values().forEach(copies -> copies.forEach(copy -> peers.put(copy.node(), peer(copy.node().value()))));
        return new CoordinatorView(
            List.of(new CoordinatorView.RoutedIndex(ACME, config(), placement(shards))),
            peers,
            List.of()
        );
    }

    private static IndexStatus placement(Map<Integer, List<IndexStatus.Copy>> shards) {
        return new IndexStatus("my-index", UUID, 3041, shards.size(), 0, IndexStatus.Replication.SEGMENT, shards);
    }

    private static NodeStatus peer(String nodeId) {
        return new NodeStatus(
            NodeId.of(nodeId),
            "eph-" + nodeId,
            "data-" + nodeId.substring(0, 4),
            EnumSet.of(org.opensearch.clusterless.contract.NodeRole.DATA),
            "127.0.0.1",
            // Distinct per node, because DiscoveryNodes.Builder refuses two nodes at one address -- which two data
            // nodes on one host would otherwise be, and which CoordinatorState now tolerates rather than failing on.
            9300 + Math.abs(nodeId.hashCode() % 100),
            "3.9.0",
            Map.of(),
            NodeStatus.Liveness.LIVE
        );
    }

    private static IndexConfig config() {
        return new IndexConfig(
            "my-index",
            UUID,
            1788969977933L,
            true,
            Map.of("index.number_of_shards", "1", "index.number_of_replicas", "0", "index.replication.type", "SEGMENT"),
            Map.of("properties", Map.of("host", Map.of("type", "keyword"))),
            3041
        );
    }

    private static DesiredState desiredHere(Map<Integer, ShardRole> shards) {
        DesiredState.IndexKey key = new DesiredState.IndexKey(ACME, UUID);
        return new DesiredState(42, Map.of(key, new DesiredState.DesiredIndex(key, config(), shards)), List.of(ACME), List.of());
    }
}
