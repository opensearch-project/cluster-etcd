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

    public void testTheSearchReplicaCountFollowsThePeersCopyEvenOnANodeHoldingThePrimary() {
        // Found by reading a real node's cluster state: number_of_search_replicas said 0 beside a routing table
        // containing a started search replica on another node, so cluster.routing.search_replica.strict had nothing
        // to act on and searches were spread across primaries and search replicas rather than preferring the
        // replicas -- which is what search replicas are for. The rule is the one DataNodeState follows: the count
        // must describe the routing table in the state it ships in, and this state's table is the union.
        ClusterState held = new DataNodeState(
            CLUSTER,
            TestFixtures.localNode(LOCAL),
            desiredHere(Map.of(0, ShardRole.PRIMARY)),
            MappingSource.WRAPPED,
            LocalShardStore.EMPTY
        ).buildClusterState(ClusterState.EMPTY_STATE);
        assertEquals("the data path describes only what it holds", 0, held.metadata().index(indexName()).getNumberOfSearchOnlyReplicas());

        ClusterState state = coordinate(
            held,
            copies(
                Map.of(
                    0,
                    List.of(copy(LOCAL, ShardRole.PRIMARY, ShardState.STARTED), copy(PEER, ShardRole.SEARCH_REPLICA, ShardState.STARTED))
                )
            ),
            desiredHere(Map.of(0, ShardRole.PRIMARY))
        );

        assertEquals(1, state.metadata().index(indexName()).getNumberOfSearchOnlyReplicas());
        assertEquals(
            "and the settings version moves, because the settings did",
            held.metadata().index(indexName()).getSettingsVersion() + 1,
            state.metadata().index(indexName()).getSettingsVersion()
        );
        // Everything else the data path decided is untouched.
        assertEquals(
            org.opensearch.clusterless.ClusterlessIndexSettings.TENANT.get(held.metadata().index(indexName()).getSettings()),
            org.opensearch.clusterless.ClusterlessIndexSettings.TENANT.get(state.metadata().index(indexName()).getSettings())
        );
    }

    public void testWithNoSearchReplicaElsewhereTheDataPathsMetadataIsUntouched() {
        ClusterState held = new DataNodeState(
            CLUSTER,
            TestFixtures.localNode(LOCAL),
            desiredHere(Map.of(0, ShardRole.PRIMARY)),
            MappingSource.WRAPPED,
            LocalShardStore.EMPTY
        ).buildClusterState(ClusterState.EMPTY_STATE);

        ClusterState state = coordinate(
            held,
            copies(Map.of(0, List.of(copy(LOCAL, ShardRole.PRIMARY, ShardState.STARTED)))),
            desiredHere(Map.of(0, ShardRole.PRIMARY))
        );

        assertEquals(
            "the settings version must not move for a state that says the same thing",
            held.metadata().index(indexName()).getSettingsVersion(),
            state.metadata().index(indexName()).getSettingsVersion()
        );
    }

    public void testTheSearchOnlyMarkerIsDroppedOnceTheTableHasAPrimary() {
        // The marker exists only to satisfy RoutingNodes' assertion that a shard has a primary, and it costs more
        // than it looks: IndexService.AsyncReplicationTask.shouldRun() is `false == INDEX_BLOCKS_SEARCH_ONLY`, and
        // that task is what syncs active search-only shards from the remote store on the refresh interval. So while
        // the marker is set, this node's search replica is refreshed by nothing except a checkpoint published by a
        // primary -- which cannot happen unless that primary's own routing table knows the replica exists.
        //
        // Coordinating puts the primary in this node's table, so the marker is no longer needed, so the sync
        // resumes. That is a better reason to coordinate on a search-replica node than routing is.
        ClusterState held = new DataNodeState(
            CLUSTER,
            TestFixtures.localNode(LOCAL),
            desiredHere(Map.of(0, ShardRole.SEARCH_REPLICA)),
            MappingSource.WRAPPED,
            LocalShardStore.EMPTY
        ).buildClusterState(ClusterState.EMPTY_STATE);
        assertTrue(
            "the data path sets it, because on its own this node's table has no primary",
            IndexMetadata.INDEX_BLOCKS_SEARCH_ONLY_SETTING.get(held.metadata().index(indexName()).getSettings())
        );

        ClusterState state = coordinate(
            held,
            copies(
                Map.of(
                    0,
                    List.of(copy(LOCAL, ShardRole.SEARCH_REPLICA, ShardState.STARTED), copy(PEER, ShardRole.PRIMARY, ShardState.STARTED))
                )
            ),
            desiredHere(Map.of(0, ShardRole.SEARCH_REPLICA))
        );

        assertFalse(
            "with the primary in the table the marker is unnecessary, and keeping it would stop replication",
            IndexMetadata.INDEX_BLOCKS_SEARCH_ONLY_SETTING.get(state.metadata().index(indexName()).getSettings())
        );
        // And the assertion the marker was silencing must still not fire, which is the whole reason it can go.
        assertNotNull(state.getRoutingNodes().node(LOCAL));
        assertNotNull(state.getRoutingNodes().node(PEER));
    }

    public void testABlockTheTenantAskedForIsNotRemoved() {
        // The flag's real meaning, and the reason it exists upstream: a rolled-over daily index has no writer, so
        // its search replicas should stop polling for updates that are not coming. Dropping it because a primary is
        // in the routing table would overrule the operator and restart the polling they asked to stop. Only a marker
        // this plugin added for its own purposes is taken away.
        IndexConfig readOnly = new IndexConfig(
            "my-index",
            UUID,
            1788969977933L,
            true,
            Map.of(
                "index.number_of_shards", "1",
                "index.number_of_replicas", "0",
                "index.replication.type", "SEGMENT",
                "index.blocks.search_only", "true"
            ),
            Map.of(),
            3041
        );
        DesiredState.IndexKey key = new DesiredState.IndexKey(ACME, UUID);
        DesiredState desired = new DesiredState(
            42,
            Map.of(key, new DesiredState.DesiredIndex(key, readOnly, Map.of(0, ShardRole.SEARCH_REPLICA))),
            List.of(ACME),
            List.of()
        );

        CoordinatorView view = new CoordinatorView(
            List.of(CoordinatorView.RoutedIndex.wholeIndex(ACME, readOnly, placement(Map.of(0, List.of(
                copy(LOCAL, ShardRole.SEARCH_REPLICA, ShardState.STARTED),
                copy(PEER, ShardRole.PRIMARY, ShardState.STARTED)
            ))))),
            Map.of(NodeId.of(PEER), peer(PEER)),
            List.of()
        );

        ClusterState state = new CoordinatorState(
            new DataNodeState(CLUSTER, TestFixtures.localNode(LOCAL), desired, MappingSource.WRAPPED, LocalShardStore.EMPTY),
            TestFixtures.localNode(LOCAL),
            view,
            MappingSource.WRAPPED
        ).buildClusterState(ClusterState.EMPTY_STATE);

        assertTrue(
            "the tenant asked for this one, and a primary in the table is not a reason to overrule them",
            IndexMetadata.INDEX_BLOCKS_SEARCH_ONLY_SETTING.get(state.metadata().index(indexName()).getSettings())
        );
    }

    public void testTheMarkerStaysWhenNoPrimaryCouldBeFound() {
        // A coordinator that cannot reach the primary's node is back to a table with no primary for that shard, so
        // the assertion needs silencing again -- and the copy is stale rather than fresh, which is the honest state.
        ClusterState held = new DataNodeState(
            CLUSTER,
            TestFixtures.localNode(LOCAL),
            desiredHere(Map.of(0, ShardRole.SEARCH_REPLICA)),
            MappingSource.WRAPPED,
            LocalShardStore.EMPTY
        ).buildClusterState(ClusterState.EMPTY_STATE);

        CoordinatorView noPeers = new CoordinatorView(
            List.of(CoordinatorView.RoutedIndex.wholeIndex(ACME, config(), placement(Map.of(0, List.of(copy(PEER, ShardRole.PRIMARY, ShardState.STARTED)))))),
            Map.of(),
            List.of()
        );
        ClusterState state = new CoordinatorState(
            new DataNodeState(
                CLUSTER,
                TestFixtures.localNode(LOCAL),
                desiredHere(Map.of(0, ShardRole.SEARCH_REPLICA)),
                MappingSource.WRAPPED,
                LocalShardStore.EMPTY
            ),
            TestFixtures.localNode(LOCAL),
            noPeers,
            MappingSource.WRAPPED
        ).buildClusterState(held);

        assertTrue(
            IndexMetadata.INDEX_BLOCKS_SEARCH_ONLY_SETTING.get(state.metadata().index(indexName()).getSettings())
        );
        assertNotNull("and the assertion must not fire", state.getRoutingNodes().node(LOCAL));
    }

    public void testASecondPassShipsTheSameSettingsAndDoesNotMoveTheVersion() {
        // The test that was missing, and its absence took a node down. IndexService.updateMetadata asserts that a
        // moved settings version means the settings actually differ, and an AssertionError inside the applier is
        // fatal -- so a version bumped for a change that nets to nothing kills the process.
        //
        // Netting to nothing is what the two stages do together: the data path rebuilds this index's settings from
        // the assignment every pass, without the search-replica count or the dropped marker this class adds, so it
        // sees a difference from the previous state and bumps for it; then this class restores exactly what was
        // applied. Every bug of this shape in this repository has been about the *second* call.
        CoordinatorView view = copies(
            Map.of(
                0,
                List.of(copy(LOCAL, ShardRole.SEARCH_REPLICA, ShardState.STARTED), copy(PEER, ShardRole.PRIMARY, ShardState.STARTED))
            )
        );
        DesiredState desired = desiredHere(Map.of(0, ShardRole.SEARCH_REPLICA));

        ClusterState first = coordinate(ClusterState.EMPTY_STATE, view, desired);
        ClusterState second = coordinate(first, view, desired);

        IndexMetadata before = first.metadata().index(indexName());
        IndexMetadata after = second.metadata().index(indexName());
        assertTrue(
            "the settings must be the ones already applied, by the server's own comparison",
            org.opensearch.index.IndexSettings.same(before.getSettings(), after.getSettings())
        );
        assertEquals("so the settings version must not move", before.getSettingsVersion(), after.getSettingsVersion());
        assertEquals("nor the metadata version", before.getVersion(), after.getVersion());
        assertTrue("and the whole state must be a fixed point", StateEquivalence.sameContent(first, second));
    }

    public void testTheSearchReplicaCountDoesNotGrowEachPass() {
        // It did, because the count was derived by adding the peers' copies to whatever the metadata already said --
        // and after one pass the metadata already included them. Deriving it from the routing table instead makes it
        // idempotent, which is the property that matters: the same table must always produce the same number.
        CoordinatorView view = copies(
            Map.of(
                0,
                List.of(copy(LOCAL, ShardRole.PRIMARY, ShardState.STARTED), copy(PEER, ShardRole.SEARCH_REPLICA, ShardState.STARTED))
            )
        );
        DesiredState desired = desiredHere(Map.of(0, ShardRole.PRIMARY));

        ClusterState state = coordinate(ClusterState.EMPTY_STATE, view, desired);
        for (int pass = 0; pass < 5; pass++) {
            state = coordinate(state, view, desired);
            assertEquals(
                "one search replica in the table means one in the metadata, on every pass",
                1,
                state.metadata().index(indexName()).getNumberOfSearchOnlyReplicas()
            );
        }
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
            List.of(CoordinatorView.RoutedIndex.wholeIndex(ACME, config(), placement(Map.of(0, List.of(copy(PEER, ShardRole.PRIMARY, ShardState.STARTED)))))),
            Map.of(),
            List.of()
        );

        ClusterState state = new CoordinatorState(holdsNothing(), TestFixtures.localNode(LOCAL), view, MappingSource.WRAPPED)
            .buildClusterState(ClusterState.EMPTY_STATE);

        assertNull("no routable copy means no shard table at all", state.routingTable().index(new Index(indexName(), UUID)).shard(0));
        assertEquals("and no node added", 1, state.nodes().getSize());
    }

    public void testACoordinatorStatesTheReplicationTypeAsTheDataPathDoes() {
        // This node holds no copy, so the metadata is this class's own, and it must describe the index as DataNodeState
        // would: DOCUMENT where the document does not say.
        IndexConfig unstated = new IndexConfig(
            "my-index",
            UUID,
            1788969977933L,
            true,
            Map.of("index.number_of_shards", "1", "index.number_of_replicas", "0"),
            Map.of(),
            3041
        );
        CoordinatorView view = new CoordinatorView(
            List.of(CoordinatorView.RoutedIndex.wholeIndex(ACME, unstated, placement(Map.of(0, List.of(copy(PEER, ShardRole.PRIMARY, ShardState.STARTED)))))),
            Map.of(NodeId.of(PEER), peer(PEER)),
            List.of()
        );

        ClusterState state = coordinate(ClusterState.EMPTY_STATE, view);

        assertEquals("DOCUMENT", state.metadata().index(indexName()).getSettings().get("index.replication.type"));
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

    // ---------------------------------------------------------------- peers, not coordination

    public void testANodeHoldingASearchReplicaLearnsThePrimaryAndNoOtherReplica() {
        // The five-node shape an operator brought up: one primary and four search replicas of a one-shard index,
        // and every node's /_cluster/state knowing all five copies. A search replica needs the primary, because
        // RoutingNodes requires index.blocks.search_only on a table with no primary and that block disables the
        // timer that does the polling. It needs nothing whatever from the other three replicas.
        //
        // The cost of the surplus was never size. It is that all five copies republish their cluster state whenever
        // any one of them changes state -- quadratic in copies per shard, on the feature whose purpose is scaling
        // reads out.
        ClusterState state = peerResolved(ShardRole.SEARCH_REPLICA);

        IndexShardRoutingTable shard = state.routingTable().index(state.metadata().index(indexName()).getIndex()).shard(0);
        assertEquals("its own copy and the primary, and nothing else", 2, shard.size());
        assertEquals(1, shard.shards().stream().filter(ShardRouting::primary).count());
        assertEquals(1, shard.shards().stream().filter(ShardRouting::isSearchOnly).count());

        // The count has to describe this table, not the fleet's -- the M8 defect, which is avoided here by counting
        // what went into the table rather than what the projection reported.
        assertEquals(1, state.metadata().index(indexName()).getNumberOfSearchOnlyReplicas());
        // A primary is present, so the marker that would stop this replica polling is not.
        assertFalse(
            state.metadata().index(indexName()).getSettings().hasValue(IndexMetadata.INDEX_BLOCKS_SEARCH_ONLY_SETTING.getKey())
        );
    }

    public void testANodeHoldingThePrimaryLearnsNoCopiesAtAll() {
        // A primary uploads segments and the replicas poll. It does not need to know who they are, and the
        // UnavailableShardsException that used to argue otherwise is a primary pushing checkpoints nothing reads.
        ClusterState state = peerResolved(ShardRole.PRIMARY);

        IndexShardRoutingTable shard = state.routingTable().index(state.metadata().index(indexName()).getIndex()).shard(0);
        assertEquals("only its own copy", 1, shard.size());
        assertTrue(shard.shards().get(0).primary());
        assertEquals(0, state.metadata().index(indexName()).getNumberOfSearchOnlyReplicas());
        assertEquals("and only itself in the node list", 1, state.nodes().getSize());
    }

    public void testNarrowedPeerResolutionIsAFixedPoint() {
        // Everything here is applied every pass, so the second one has to produce the same state -- including the
        // same allocation ids for the peers it did keep.
        ClusterState first = peerResolved(ShardRole.SEARCH_REPLICA);
        ClusterState second = new CoordinatorState(
            new DataNodeState(
                CLUSTER,
                TestFixtures.localNode(LOCAL),
                holdingOnly(Map.of(0, ShardRole.SEARCH_REPLICA)),
                MappingSource.WRAPPED,
                LocalShardStore.EMPTY
            ),
            TestFixtures.localNode(LOCAL),
            fiveCopies(Map.of(0, ShardRole.SEARCH_REPLICA)),
            MappingSource.WRAPPED
        ).buildClusterState(first);

        assertTrue("and the whole state must be a fixed point", StateEquivalence.sameContent(first, second));
    }

    public void testADocumentReplicationPrimaryKeepsItsReplicas() {
        // The narrowing must not reach document replication, and this is the one arm where getting it wrong is
        // silent. A docrep primary's replication group is exactly the copies in its own routing table --
        // ReplicationOperation sends to what ReplicationTracker found there -- so a primary that knows no replicas
        // does not fail to push, it acknowledges writes that only it holds. Nothing logs that.
        ClusterState state = docrepPrimary(ClusterState.EMPTY_STATE, copy(PEER, ShardRole.REPLICA, ShardState.STARTED), holdingPeer());

        IndexShardRoutingTable shard = state.routingTable().index(state.metadata().index(indexName()).getIndex()).shard(0);
        assertEquals("a docrep primary must keep the replica it replicates to", 2, shard.size());
        assertEquals(2, state.nodes().getSize());
    }

    public void testADocrepPeerIsRoutedUnderTheAllocationIdItsNodePublished() {
        // Peer recovery waits until the recovering replica's own id is in the primary's routing table, so an id minted
        // here, as every peer's once was, is one no recovery ever matches.
        ClusterState first = docrepPrimary(ClusterState.EMPTY_STATE, copy(PEER, ShardRole.REPLICA, ShardState.INITIALIZING));

        ShardRouting replica = peerCopy(first);
        assertEquals(allocationIdOf(PEER), replica.allocationId().getId());
        assertTrue(replica.initializing());

        ClusterState second = docrepPrimary(first, copy(PEER, ShardRole.REPLICA, ShardState.INITIALIZING));
        assertEquals(replica, peerCopy(second));
        assertTrue("the pass after changes nothing", StateEquivalence.sameContent(first, second));

        ClusterState started = docrepPrimary(second, copy(PEER, ShardRole.REPLICA, ShardState.STARTED), holdingPeer());
        assertTrue(peerCopy(started).started());
        assertEquals("the same copy once started, so the same id", allocationIdOf(PEER), peerCopy(started).allocationId().getId());
    }

    public void testADocrepPeerWithNoPublishedAllocationIdIsLeftOut() {
        // "A copy without an id is published without one", and a node must not mint one in its place.
        ClusterState state = docrepPrimary(ClusterState.EMPTY_STATE, copy(PEER, ShardRole.REPLICA, ShardState.STARTED, null));

        IndexShardRoutingTable shard = state.routingTable().index(state.metadata().index(indexName()).getIndex()).shard(0);
        assertEquals("only this node's own copy: " + shard, 1, shard.size());
    }

    public void testADocrepPeerBuiltAgainIsANewEntry() {
        // Its node built the copy again -- after a failure, say -- so it has a new id, and the entry reused from the last
        // pass names one that no longer exists.
        // Initializing: a copy its node has just built again is recovering, and is routed whether or not it is in sync.
        ClusterState before = docrepPrimary(ClusterState.EMPTY_STATE, copy(PEER, ShardRole.REPLICA, ShardState.INITIALIZING));

        ClusterState after = docrepPrimary(before, copy(PEER, ShardRole.REPLICA, ShardState.INITIALIZING, "aid-rebuilt"));

        assertEquals("aid-rebuilt", peerCopy(after).allocationId().getId());
    }

    // ---------------------------------------------------------------- a docrep primary's in-sync set (finding 12)

    public void testADocrepPrimaryCallsInSyncWhatItsTrackerHolds() {
        // A started replica that is in neither the cluster state's in-sync set nor initializing is dropped by
        // ReplicationTracker.updateFromClusterManager, and stops receiving writes without anything saying so.
        IndexStatus.Copy replica = copy(PEER, ShardRole.REPLICA, ShardState.STARTED);
        ClusterState first = docrepPrimary(ClusterState.EMPTY_STATE, replica, TrackedInSync.UNKNOWN);
        String primary = localPrimaryId(first);
        assertEquals("before the tracker can say, the primary alone", Set.of(primary), inSync(first));
        TrackedInSync tracked = shardId -> Set.of(primary, allocationIdOf(PEER));

        ClusterState second = docrepPrimary(first, replica, tracked);

        assertEquals(Set.of(primary, allocationIdOf(PEER)), inSync(second));
        IndexMetadata before = first.metadata().index(indexName());
        IndexMetadata after = second.metadata().index(indexName());
        assertEquals("a metadata change, so the version moves by one", before.getVersion() + 1, after.getVersion());
        assertEquals("and not a settings change", before.getSettingsVersion(), after.getSettingsVersion());

        ClusterState third = docrepPrimary(second, replica, tracked);
        assertTrue("the pass after changes nothing", StateEquivalence.sameContent(second, third));
        assertEquals(after.getVersion(), third.metadata().index(indexName()).getVersion());
    }

    public void testAStartedCopyThePrimaryDoesNotHoldInSyncIsNotRouted() {
        // ReplicationTracker asserts that every copy in the routing table is tracked, and drops a started one that is not
        // in the in-sync set (TrackedInSyncReplicationTests shows the server rejecting it). So such a copy is left out of
        // the primary's table, whether the tracker cannot say -- after a restart or a promotion -- or says it is not in sync.
        IndexStatus.Copy replica = copy(PEER, ShardRole.REPLICA, ShardState.STARTED);

        ClusterState unknown = docrepPrimary(ClusterState.EMPTY_STATE, replica, TrackedInSync.UNKNOWN);
        assertEquals(1, shardTable(unknown).size());

        String primary = localPrimaryId(unknown);
        ClusterState notHeld = docrepPrimary(unknown, replica, shardId -> Set.of(primary));
        assertEquals(1, shardTable(notHeld).size());
        assertEquals(Set.of(primary), inSync(notHeld));
        assertTrue("and that is a fixed point", StateEquivalence.sameContent(notHeld, docrepPrimary(notHeld, replica, shardId -> Set.of(primary))));
    }

    public void testAnInitializingCopyIsRoutedWhetherOrNotThePrimaryHoldsIt() {
        // That is how a copy is recovered and brought into the set.
        ClusterState state = docrepPrimary(ClusterState.EMPTY_STATE, copy(PEER, ShardRole.REPLICA, ShardState.INITIALIZING), TrackedInSync.UNKNOWN);

        assertEquals(2, shardTable(state).size());
        assertTrue(peerCopy(state).initializing());
    }

    public void testACopyTheTrackerHoldsThatIsNotInTheTableIsNotInSync() {
        // The tracker keeps a copy for as long as the cluster state calls it in sync, so without this the set would feed
        // itself: a copy that left would stay in it, and hold back the global checkpoint, for ever.
        IndexStatus.Copy replica = copy(PEER, ShardRole.REPLICA, ShardState.STARTED);
        ClusterState first = docrepPrimary(ClusterState.EMPTY_STATE, replica, TrackedInSync.UNKNOWN);
        String primary = localPrimaryId(first);

        ClusterState second = docrepPrimary(first, replica, shardId -> Set.of(primary, "aid-long-gone"));

        assertEquals(Set.of(primary), inSync(second));
    }

    public void testAnInitializingCopyTheTrackerHoldsIsInSync() {
        // The primary marks a copy in sync as its recovery finishes, before the copy's own node has started it.
        IndexStatus.Copy replica = copy(PEER, ShardRole.REPLICA, ShardState.INITIALIZING);
        ClusterState first = docrepPrimary(ClusterState.EMPTY_STATE, replica, TrackedInSync.UNKNOWN);
        String primary = localPrimaryId(first);

        ClusterState second = docrepPrimary(first, replica, shardId -> Set.of(primary, allocationIdOf(PEER)));

        assertEquals(Set.of(primary, allocationIdOf(PEER)), inSync(second));
    }

    public void testASegmentReplicatedPrimarysTrackerIsNotAsked() {
        IndexConfig segment = new IndexConfig(
            "my-index",
            UUID,
            1788969977933L,
            true,
            Map.of("index.number_of_shards", "1", "index.number_of_replicas", "0", "index.replication.type", "SEGMENT"),
            Map.of("properties", Map.of("host", Map.of("type", "keyword"))),
            3041
        );
        docrepPrimary(ClusterState.EMPTY_STATE, copy(PEER, ShardRole.SEARCH_REPLICA, ShardState.STARTED), shardId -> {
            throw new AssertionError("a segment-replicated primary's tracker must not be asked about " + shardId);
        }, segment);
    }

    // ---------------------------------------------------------------- a lost primary (seen on a four-node run)

    // The run: one docrep shard, its primary on one node and replicas on three. The primary's node was stopped, and when
    // the supervisor marked it lost, every replica's node exited on RoutingNodes' "Primary shard routing can't be null
    // for non-search-only indices": its own replica had been declined, and the others' were routed with no primary.

    private static final String PRIMARY_NODE = "AAAAAAAAAAAAAAAAAAAAAA";

    public void testAReplicaWhosePrimaryIsLostStaysOpenUnderAnUnassignedPrimary() {
        ClusterState started = replicaHere(ClusterState.EMPTY_STATE, ShardRole.REPLICA, 5L, true);
        ShardRouting mine = localCopy(started);
        assertTrue(mine.started());

        ClusterState lost = replicaHere(started, ShardRole.REPLICA, 5L, false);

        lost.getRoutingNodes();
        IndexShardRoutingTable shard = shardTable(lost);
        assertEquals("the same open copy, not declined and closed", mine, localCopy(lost));
        assertNotNull("a primary entry, for RoutingNodes", shard.primaryShard());
        assertTrue("and an honest one: it is nowhere this node can reach", shard.primaryShard().unassigned());
        assertTrue("the other started replica is still routed", shard.shards().stream().anyMatch(copy -> PEER.equals(copy.currentNodeId())));
        assertTrue("and the pass after changes nothing", StateEquivalence.sameContent(lost, replicaHere(lost, ShardRole.REPLICA, 5L, false)));
    }

    public void testAReplicaPromotedAfterItsPrimaryWasLostIsPromotedInPlace() {
        // The point of keeping it open: the same allocation, at the new term, which is the promotion updateShardState
        // performs -- not a primary rebuilt from its store.
        ClusterState lost = replicaHere(replicaHere(ClusterState.EMPTY_STATE, ShardRole.REPLICA, 5L, true), ShardRole.REPLICA, 5L, false);
        ShardRouting replica = localCopy(lost);

        ClusterState promoted = replicaHere(lost, ShardRole.PRIMARY, 6L, false);

        promoted.getRoutingNodes();
        ShardRouting primary = localCopy(promoted);
        assertTrue(primary.primary());
        assertTrue(primary.started());
        assertEquals(replica.allocationId().getId(), primary.allocationId().getId());
        assertEquals(6L, promoted.metadata().index(indexName()).primaryTerm(0));
        assertEquals("no placeholder once there is a primary", 1, shardTable(promoted).shards().stream().filter(ShardRouting::primary).count());
    }

    public void testAnInitializingCopyIsLeftOutWhileThePrimaryIsUnassigned() {
        // RoutingNodes asserts that a copy recovering from its primary has an assigned one.
        ClusterState started = replicaHere(ClusterState.EMPTY_STATE, ShardRole.REPLICA, 5L, true);

        ClusterState lost = replicaHere(started, ShardRole.REPLICA, 5L, false, ShardState.INITIALIZING);

        lost.getRoutingNodes();
        assertTrue(shardTable(lost).shards().stream().noneMatch(ShardRouting::initializing));
    }

    /**
     * A node holding a copy of a docrep index, with the primary on {@link #PRIMARY_NODE} and another replica on {@link #PEER}.
     *
     * @param primaryReachable whether the primary's node is in the peer view; when the supervisor marks it lost, it is not
     */
    private static ClusterState replicaHere(ClusterState previous, ShardRole mine, long term, boolean primaryReachable) {
        return replicaHere(previous, mine, term, primaryReachable, ShardState.STARTED);
    }

    private static ClusterState replicaHere(ClusterState previous, ShardRole mine, long term, boolean primaryReachable, ShardState peerState) {
        List<IndexStatus.Copy> copies = List.of(
            copy(PRIMARY_NODE, ShardRole.PRIMARY, ShardState.STARTED),
            copy(PEER, ShardRole.REPLICA, peerState),
            copy(LOCAL, ShardRole.REPLICA, ShardState.STARTED)
        );
        Map<NodeId, NodeStatus> peers = new LinkedHashMap<>();
        if (primaryReachable) {
            peers.put(NodeId.of(PRIMARY_NODE), peer(PRIMARY_NODE));
        }
        peers.put(NodeId.of(PEER), peer(PEER));
        CoordinatorView view = new CoordinatorView(
            List.of(new CoordinatorView.RoutedIndex(ACME, docrepConfig(), placement(Map.of(0, copies)), Set.of(0), Map.of(0, mine))),
            peers,
            List.of()
        );
        DesiredState.IndexKey key = new DesiredState.IndexKey(ACME, UUID);
        DesiredState desired = PeerRecovery.reduce(
            new DesiredState(
                42,
                Map.of(key, new DesiredState.DesiredIndex(key, docrepConfig(), Map.of(0, mine), Map.of(0, term))),
                List.of(),
                List.of()
            ),
            view,
            previous
        );
        ClusterState built = new CoordinatorState(
            new DataNodeState(CLUSTER, TestFixtures.localNode(LOCAL), desired, MappingSource.WRAPPED, LocalShardStore.EMPTY),
            TestFixtures.localNode(LOCAL),
            view,
            MappingSource.WRAPPED
        ).buildClusterState(previous);
        return primaryReachable ? startedHere(built) : built;
    }

    /** What the server does once this node's copy has recovered. */
    private static ClusterState startedHere(ClusterState state) {
        Index index = state.metadata().index(indexName()).getIndex();
        org.opensearch.cluster.routing.IndexRoutingTable.Builder routing = org.opensearch.cluster.routing.IndexRoutingTable.builder(index);
        for (IndexShardRoutingTable shard : state.routingTable().index(index)) {
            for (ShardRouting copy : shard) {
                routing.addShard(copy.initializing() && LOCAL.equals(copy.currentNodeId()) ? copy.moveToStarted() : copy);
            }
        }
        return ClusterState.builder(state).routingTable(org.opensearch.cluster.routing.RoutingTable.builder().add(routing).build()).build();
    }

    private static ShardRouting localCopy(ClusterState state) {
        return shardTable(state).shards().stream().filter(copy -> LOCAL.equals(copy.currentNodeId())).findFirst().orElseThrow();
    }

    /** A node holding the primary of a docrep index, given the projection's copy of its replica. */
    private static ClusterState docrepPrimary(ClusterState previous, IndexStatus.Copy replica) {
        return docrepPrimary(previous, replica, TrackedInSync.UNKNOWN, docrepConfig());
    }

    private static ClusterState docrepPrimary(ClusterState previous, IndexStatus.Copy replica, TrackedInSync tracked) {
        return docrepPrimary(previous, replica, tracked, docrepConfig());
    }

    private static ClusterState docrepPrimary(ClusterState previous, IndexStatus.Copy replica, TrackedInSync tracked, IndexConfig config) {
        List<IndexStatus.Copy> copies = List.of(copy(LOCAL, ShardRole.PRIMARY, ShardState.STARTED), replica);
        Map<NodeId, NodeStatus> peers = new LinkedHashMap<>();
        copies.forEach(copy -> peers.put(copy.node(), peer(copy.node().value())));
        CoordinatorView view = new CoordinatorView(
            List.of(new CoordinatorView.RoutedIndex(ACME, config, placement(Map.of(0, copies)), Set.of(0), Map.of(0, ShardRole.PRIMARY))),
            peers,
            List.of()
        );
        return new CoordinatorState(
            new DataNodeState(
                CLUSTER,
                TestFixtures.localNode(LOCAL),
                new DesiredState(
                    42,
                    Map.of(
                        new DesiredState.IndexKey(ACME, UUID),
                        new DesiredState.DesiredIndex(new DesiredState.IndexKey(ACME, UUID), config, Map.of(0, ShardRole.PRIMARY))
                    ),
                    List.of(),
                    List.of()
                ),
                MappingSource.WRAPPED,
                LocalShardStore.EMPTY
            ),
            TestFixtures.localNode(LOCAL),
            view,
            MappingSource.WRAPPED,
            tracked
        ).buildClusterState(previous);
    }

    /** A tracker that holds the peer's copy in sync, as a primary does once that copy has recovered from it. */
    private static TrackedInSync holdingPeer() {
        return shardId -> Set.of(allocationIdOf(PEER));
    }

    private static IndexShardRoutingTable shardTable(ClusterState state) {
        return state.routingTable().index(state.metadata().index(indexName()).getIndex()).shard(0);
    }

    private static String localPrimaryId(ClusterState state) {
        IndexShardRoutingTable shard = state.routingTable().index(state.metadata().index(indexName()).getIndex()).shard(0);
        return shard.primaryShard().allocationId().getId();
    }

    private static Set<String> inSync(ClusterState state) {
        return state.metadata().index(indexName()).inSyncAllocationIds(0);
    }

    private static ShardRouting peerCopy(ClusterState state) {
        IndexShardRoutingTable shard = state.routingTable().index(state.metadata().index(indexName()).getIndex()).shard(0);
        return shard.shards().stream().filter(copy -> PEER.equals(copy.currentNodeId())).findFirst().orElseThrow();
    }

    /** The same index, under document replication, which is the case the narrowing must not touch. */
    private static IndexConfig docrepConfig() {
        return new IndexConfig(
            "my-index",
            UUID,
            1788969977933L,
            true,
            Map.of("index.number_of_shards", "1", "index.number_of_replicas", "1", "index.replication.type", "DOCUMENT"),
            Map.of("properties", Map.of("host", Map.of("type", "keyword"))),
            3041
        );
    }

    /** One primary elsewhere, this node plus three others holding search replicas, and no coordinator role. */
    private static ClusterState peerResolved(ShardRole mine) {
        return new CoordinatorState(
            new DataNodeState(
                CLUSTER,
                TestFixtures.localNode(LOCAL),
                holdingOnly(Map.of(0, mine)),
                MappingSource.WRAPPED,
                LocalShardStore.EMPTY
            ),
            TestFixtures.localNode(LOCAL),
            fiveCopies(Map.of(0, mine)),
            MappingSource.WRAPPED
        ).buildClusterState(ClusterState.EMPTY_STATE);
    }

    /**
     * The projection for a one-shard index with five copies, seen by a node that holds one of them and coordinates
     * nothing — so the {@code RoutedIndex} carries this node's roles and {@code needs} decides the rest.
     */
    private static CoordinatorView fiveCopies(Map<Integer, ShardRole> localRoles) {
        String primaryNode = localRoles.get(0) == ShardRole.PRIMARY ? LOCAL : PEER;
        List<IndexStatus.Copy> copies = new java.util.ArrayList<>();
        copies.add(copy(primaryNode, ShardRole.PRIMARY, ShardState.STARTED));
        for (String replica : List.of("replica-one-aaaaaaaaaa", "replica-two-aaaaaaaaaa", "replica-three-aaaaaaa")) {
            copies.add(copy(replica, ShardRole.SEARCH_REPLICA, ShardState.STARTED));
        }
        if (localRoles.get(0) == ShardRole.SEARCH_REPLICA) {
            copies.add(copy(LOCAL, ShardRole.SEARCH_REPLICA, ShardState.STARTED));
        }

        Map<NodeId, NodeStatus> peers = new LinkedHashMap<>();
        copies.forEach(copy -> peers.put(copy.node(), peer(copy.node().value())));
        return new CoordinatorView(
            List.of(new CoordinatorView.RoutedIndex(ACME, config(), placement(Map.of(0, copies)), Set.of(0), localRoles)),
            peers,
            List.of()
        );
    }

    /** Holds a copy, coordinates nothing — which is what makes this peer resolution rather than routing. */
    private static DesiredState holdingOnly(Map<Integer, ShardRole> shards) {
        DesiredState.IndexKey key = new DesiredState.IndexKey(ACME, UUID);
        return new DesiredState(42, Map.of(key, new DesiredState.DesiredIndex(key, config(), shards)), List.of(), List.of());
    }

    /** With an allocation id published for it, as a supervisor carrying finding 12 does for a docrep copy. */
    private static IndexStatus.Copy copy(String node, ShardRole role, ShardState state) {
        return copy(node, role, state, allocationIdOf(node));
    }

    private static IndexStatus.Copy copy(String node, ShardRole role, ShardState state, String allocationId) {
        return new IndexStatus.Copy(NodeId.of(node), role, state, state == ShardState.STARTED, allocationId, null);
    }

    private static String allocationIdOf(String node) {
        return "aid-" + node;
    }

    private static CoordinatorView copies(Map<Integer, List<IndexStatus.Copy>> shards) {
        Map<NodeId, NodeStatus> peers = new LinkedHashMap<>();
        shards.values().forEach(copies -> copies.forEach(copy -> peers.put(copy.node(), peer(copy.node().value()))));
        return new CoordinatorView(
            List.of(CoordinatorView.RoutedIndex.wholeIndex(ACME, config(), placement(shards))),
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
            // Distinct per node, because an address is host and port together: DiscoveryNodes.Builder refuses two
            // node ids claiming the same one, and real co-located nodes are on different ports. A fixture that gave
            // them one port was testing an impossible fleet.
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
