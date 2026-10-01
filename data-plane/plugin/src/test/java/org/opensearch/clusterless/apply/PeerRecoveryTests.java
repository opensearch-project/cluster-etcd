/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.clusterless.apply;

import org.opensearch.clusterless.contract.IndexConfig;
import org.opensearch.clusterless.contract.IndexStatus;
import org.opensearch.clusterless.contract.NodeId;
import org.opensearch.clusterless.contract.NodeRole;
import org.opensearch.clusterless.contract.NodeStatus;
import org.opensearch.clusterless.contract.ShardRole;
import org.opensearch.clusterless.contract.ShardState;
import org.opensearch.clusterless.contract.TenantId;
import org.opensearch.test.OpenSearchTestCase;

import java.util.EnumSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Whether a document-replication replica may be built, which is the one thing M9 has to decide before building it.
 *
 * <p>The rule exists because of an assertion rather than a preference: {@code RoutingNodes} requires a primary entry
 * for every shard in a routing table unless {@code index.blocks.search_only} is set, and a docrep replica has no
 * marker it could honestly set. Tripping it does not decline a copy — it runs on the cluster applier thread and
 * exits the node.
 */
public class PeerRecoveryTests extends OpenSearchTestCase {

    private static final TenantId ACME = TenantId.of("acme");
    private static final String UUID = "uzKxLWZu4nPSrTknEcDpoA";
    private static final String PRIMARY_NODE = "AAAAAAAAAAAAAAAAAAAAAA";

    public void testAReplicaIsBuiltWhenItsPrimaryIsVisible() {
        DesiredState reduced = PeerRecovery.reduce(
            holding(Map.of(0, ShardRole.REPLICA), documentReplicated()),
            viewWith(ShardState.STARTED, true)
        );

        assertEquals(1, reduced.copyCount());
        assertTrue(reduced.deferred().toString(), reduced.deferred().isEmpty());
    }

    public void testAnInitializingPrimaryCountsAsVisible() {
        // The same test the fabricator applies, and it has to be: a copy this step called unbuildable that the
        // fabricator would have routed to, or the reverse, is the class of bug that takes a day to find.
        DesiredState reduced = PeerRecovery.reduce(
            holding(Map.of(0, ShardRole.REPLICA), documentReplicated()),
            viewWith(ShardState.INITIALIZING, true)
        );

        assertEquals(1, reduced.copyCount());
    }

    public void testAReplicaIsDeclinedWhenNoPrimaryIsInTheProjectionYet() {
        // The ordinary first pass: the supervisor has placed the replica and this node has not yet observed a
        // primary anywhere. Declined with a reason and retried, not built into a state the applier will reject.
        DesiredState reduced = PeerRecovery.reduce(
            holding(Map.of(0, ShardRole.REPLICA), documentReplicated()),
            new CoordinatorView(
                List.of(
                    new CoordinatorView.RoutedIndex(
                        ACME,
                        documentReplicated(),
                        placement(List.of(copy("BBBBBBBBBBBBBBBBBBBBBB", ShardRole.REPLICA, ShardState.STARTED))),
                        Set.of(0),
                        Map.of(0, ShardRole.REPLICA)
                    )
                ),
                Map.of(NodeId.of("BBBBBBBBBBBBBBBBBBBBBB"), peer("BBBBBBBBBBBBBBBBBBBBBB")),
                List.of()
            )
        );

        assertEquals(0, reduced.copyCount());
        assertEquals(1, reduced.deferred().size());
        assertTrue(reduced.deferred().get(0), reduced.deferred().get(0).contains("recovers from its primary"));
    }

    public void testAReplicaIsDeclinedWhenItsPrimaryPublishedNoAllocationId() {
        // CoordinatorState leaves such a primary out of the routing table, since recovery could never match a minted id,
        // so building the replica would put it beside no primary at all.
        IndexStatus.Copy primary = new IndexStatus.Copy(NodeId.of(PRIMARY_NODE), ShardRole.PRIMARY, ShardState.STARTED, true, null, null);
        DesiredState reduced = PeerRecovery.reduce(
            holding(Map.of(0, ShardRole.REPLICA), documentReplicated()),
            new CoordinatorView(
                List.of(
                    new CoordinatorView.RoutedIndex(ACME, documentReplicated(), placement(List.of(primary)), Set.of(0), Map.of(0, ShardRole.REPLICA))
                ),
                Map.of(NodeId.of(PRIMARY_NODE), peer(PRIMARY_NODE)),
                List.of()
            )
        );

        assertEquals(0, reduced.copyCount());
        assertTrue(reduced.deferred().get(0), reduced.deferred().get(0).contains("published allocation id"));
    }

    public void testAReplicaIsDeclinedWhenThePrimarysNodeCannotBeReached() {
        // The projection has a primary and the peer view resolved no address for its node, so the fabricator would
        // leave it out of the routing table -- which puts this node one applied state away from the assertion.
        DesiredState reduced = PeerRecovery.reduce(
            holding(Map.of(0, ShardRole.REPLICA), documentReplicated()),
            viewWith(ShardState.STARTED, false)
        );

        assertEquals(0, reduced.copyCount());
        assertTrue(reduced.deferred().get(0), reduced.deferred().get(0).contains("reachable"));
    }

    public void testAPrimaryIsNeverDeclinedForWantOfReplicas() {
        // A primary with no replicas replicates to fewer copies than the tenant asked for, which is a health
        // question the supervisor already reports. It is not a reason to refuse to serve.
        DesiredState reduced = PeerRecovery.reduce(
            holding(Map.of(0, ShardRole.PRIMARY), documentReplicated()),
            new CoordinatorView(List.of(), Map.of(), List.of())
        );

        assertEquals(1, reduced.copyCount());
        assertTrue(reduced.deferred().toString(), reduced.deferred().isEmpty());
    }

    public void testASegmentReplicatedIndexIsNotThisStepsBusiness() {
        // A search replica's primary is wanted for freshness rather than for existence, and the search_only marker
        // covers its absence. Reducing it here would decline copies that work.
        DesiredState reduced = PeerRecovery.reduce(
            holding(Map.of(0, ShardRole.SEARCH_REPLICA), segmentReplicated()),
            new CoordinatorView(List.of(), Map.of(), List.of())
        );

        assertEquals(1, reduced.copyCount());
        assertTrue(reduced.deferred().toString(), reduced.deferred().isEmpty());
    }

    public void testDecliningOneShardKeepsTheOthers() {
        // One shard's primary is visible and the other's is not, so the index stays with the buildable half rather
        // than being dropped whole.
        Map<Integer, List<IndexStatus.Copy>> shards = new LinkedHashMap<>();
        shards.put(0, List.of(copy(PRIMARY_NODE, ShardRole.PRIMARY, ShardState.STARTED)));
        shards.put(1, List.of(copy("BBBBBBBBBBBBBBBBBBBBBB", ShardRole.REPLICA, ShardState.STARTED)));

        DesiredState reduced = PeerRecovery.reduce(
            holding(Map.of(0, ShardRole.REPLICA, 1, ShardRole.REPLICA), documentReplicated()),
            new CoordinatorView(
                List.of(
                    new CoordinatorView.RoutedIndex(
                        ACME,
                        documentReplicated(),
                        new IndexStatus("my-index", UUID, 3041, 2, 1, IndexStatus.Replication.DOCUMENT, shards),
                        Set.of(0, 1),
                        Map.of(0, ShardRole.REPLICA, 1, ShardRole.REPLICA)
                    )
                ),
                Map.of(NodeId.of(PRIMARY_NODE), peer(PRIMARY_NODE), NodeId.of("BBBBBBBBBBBBBBBBBBBBBB"), peer("BBBBBBBBBBBBBBBBBBBBBB")),
                List.of()
            )
        );

        assertEquals(1, reduced.copyCount());
        assertEquals(ShardRole.REPLICA, reduced.indices().values().iterator().next().shards().get(0));
        assertNull("shard 1 has no visible primary", reduced.indices().values().iterator().next().shards().get(1));
        assertEquals(1, reduced.deferred().size());
    }

    public void testDecliningOneShardKeepsTheOthersTerms() {
        // Rebuilding the index around the declined shard once dropped every term, so a primary promoted to term 3
        // went into the fabrication at term 1 -- and PrimaryTerms, which runs next, then declined it as a term going
        // backwards.
        DesiredState.IndexKey key = new DesiredState.IndexKey(ACME, UUID);
        DesiredState desired = new DesiredState(
            42,
            Map.of(
                key,
                new DesiredState.DesiredIndex(
                    key,
                    documentReplicated(),
                    Map.of(0, ShardRole.PRIMARY, 1, ShardRole.REPLICA),
                    Map.of(0, 3L, 1, 2L)
                )
            ),
            List.of(),
            List.of()
        );

        DesiredState reduced = PeerRecovery.reduce(desired, new CoordinatorView(List.of(), Map.of(), List.of()));

        DesiredState.DesiredIndex kept = reduced.indices().get(key);
        assertEquals(Map.of(0, ShardRole.PRIMARY), kept.shards());
        assertEquals(3L, kept.primaryTermOf(0));
        assertEquals(1, reduced.deferred().size());
    }

    // ---------------------------------------------------------------- fixtures

    private static DesiredState holding(Map<Integer, ShardRole> shards, IndexConfig config) {
        DesiredState.IndexKey key = new DesiredState.IndexKey(ACME, UUID);
        return new DesiredState(42, Map.of(key, new DesiredState.DesiredIndex(key, config, shards)), List.of(), List.of());
    }

    /** A projection with one primary, in the given state, whose node the view has resolved an address for or not. */
    private static CoordinatorView viewWith(ShardState primaryState, boolean reachable) {
        Map<NodeId, NodeStatus> peers = new LinkedHashMap<>();
        if (reachable) {
            peers.put(NodeId.of(PRIMARY_NODE), peer(PRIMARY_NODE));
        }
        return new CoordinatorView(
            List.of(
                new CoordinatorView.RoutedIndex(
                    ACME,
                    documentReplicated(),
                    placement(List.of(copy(PRIMARY_NODE, ShardRole.PRIMARY, primaryState))),
                    Set.of(0),
                    Map.of(0, ShardRole.REPLICA)
                )
            ),
            peers,
            List.of()
        );
    }

    private static IndexStatus placement(List<IndexStatus.Copy> copies) {
        return new IndexStatus("my-index", UUID, 3041, 1, 1, IndexStatus.Replication.DOCUMENT, Map.of(0, copies));
    }

    private static IndexStatus.Copy copy(String node, ShardRole role, ShardState state) {
        return new IndexStatus.Copy(NodeId.of(node), role, state, state == ShardState.STARTED, "aid-" + node, null);
    }

    private static NodeStatus peer(String nodeId) {
        return new NodeStatus(
            NodeId.of(nodeId),
            "eph-" + nodeId,
            "data-" + nodeId.substring(0, 4),
            EnumSet.of(NodeRole.DATA),
            "127.0.0.1",
            9300 + Math.abs(nodeId.hashCode() % 100),
            "3.9.0",
            Map.of(),
            NodeStatus.Liveness.LIVE
        );
    }

    private static IndexConfig documentReplicated() {
        return config("DOCUMENT");
    }

    private static IndexConfig segmentReplicated() {
        return config("SEGMENT");
    }

    private static IndexConfig config(String replication) {
        return new IndexConfig(
            "my-index",
            UUID,
            1788969977933L,
            true,
            Map.of("index.number_of_shards", "2", "index.number_of_replicas", "1", "index.replication.type", replication),
            Map.of(),
            3041
        );
    }
}
