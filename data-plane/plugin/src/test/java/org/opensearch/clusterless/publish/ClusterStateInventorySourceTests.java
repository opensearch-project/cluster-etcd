/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.clusterless.publish;

import org.opensearch.Version;
import org.opensearch.cluster.ClusterName;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.node.DiscoveryNodeRole;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.cluster.routing.IndexRoutingTable;
import org.opensearch.cluster.routing.RecoverySource;
import org.opensearch.cluster.routing.RoutingTable;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.cluster.routing.UnassignedInfo;
import org.opensearch.clusterless.ClusterlessIndexSettings;
import org.opensearch.clusterless.contract.ShardInventory;
import org.opensearch.clusterless.contract.ShardRole;
import org.opensearch.clusterless.contract.ShardState;
import org.opensearch.clusterless.contract.TenantId;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.common.transport.TransportAddress;
import org.opensearch.core.index.Index;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.test.OpenSearchTestCase;

import java.net.InetAddress;
import java.util.List;
import java.util.Map;
import java.util.Set;


/**
 * Turning the node's own routing table into the contract's inventory.
 *
 * <p>Reading back the state this plugin fabricated sounds circular and is not, because the two say
 * different things: the fabricated state is intent, and the routing table's shard states are the server's
 * answer to it. This is where that answer becomes the document the supervisor plans against, so it is
 * where a mistranslation would make the fleet's view of a node wrong while the node itself was fine.
 *
 * <p>The cluster states here are hand-built rather than obtained from a running node. That is a real
 * limitation and it is the reason the mapping from {@code ShardRoutingState} lives in {@link ShardState}
 * as a total function over the enum: a state OpenSearch adds is then a compilation failure rather than
 * something these fixtures silently never produce. {@code SPLITTING} was found exactly that way.
 */
public class ClusterStateInventorySourceTests extends OpenSearchTestCase {

    private static final String LOCAL = "pv1sDMfZRxSTMiW5VrbN9w";
    private static final String OTHER = "72ZSKeAbRxSTMiW5VrbN9w";
    private static final String INDEX_UUID = "uzKxLWZu4nPSrTknEcDpoA";

    public void testANodeWithNoRoutingNodeHoldsNothingRatherThanFailing() {
        // Before the first assignment is applied there is no routing node for this node at all. "I hold
        // nothing" is a fact worth publishing; it is not the same as "I could not tell", which would be an
        // exception and would leave the last inventory standing.
        ClusterState empty = ClusterState.builder(new ClusterName("clusterless")).nodes(nodes()).build();
        ClusterStateInventorySource source = source(empty, ClusterStateInventorySource.ShardSizes.UNKNOWN);

        assertEquals(List.of(), source.heldCopies());
    }

    public void testAStartedPrimaryIsReportedWithItsTenantUuidAndSize() {
        ClusterState state = stateWith(startedPrimary(0, LOCAL));
        ClusterStateInventorySource source = source(state, shardId -> 4_823_012L);

        List<ShardInventory.HeldCopy> copies = source.heldCopies();
        assertEquals(1, copies.size());
        ShardInventory.HeldCopy copy = copies.get(0);
        assertEquals(TenantId.of("acme"), copy.tenant());
        // The identity is the uuid the control plane gave the index, carried through as the ClusterState's
        // own index uuid -- so Index equality inside the server is already identity in the contract's sense.
        assertEquals(INDEX_UUID, copy.indexUuid());
        // The name is the tenant-scoped one, which is what an operator will search the logs for, and not
        // whatever name the flat ClusterState namespace forced on the index.
        assertEquals("my-index", copy.indexName());
        assertEquals(3041L, copy.configRevision());
        assertEquals(ShardRole.PRIMARY, copy.role());
        assertEquals(ShardState.STARTED, copy.state());
        assertEquals(Long.valueOf(4_823_012L), copy.sizeBytes());
    }

    public void testAnInitializingCopyIsReportedWithNoSizeAtAll() {
        // D3.5 and D3.8 together: the copy must appear -- a projection filtered to started copies would
        // deadlock a replica waiting for its primary -- and it must appear without a size, because zero is
        // a real size and would tell a disk-aware decider the copy costs nothing.
        ClusterState state = stateWith(initializingShard(0, LOCAL, true));
        ClusterStateInventorySource source = source(state, shardId -> {
            throw new AssertionError("an initializing copy must not be measured: its store is a partial one");
        });

        ShardInventory.HeldCopy copy = source.heldCopies().get(0);
        assertEquals(ShardState.INITIALIZING, copy.state());
        assertEquals("a copy knows what it is going to be before it can serve", ShardRole.PRIMARY, copy.role());
        assertNull(copy.sizeBytes());
    }

    public void testASizeThatCannotBeMeasuredIsOmittedRatherThanGuessed() {
        // Absent means unknown, and the supervisor fills the gap from the largest copy of the same shard
        // elsewhere in the pool. A zero would be believed.
        ClusterState state = stateWith(startedPrimary(0, LOCAL));
        ClusterStateInventorySource source = source(state, shardId -> null);

        assertNull(source.heldCopies().get(0).sizeBytes());
    }

    public void testOnlyThisNodesCopiesAreReported() {
        // The document says what this node has. A node reporting a copy held elsewhere would have the
        // supervisor believe a shard has two copies where it has one.
        //
        // Shard 1's copy is a primary rather than a replica, because a replica with no primary anywhere is
        // not a state OpenSearch will build: RoutingNodes asserts "Primary shard routing can't be null for
        // non-search-only indices" in its constructor. Assertions are on under Gradle and under
        // tools/jbuild.sh, so an invalid fixture fails rather than quietly testing something impossible.
        ClusterState state = stateWith(startedPrimary(0, LOCAL), startedPrimary(1, OTHER));
        ClusterStateInventorySource source = source(state, shardId -> 1L);

        assertEquals(1, source.heldCopies().size());
        assertEquals(0, source.heldCopies().get(0).shard());
    }

    public void testASearchReplicaIsNotReportedAsAPrimary() {
        // primary() and isSearchOnly() are independent flags rather than one enum, so testing for the
        // primary first would report a search replica as one.
        //
        // The primary on the other node is not decoration. RoutingNodes refuses a shard with no primary
        // unless the index is marked search-only, which is a real constraint on the fabricator and not just
        // on this fixture: at milestone 7, a node holding nothing but a search replica has no primary to put
        // in its own cluster state, so it will have to either name the primary's node or set
        // index.blocks.search_only. cluster-etcd chose the latter and left a comment saying why; that choice
        // is still open here, and this is where it will be forced.
        ClusterState state = stateWith(startedSearchReplica(0, LOCAL), startedPrimary(0, OTHER));

        List<ShardInventory.HeldCopy> copies = source(state, shardId -> 1L).heldCopies();
        assertEquals(1, copies.size());
        assertEquals(ShardRole.SEARCH_REPLICA, copies.get(0).role());
    }

    public void testAnIndexWithNoTenantIsLeftOutRatherThanAttributedToOne() {
        // It should not happen -- this plugin writes those settings -- but an index with no tenant cannot
        // be named in the contract's vocabulary at all, and inventing a tenant would attribute a shard to
        // somebody.
        IndexMetadata anonymous = IndexMetadata.builder("anonymous")
            .settings(
                Settings.builder()
                    .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
                    .put(IndexMetadata.SETTING_INDEX_UUID, "someOtherUuid12345678A")
                    .build()
            )
            .numberOfShards(1)
            .numberOfReplicas(0)
            .build();

        ShardRouting routing = ShardRouting.newUnassigned(
            new ShardId(anonymous.getIndex(), 0),
            true,
            RecoverySource.EmptyStoreRecoverySource.INSTANCE,
            new UnassignedInfo(UnassignedInfo.Reason.INDEX_CREATED, "created")
        ).initialize(LOCAL, null, ShardRouting.UNAVAILABLE_EXPECTED_SHARD_SIZE).moveToStarted();

        ClusterState state = ClusterState.builder(new ClusterName("clusterless"))
            .nodes(nodes())
            .metadata(Metadata.builder().put(anonymous, false))
            .routingTable(RoutingTable.builder().add(IndexRoutingTable.builder(anonymous.getIndex()).addShard(routing)).build())
            .build();

        assertTrue(source(state, shardId -> 1L).heldCopies().isEmpty());
    }

    // ---------------------------------------------------------------- fixtures

    private static ClusterStateInventorySource source(ClusterState state, ClusterStateInventorySource.ShardSizes sizes) {
        return new ClusterStateInventorySource(() -> state, LOCAL, sizes);
    }

    private static IndexMetadata indexMetadata() {
        return IndexMetadata.builder("acme.my-index")
            .settings(
                Settings.builder()
                    .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
                    .put(IndexMetadata.SETTING_INDEX_UUID, INDEX_UUID)
                    .put(ClusterlessIndexSettings.TENANT.getKey(), "acme")
                    .put(ClusterlessIndexSettings.TENANT_INDEX_NAME.getKey(), "my-index")
                    .put(ClusterlessIndexSettings.CONFIG_REVISION.getKey(), 3041L)
                    .build()
            )
            .numberOfShards(2)
            .numberOfReplicas(1)
            .build();
    }

    private static ClusterState stateWith(ShardRouting... shards) {
        IndexMetadata metadata = indexMetadata();
        IndexRoutingTable.Builder routing = IndexRoutingTable.builder(metadata.getIndex());
        for (ShardRouting shard : shards) {
            routing.addShard(shard);
        }
        return ClusterState.builder(new ClusterName("clusterless"))
            .nodes(nodes())
            .metadata(Metadata.builder().put(metadata, false))
            .routingTable(RoutingTable.builder().add(routing).build())
            .build();
    }

    private static ShardRouting startedPrimary(int shard, String nodeId) {
        return initializingShard(shard, nodeId, true).moveToStarted();
    }

    private static ShardRouting startedReplica(int shard, String nodeId) {
        return initializingShard(shard, nodeId, false).moveToStarted();
    }

    private static ShardRouting startedSearchReplica(int shard, String nodeId) {
        Index index = indexMetadata().getIndex();
        return ShardRouting.newUnassigned(
            new ShardId(index, shard),
            false,
            true,
            RecoverySource.EmptyStoreRecoverySource.INSTANCE,
            new UnassignedInfo(UnassignedInfo.Reason.INDEX_CREATED, "created")
        ).initialize(nodeId, null, ShardRouting.UNAVAILABLE_EXPECTED_SHARD_SIZE).moveToStarted();
    }

    private static ShardRouting initializingShard(int shard, String nodeId, boolean primary) {
        Index index = indexMetadata().getIndex();
        return ShardRouting.newUnassigned(
            new ShardId(index, shard),
            primary,
            primary ? RecoverySource.EmptyStoreRecoverySource.INSTANCE : RecoverySource.PeerRecoverySource.INSTANCE,
            new UnassignedInfo(UnassignedInfo.Reason.INDEX_CREATED, "created")
        ).initialize(nodeId, null, ShardRouting.UNAVAILABLE_EXPECTED_SHARD_SIZE);
    }

    private static DiscoveryNodes nodes() {
        return DiscoveryNodes.builder().localNodeId(LOCAL).add(node(LOCAL, 9300)).add(node(OTHER, 9301)).build();
    }

    /** Distinct ports, because DiscoveryNodes rejects two nodes at one address. */
    private static DiscoveryNode node(String id, int port) {
        return new DiscoveryNode(
            "node-" + id,
            id,
            "ephemeral-" + id,
            "localhost",
            "127.0.0.1",
            new TransportAddress(InetAddress.getLoopbackAddress(), port),
            Map.of(),
            Set.of(DiscoveryNodeRole.DATA_ROLE),
            Version.CURRENT
        );
    }
}
