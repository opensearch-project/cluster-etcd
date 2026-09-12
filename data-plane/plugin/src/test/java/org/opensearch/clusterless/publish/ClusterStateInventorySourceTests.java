/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.clusterless.publish;

import org.junit.jupiter.api.Test;
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

import java.net.InetAddress;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

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
class ClusterStateInventorySourceTests {

    private static final String LOCAL = "pv1sDMfZRxSTMiW5VrbN9w";
    private static final String OTHER = "72ZSKeAbRxSTMiW5VrbN9w";
    private static final String INDEX_UUID = "uzKxLWZu4nPSrTknEcDpoA";

    @Test
    void aNodeWithNoRoutingNodeHoldsNothingRatherThanFailing() {
        // Before the first assignment is applied there is no routing node for this node at all. "I hold
        // nothing" is a fact worth publishing; it is not the same as "I could not tell", which would be an
        // exception and would leave the last inventory standing.
        ClusterState empty = ClusterState.builder(new ClusterName("clusterless")).nodes(nodes()).build();
        ClusterStateInventorySource source = source(empty, ClusterStateInventorySource.ShardSizes.UNKNOWN);

        assertEquals(List.of(), source.heldCopies());
    }

    @Test
    void aStartedPrimaryIsReportedWithItsTenantUuidAndSize() {
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
        assertEquals(4_823_012L, copy.sizeBytes());
    }

    @Test
    void anInitializingCopyIsReportedWithNoSizeAtAll() {
        // D3.5 and D3.8 together: the copy must appear -- a projection filtered to started copies would
        // deadlock a replica waiting for its primary -- and it must appear without a size, because zero is
        // a real size and would tell a disk-aware decider the copy costs nothing.
        ClusterState state = stateWith(initializingShard(0, LOCAL, true));
        ClusterStateInventorySource source = source(state, shardId -> {
            throw new AssertionError("an initializing copy must not be measured: its store is a partial one");
        });

        ShardInventory.HeldCopy copy = source.heldCopies().get(0);
        assertEquals(ShardState.INITIALIZING, copy.state());
        assertEquals(ShardRole.PRIMARY, copy.role(), "a copy knows what it is going to be before it can serve");
        assertNull(copy.sizeBytes());
    }

    @Test
    void aSizeThatCannotBeMeasuredIsOmittedRatherThanGuessed() {
        // Absent means unknown, and the supervisor fills the gap from the largest copy of the same shard
        // elsewhere in the pool. A zero would be believed.
        ClusterState state = stateWith(startedPrimary(0, LOCAL));
        ClusterStateInventorySource source = source(state, shardId -> null);

        assertNull(source.heldCopies().get(0).sizeBytes());
    }

    @Test
    void onlyThisNodesCopiesAreReported() {
        // The document says what this node has. A node reporting a copy held elsewhere would have the
        // supervisor believe a shard has two copies where it has one.
        ClusterState state = stateWith(startedPrimary(0, LOCAL), startedReplica(1, OTHER));
        ClusterStateInventorySource source = source(state, shardId -> 1L);

        assertEquals(1, source.heldCopies().size());
        assertEquals(0, source.heldCopies().get(0).shard());
    }

    @Test
    void aSearchReplicaIsNotReportedAsAPrimary() {
        // primary() and isSearchOnly() are independent flags rather than one enum, so testing for the
        // primary first would report a search replica as one.
        ClusterState state = stateWith(startedSearchReplica(0, LOCAL));
        assertEquals(ShardRole.SEARCH_REPLICA, source(state, shardId -> 1L).heldCopies().get(0).role());
    }

    @Test
    void anIndexWithNoTenantIsLeftOutRatherThanAttributedToOne() {
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
