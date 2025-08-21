/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.cluster.etcd.changeapplier;

import org.opensearch.Version;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.cluster.routing.IndexRoutingTable;
import org.opensearch.cluster.routing.IndexShardRoutingTable;
import org.opensearch.cluster.routing.RecoverySource;
import org.opensearch.cluster.routing.RoutingTable;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.cluster.routing.UnassignedInfo;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.index.Index;
import org.opensearch.core.index.shard.ShardId;

import java.util.Collection;
import java.util.List;
import java.util.Map;

public class CoordinatorNodeState extends NodeState {

    private final Collection<RemoteNode> remoteNodes;
    private final Map<Index, List<List<NodeShardAssignment>>> remoteShardAssignments;

    public CoordinatorNodeState(
        DiscoveryNode localNode,
        Collection<RemoteNode> remoteNodes,
        Map<Index, List<List<NodeShardAssignment>>> remoteShardAssignments
    ) {
        super(localNode);
        this.remoteNodes = remoteNodes;
        this.remoteShardAssignments = remoteShardAssignments;
    }

    @Override
    public ClusterState buildClusterState(ClusterState previous) {
        DiscoveryNodes.Builder nodesBuilder = DiscoveryNodes.builder().localNodeId(localNode.getId()).add(localNode);

        Metadata.Builder metadataBuilder = Metadata.builder();
        RoutingTable.Builder routingTableBuilder = RoutingTable.builder();

        for (Map.Entry<Index, List<List<NodeShardAssignment>>> indexEntry : remoteShardAssignments.entrySet()) {
            int shardNum = 0;

            boolean indexHasPrimary = false;
            IndexRoutingTable.Builder indexRoutingTableBuilder = IndexRoutingTable.builder(indexEntry.getKey());
            for (List<NodeShardAssignment> shardRouting : indexEntry.getValue()) {
                ShardId shardId = new ShardId(indexEntry.getKey(), shardNum);
                IndexShardRoutingTable.Builder shardRoutingTableBuilder = new IndexShardRoutingTable.Builder(shardId);
                boolean shardHasPrimary = false;
                for (NodeShardAssignment shardAssignment : shardRouting) {
                    ShardRole shardRole = shardAssignment.shardRole();
                    if (shardRole == ShardRole.PRIMARY) {
                        if (indexHasPrimary == false && shardNum > 0) {
                            // If the index has primary shards, we need to figure it out from the first shard.
                            throw new IllegalStateException(
                                "Index "
                                    + indexEntry.getKey().getName()
                                    + " has at least one primary shard, but the first shard has no primary assigned."
                            );
                        }
                        indexHasPrimary = true;
                        shardHasPrimary = true;
                    }
                    ShardRouting nodeEntry = ShardRouting.newUnassigned(
                        shardId,
                        shardRole == ShardRole.PRIMARY,
                        shardRole == ShardRole.SEARCH_REPLICA,
                        RecoverySource.EmptyStoreRecoverySource.INSTANCE,
                        new UnassignedInfo(UnassignedInfo.Reason.INDEX_CREATED, "initializing")
                    );
                    nodeEntry = nodeEntry.initialize(shardAssignment.nodeId(), null, ShardRouting.UNAVAILABLE_EXPECTED_SHARD_SIZE);
                    nodeEntry = nodeEntry.moveToStarted();
                    shardRoutingTableBuilder.addShard(nodeEntry);
                }
                if (indexHasPrimary == true && shardHasPrimary == false) {
                    throw new IllegalStateException(
                        "Index "
                            + indexEntry.getKey().getName()
                            + " has a primary shard, but shard "
                            + shardNum
                            + " has no primary assigned."
                    );
                }
                indexRoutingTableBuilder.addIndexShard(shardRoutingTableBuilder.build());
                shardNum++;
            }
            Settings.Builder indexSettings = Settings.builder()
                .put(IndexMetadata.SETTING_INDEX_UUID, indexEntry.getKey().getUUID())
                .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, indexEntry.getValue().size())
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0);
            if (indexHasPrimary == false) {
                indexSettings.put(IndexMetadata.INDEX_BLOCKS_SEARCH_ONLY_SETTING.getKey(), true);
            }
            IndexMetadata indexMetadata = IndexMetadata.builder(indexEntry.getKey().getName()).settings(indexSettings).build();
            routingTableBuilder.add(indexRoutingTableBuilder);
            metadataBuilder.put(indexMetadata, false);
        }
        for (RemoteNode remoteNode : remoteNodes) {
            nodesBuilder.add(remoteNode.toDiscoveryNode());
        }

        return ClusterState.builder(ClusterState.EMPTY_STATE)
            .nodes(nodesBuilder)
            .metadata(metadataBuilder)
            .routingTable(routingTableBuilder.build())
            .build();
    }

}
