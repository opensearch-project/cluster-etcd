/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.etcd.changeapplier;

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
import org.opensearch.core.index.Index;
import org.opensearch.core.index.shard.ShardId;

import java.util.Map;
import java.util.Optional;
import java.util.Set;

public class DataNodeState extends NodeState {
    private final Map<String, IndexMetadata> indices;
    private final Map<String, Map<Integer, ShardRole>> assignedShards;

    public DataNodeState(DiscoveryNode localNode, Map<String, IndexMetadata> indices, Map<String, Map<Integer, ShardRole>> assignedShards) {
        super(localNode);
        // The index metadata and shard assignment should be identical
        assert indices.keySet().equals(assignedShards.keySet());
        this.indices = indices;
        this.assignedShards = assignedShards;
    }

    @Override
    public ClusterState buildClusterState(ClusterState previousState) {
        ClusterState.Builder clusterStateBuilder = ClusterState.builder(ClusterState.EMPTY_STATE);
        DiscoveryNodes.Builder nodesBuilder = DiscoveryNodes.builder().localNodeId(localNode.getId()).add(localNode);

        clusterStateBuilder.nodes(nodesBuilder);
        RoutingTable.Builder routingTableBuilder = RoutingTable.builder();
        Metadata.Builder metadataBuilder = Metadata.builder();
        for (Map.Entry<String, IndexMetadata> entry : indices.entrySet()) {
            IndexMetadata indexMetadata = entry.getValue();
            Index index = indexMetadata.getIndex();

            IndexRoutingTable previousIndexRoutingTable = previousState.routingTable().index(index);
            IndexRoutingTable.Builder indexRoutingTableBuilder = IndexRoutingTable.builder(index);
            IndexMetadata.Builder indexMetadataBuilder = IndexMetadata.builder(indexMetadata);
            for (Map.Entry<Integer, ShardRole> shardRoleEntry :
                    assignedShards.get(index.getName()).entrySet()) {
                int shardNum = shardRoleEntry.getKey();
                ShardRole role = shardRoleEntry.getValue();
                ShardId shardId = new ShardId(indexMetadata.getIndex(), shardNum);
                UnassignedInfo unassignedInfo = new UnassignedInfo(UnassignedInfo.Reason.INDEX_CREATED, "created");
                ShardRouting shardRouting = ShardRouting.newUnassigned(
                    shardId,
                    role == ShardRole.PRIMARY,
                    role == ShardRole.SEARCH_REPLICA,
                    RecoverySource.EmptyStoreRecoverySource.INSTANCE, // TODO: Support other recovery sources
                    unassignedInfo);
                shardRouting = shardRouting.initialize(localNode.getId(), null, ShardRouting.UNAVAILABLE_EXPECTED_SHARD_SIZE);
                IndexShardRoutingTable previousShardRoutingTable = previousIndexRoutingTable == null ? null : previousIndexRoutingTable.shard(shardNum);
                Optional<ShardRouting> previouslyStartedShard = previousShardRoutingTable == null ? Optional.empty() : previousShardRoutingTable
                    .shards()
                    .stream()
                    .filter(sr -> localNode.getId().equals(sr.currentNodeId()))
                    .filter(ShardRouting::started)
                    .findAny();
                if (previouslyStartedShard.isPresent()) {
                    // TODO: Someone needs to moveToStarted the first time. Probably on the local node.
                    shardRouting = shardRouting.moveToStarted();
                }
                indexRoutingTableBuilder.addShard(shardRouting);
                indexMetadataBuilder.putInSyncAllocationIds(shardNum, Set.of(shardRouting.allocationId().getId()));
            }
            routingTableBuilder.add(indexRoutingTableBuilder);
            metadataBuilder.put(indexMetadataBuilder);
        }
        clusterStateBuilder.routingTable(routingTableBuilder.build());
        clusterStateBuilder.metadata(metadataBuilder);

        return clusterStateBuilder.build();
    }

}
