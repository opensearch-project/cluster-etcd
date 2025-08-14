/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
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
import org.opensearch.common.settings.Settings;
import org.opensearch.core.index.Index;
import org.opensearch.core.index.shard.ShardId;

import java.util.Collection;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

public class DataNodeState extends NodeState {
    private final Map<String, IndexMetadata> indices;
    private final Map<String, Set<DataNodeShard>> assignedShards;

    public DataNodeState(DiscoveryNode localNode, Map<String, IndexMetadata> indices, Map<String, Set<DataNodeShard>> assignedShards) {
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
            for (DataNodeShard dataNodeShard : assignedShards.get(index.getName())) {
                int shardNum = dataNodeShard.getShardNum();
                ShardRole role = dataNodeShard.getShardRole();
                ShardId shardId = new ShardId(indexMetadata.getIndex(), shardNum);
                UnassignedInfo unassignedInfo = new UnassignedInfo(UnassignedInfo.Reason.INDEX_CREATED, "created");

                final RecoverySource recoverySource;
                if (role == ShardRole.REPLICA && dataNodeShard.getPrimaryNode().isPresent()) {
                    DiscoveryNode primaryNode = dataNodeShard.getPrimaryNode().get();
                    recoverySource = RecoverySource.PeerRecoverySource.INSTANCE;
                    ShardRouting primaryShardRouting = ShardRouting.newUnassigned(
                                    shardId,
                                    true,
                                    false,
                                    RecoverySource.ExistingStoreRecoverySource.INSTANCE,
                                    unassignedInfo
                            ).initialize(primaryNode.getId(), null, ShardRouting.UNAVAILABLE_EXPECTED_SHARD_SIZE)
                            .moveToStarted();
                    // Add the primary shard routing to the index routing table
                    indexRoutingTableBuilder.addShard(primaryShardRouting);
                } else {
                    recoverySource = RecoverySource.ExistingStoreRecoverySource.INSTANCE; // For primary and search replica
                }
                if (role == ShardRole.PRIMARY && dataNodeShard.getReplicaNodes().isEmpty() == false) {
                    // If this is a primary, we need to add the replica shards
                    for (DiscoveryNode replicaNode : dataNodeShard.getReplicaNodes()) {
                        ShardRouting replicaShardRouting = ShardRouting.newUnassigned(
                                shardId,
                                false,
                                false,
                                RecoverySource.ExistingStoreRecoverySource.INSTANCE,
                                unassignedInfo
                        ).initialize(replicaNode.getId(), null, ShardRouting.UNAVAILABLE_EXPECTED_SHARD_SIZE)
                                .moveToStarted();
                        // Add the replica shard routing to the index routing table
                        indexRoutingTableBuilder.addShard(replicaShardRouting);
                    }
                }

                IndexShardRoutingTable previousShardRoutingTable = previousIndexRoutingTable == null
                        ? null
                        : previousIndexRoutingTable.shard(shardNum);
                Optional<ShardRouting> previouslyStartedShard = previousShardRoutingTable == null
                        ? Optional.empty()
                        : previousShardRoutingTable.shards()
                        .stream()
                        .filter(sr -> localNode.getId().equals(sr.currentNodeId()))
                        .filter(ShardRouting::started)
                        .findAny();
                ShardRouting shardRouting;

                if (previouslyStartedShard.isPresent()) {
                    shardRouting = previouslyStartedShard.get();
                } else {
                    shardRouting = ShardRouting.newUnassigned(
                            shardId,
                            role == ShardRole.PRIMARY,
                            role == ShardRole.SEARCH_REPLICA,
                            recoverySource,
                            unassignedInfo
                    );
                    shardRouting = shardRouting.initialize(localNode.getId(), null, ShardRouting.UNAVAILABLE_EXPECTED_SHARD_SIZE);
                }

                indexRoutingTableBuilder.addShard(shardRouting);
                indexMetadataBuilder.putInSyncAllocationIds(shardNum, Set.of(shardRouting.allocationId().getId()));
                if (role == ShardRole.SEARCH_REPLICA) {
                    // For local search replicas, we have no reference to the primary shard, so we must claim that
                    // the index is search-only. Otherwise, an assertion in the RoutingNodes constructor will fail.
                    Settings.Builder settingsBuilder = Settings.builder().put(indexMetadata.getSettings());
                    settingsBuilder.put(IndexMetadata.INDEX_BLOCKS_SEARCH_ONLY_SETTING.getKey(), true);
                    indexMetadataBuilder.settings(settingsBuilder.build());
                }
            }
            routingTableBuilder.add(indexRoutingTableBuilder);
            metadataBuilder.put(indexMetadataBuilder);
        }
        clusterStateBuilder.routingTable(routingTableBuilder.build());
        clusterStateBuilder.metadata(metadataBuilder);

        return clusterStateBuilder.build();
    }

}
