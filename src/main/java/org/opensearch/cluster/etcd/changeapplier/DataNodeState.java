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
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

public class DataNodeState extends NodeState {
    private static final Logger logger = LogManager.getLogger(DataNodeState.class);

    private final Map<String, IndexMetadata> indices;
    private final Map<String, Set<DataNodeShard>> assignedShards;

    public DataNodeState(DiscoveryNode localNode, Map<String, IndexMetadata> indices, Map<String, Set<DataNodeShard>> assignedShards) {
        super(localNode);
        // The index metadata and shard assignment should be identical
        assert indices.keySet().equals(assignedShards.keySet());
        this.indices = indices;
        this.assignedShards = assignedShards;
    }

    /**
     * Determines the appropriate recovery source for a shard.
     * This prevents data loss on node restarts by using existing data when available.
     *
     * @param dataNodeShard the DataNodeShard containing all necessary information
     * @return the recovery source to use for this shard
     */
    private RecoverySource determineRecoverySource(DataNodeShard dataNodeShard) {
        String indexName = dataNodeShard.getIndexName();
        int shardNum = dataNodeShard.getShardNum();
        ShardRole role = dataNodeShard.getShardRole();

        logger.debug("Determining recovery source for shard {}[{}] with role {}", indexName, shardNum, role);

        // Search replicas should always fetch fresh data from remote store
        if (role == ShardRole.SEARCH_REPLICA) {
            logger.info("Shard {}[{}] with role {} is search-replica, using EmptyStoreRecoverySource", indexName, shardNum, role);
            return RecoverySource.EmptyStoreRecoverySource.INSTANCE;
        }

        // Regular replica shards will recover from primary
        if (role == ShardRole.REPLICA) {
            logger.info("Shard {}[{}] with role {} is replica, using PeerRecoverySource", indexName, shardNum, role);
            return RecoverySource.PeerRecoverySource.INSTANCE;
        }

        // For PRIMARY shards: If previously allocated then existing, else empty
        String previousAllocationId = dataNodeShard.getAllocationId();
        if (previousAllocationId != null) {
            logger.info(
                "Primary shard {}[{}] was previously allocated (ID: {}), using ExistingStoreRecoverySource",
                indexName,
                shardNum,
                previousAllocationId
            );
            return RecoverySource.ExistingStoreRecoverySource.INSTANCE;
        } else {
            logger.info("Primary shard {}[{}] was not previously allocated, using EmptyStoreRecoverySource", indexName, shardNum);
            return RecoverySource.EmptyStoreRecoverySource.INSTANCE;
        }
    }

    @Override
    public ClusterState buildClusterState(ClusterState previousState) {
        ClusterState.Builder clusterStateBuilder = ClusterState.builder(ClusterState.EMPTY_STATE);
        clusterStateBuilder.version(previousState.version() + 1);
        DiscoveryNodes.Builder nodesBuilder = DiscoveryNodes.builder().localNodeId(localNode.getId()).add(localNode);

        clusterStateBuilder.nodes(nodesBuilder);
        RoutingTable.Builder routingTableBuilder = RoutingTable.builder();
        Metadata.Builder metadataBuilder = Metadata.builder();
        for (Map.Entry<String, IndexMetadata> entry : indices.entrySet()) {
            IndexMetadata indexMetadata = entry.getValue();
            Index index = indexMetadata.getIndex();
            IndexMetadata oldIndexMetadata = previousState.metadata().index(index);

            IndexRoutingTable previousIndexRoutingTable = previousState.routingTable().index(index);
            IndexRoutingTable.Builder indexRoutingTableBuilder = IndexRoutingTable.builder(index);
            IndexMetadata.Builder indexMetadataBuilder = IndexMetadata.builder(indexMetadata);
            if (oldIndexMetadata != null) {
                indexMetadataBuilder.version(oldIndexMetadata.getVersion())
                    .settingsVersion(oldIndexMetadata.getSettingsVersion())
                    .mappingVersion(oldIndexMetadata.getMappingVersion());
            }
            for (DataNodeShard dataNodeShard : assignedShards.get(index.getName())) {
                int shardNum = dataNodeShard.getShardNum();
                ShardRole role = dataNodeShard.getShardRole();
                ShardId shardId = new ShardId(indexMetadata.getIndex(), shardNum);

                IndexShardRoutingTable.Builder newShardRoutingTable = new IndexShardRoutingTable.Builder(shardId);
                IndexShardRoutingTable previousShardRoutingTable = previousIndexRoutingTable == null
                    ? new IndexShardRoutingTable.Builder(shardId).build()
                    : previousIndexRoutingTable.shard(shardNum);

                UnassignedInfo unassignedInfo = new UnassignedInfo(UnassignedInfo.Reason.INDEX_CREATED, "created");

                // Handle replica shards with primary allocation
                if (role == ShardRole.REPLICA && dataNodeShard.getPrimaryAllocation().isPresent()) {
                    DataNodeShard.ShardAllocation primaryAllocation = dataNodeShard.getPrimaryAllocation().get();
                    RemoteNode primaryNode = primaryAllocation.node();

                    if (previousShardRoutingTable.primaryShard() != null
                        && previousShardRoutingTable.primaryShard().currentNodeId().equals(primaryNode.nodeId())) {
                        newShardRoutingTable.addShard(previousShardRoutingTable.primaryShard());
                    } else {
                        ShardRouting primaryShardRouting = ShardRouting.newUnassigned(
                            shardId,
                            true,
                            false,
                            RecoverySource.ExistingStoreRecoverySource.INSTANCE,
                            unassignedInfo
                        )
                            .initialize(
                                primaryNode.nodeId(),
                                primaryAllocation.allocationId(),
                                ShardRouting.UNAVAILABLE_EXPECTED_SHARD_SIZE
                            )
                            .moveToStarted();
                        // Add the primary shard routing to the index routing table
                        newShardRoutingTable.addShard(primaryShardRouting);
                    }
                    nodesBuilder.add(primaryNode.toDiscoveryNode());
                }

                // Determine recovery source for this shard
                String previousAllocationId = dataNodeShard.getAllocationId();

                Set<String> inSyncAllocationIds = new HashSet<>();
                if (role == ShardRole.PRIMARY && dataNodeShard.getReplicaAssignments().isEmpty() == false) {
                    // If this is a primary, we need to add the replica shards
                    Map<String, DataNodeShard.ShardAllocation> replicaNodesMap = new HashMap<>(
                        dataNodeShard.getReplicaAssignments()
                            .stream()
                            .collect(Collectors.toMap(k -> k.node().nodeId(), Function.identity()))
                    );
                    for (DataNodeShard.ShardAllocation shardAllocation : replicaNodesMap.values()) {
                        RemoteNode replicaNode = shardAllocation.node();
                        ShardRouting replicaShardRouting = ShardRouting.newUnassigned(
                            shardId,
                            false,
                            false,
                            RecoverySource.PeerRecoverySource.INSTANCE,
                            unassignedInfo
                        ).initialize(replicaNode.nodeId(), shardAllocation.allocationId(), ShardRouting.UNAVAILABLE_EXPECTED_SHARD_SIZE);
                        if (shardAllocation.shardState() == DataNodeShard.ShardState.STARTED) {
                            inSyncAllocationIds.add(shardAllocation.allocationId());
                            replicaShardRouting = replicaShardRouting.moveToStarted();
                        }
                        // Add the replica shard routing to the index routing table
                        newShardRoutingTable.addShard(replicaShardRouting);
                        nodesBuilder.add(replicaNode.toDiscoveryNode());
                    }
                }

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
                    logger.debug(
                        "Reusing existing ShardRouting for shard {}[{}] with allocation ID {}",
                        indexMetadata.getIndex().getName(),
                        shardNum,
                        shardRouting.allocationId().getId()
                    );
                } else {
                    // No previous shard in cluster state - use ETCD-based allocation ID preservation
                    RecoverySource recoverySource = determineRecoverySource(dataNodeShard);
                    shardRouting = ShardRouting.newUnassigned(
                        shardId,
                        role == ShardRole.PRIMARY,
                        role == ShardRole.SEARCH_REPLICA,
                        recoverySource,
                        unassignedInfo
                    );

                    shardRouting = shardRouting.initialize(
                        localNode.getId(),
                        previousAllocationId,
                        ShardRouting.UNAVAILABLE_EXPECTED_SHARD_SIZE
                    );

                    // ALLOCATION ID TRACKING: Log allocation ID changes for monitoring purposes
                    String currentAllocationId = shardRouting.allocationId().getId();

                    if (previousAllocationId != null) {
                        if (previousAllocationId.equals(currentAllocationId)) {
                            logger.info(
                                "✅ ALLOCATION ID PRESERVED: shard {}[{}] kept allocation ID {}",
                                indexMetadata.getIndex().getName(),
                                shardNum,
                                previousAllocationId
                            );
                        } else {
                            logger.info(
                                "🔄 ALLOCATION ID CHANGED: shard {}[{}] from {} to {}",
                                indexMetadata.getIndex().getName(),
                                shardNum,
                                previousAllocationId,
                                currentAllocationId
                            );
                        }
                    } else {
                        logger.debug(
                            "No previous allocation ID found for shard {}[{}], using new ID: {}",
                            indexMetadata.getIndex().getName(),
                            shardNum,
                            currentAllocationId
                        );
                    }
                }
                newShardRoutingTable.addShard(shardRouting);

                indexRoutingTableBuilder.addIndexShard(newShardRoutingTable.build());
                inSyncAllocationIds.add(shardRouting.allocationId().getId());
                indexMetadataBuilder.putInSyncAllocationIds(shardNum, inSyncAllocationIds);
                if (role == ShardRole.SEARCH_REPLICA) {
                    // For local search replicas, we have no reference to the primary shard, so we must claim that
                    // the index is search-only. Otherwise, an assertion in the RoutingNodes constructor will fail.
                    Settings.Builder settingsBuilder = Settings.builder().put(indexMetadata.getSettings());
                    settingsBuilder.put(IndexMetadata.INDEX_BLOCKS_SEARCH_ONLY_SETTING.getKey(), true);
                    indexMetadataBuilder.settings(settingsBuilder.build());
                }
            }
            IndexMetadata newIndexMetadata = indexMetadataBuilder.build();
            if (oldIndexMetadata != null && oldIndexMetadata.equals(newIndexMetadata) == false) {
                logger.info("Index metadata for index {} changed", index.getName());
                indexMetadataBuilder.version(oldIndexMetadata.getVersion() + 1);
                if (oldIndexMetadata.getSettings().equals(newIndexMetadata.getSettings()) == false) {
                    indexMetadataBuilder.settingsVersion(oldIndexMetadata.getSettingsVersion() + 1);
                }
                if (Objects.equals(oldIndexMetadata.mapping(), newIndexMetadata.mapping()) == false) {
                    indexMetadataBuilder.mappingVersion(oldIndexMetadata.getMappingVersion() + 1);
                }
                newIndexMetadata = indexMetadataBuilder.build();
            }

            routingTableBuilder.add(indexRoutingTableBuilder);
            metadataBuilder.put(newIndexMetadata, false);
        }
        clusterStateBuilder.routingTable(routingTableBuilder.build());
        clusterStateBuilder.metadata(metadataBuilder);

        return clusterStateBuilder.build();
    }

}
