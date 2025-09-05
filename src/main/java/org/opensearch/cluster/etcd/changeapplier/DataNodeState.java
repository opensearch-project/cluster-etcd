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
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

public class DataNodeState extends NodeState {
    private static final Logger logger = LogManager.getLogger(DataNodeState.class);

    private final Map<String, IndexMetadata> indices;
    private final Map<String, Set<DataNodeShard>> assignedShards;

    public DataNodeState(
        DiscoveryNode localNode,
        Map<String, IndexMetadata> indices,
        Map<String, Set<DataNodeShard>> assignedShards,
        boolean converged
    ) {
        super(localNode, converged);
        // The index metadata and shard assignment should be identical
        assert indices.keySet().equals(assignedShards.keySet());
        this.indices = indices;
        this.assignedShards = assignedShards;
    }

    /**
     * Determines the appropriate recovery source for a shard.
     * This prevents data loss on node restarts by using existing data when available.
     *
     * @param shardNum the shard number
     * @param indexMetadata metadata for the index containing this shard
     * @param role the shard role (PRIMARY, REPLICA, SEARCH_REPLICA)
     * @return the recovery source to use for this shard
     */
    private RecoverySource determineRecoverySource(int shardNum, IndexMetadata indexMetadata, ShardRole role) {
        String indexName = indexMetadata.getIndex().getName();
        logger.debug("Determining recovery source for shard {}[{}] with role {}", indexName, shardNum, role);

        // Replica shards will recover from primary
        if (role != ShardRole.PRIMARY) {
            logger.info("Shard {}[{}] with role {} is replica/search-replica, using PeerRecoverySource", indexName, shardNum, role);
            return RecoverySource.PeerRecoverySource.INSTANCE;
        }

        // For PRIMARY shards: Check if there's a previous allocation ID from ETCD
        String previousAllocationId = getPreviousAllocationId(indexName, shardNum);
        if (previousAllocationId != null) {
            logger.info(
                "Found previous allocation ID {} for shard {}[{}] from ETCD, using ExistingStoreRecoverySource",
                previousAllocationId,
                indexName,
                shardNum
            );
            return RecoverySource.ExistingStoreRecoverySource.INSTANCE;
        } else {
            logger.info("No previous allocation ID found for shard {}[{}] in ETCD, using EmptyStoreRecoverySource", indexName, shardNum);
            return RecoverySource.EmptyStoreRecoverySource.INSTANCE;
        }
    }

    /**
     * Gets the allocation ID for a specific shard from the assigned shards.
     * Returns null if no previous allocation ID is found.
     */
    private String getPreviousAllocationId(String indexName, int shardNum) {
        try {
            Set<DataNodeShard> shards = assignedShards.get(indexName);
            if (shards == null) {
                logger.debug("No shards found for index: {}", indexName);
                return null;
            }

            // Find the shard that matches the index and shard number
            DataNodeShard matchingShard = null;
            for (DataNodeShard shard : shards) {
                if (shard.getIndexName().equals(indexName) && shard.getShardNum() == shardNum) {
                    matchingShard = shard;
                    break;
                }
            }

            if (matchingShard == null) {
                logger.debug("No shard found for {}[{}]", indexName, shardNum);
                return null;
            }

            String allocationId = matchingShard.getAllocationId();
            logger.debug("Found previous allocation ID for shard {}[{}]: {}", indexName, shardNum, allocationId);
            return allocationId;

        } catch (Exception e) {
            logger.warn("Error extracting previous allocation ID for {}[{}]", indexName, shardNum, e);
            return null;
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

            IndexRoutingTable previousIndexRoutingTable = previousState.routingTable().index(index);
            IndexRoutingTable.Builder indexRoutingTableBuilder = IndexRoutingTable.builder(index);
            IndexMetadata.Builder indexMetadataBuilder = IndexMetadata.builder(indexMetadata);
            for (DataNodeShard dataNodeShard : assignedShards.get(index.getName())) {
                int shardNum = dataNodeShard.getShardNum();
                ShardRole role = dataNodeShard.getShardRole();
                ShardId shardId = new ShardId(indexMetadata.getIndex(), shardNum);

                IndexShardRoutingTable.Builder newShardRoutingTable = new IndexShardRoutingTable.Builder(shardId);
                IndexShardRoutingTable previousShardRoutingTable = previousIndexRoutingTable == null
                    ? new IndexShardRoutingTable.Builder(shardId).build()
                    : previousIndexRoutingTable.shard(shardNum);

                UnassignedInfo unassignedInfo = new UnassignedInfo(UnassignedInfo.Reason.INDEX_CREATED, "created");

                final RecoverySource recoverySource;
                if (role == ShardRole.REPLICA && dataNodeShard.getPrimaryAllocation().isPresent()) {
                    DataNodeShard.ShardAllocation primaryAllocation = dataNodeShard.getPrimaryAllocation().get();
                    RemoteNode primaryNode = primaryAllocation.node();
                    recoverySource = RecoverySource.PeerRecoverySource.INSTANCE;
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
                } else {
                    recoverySource = RecoverySource.EmptyStoreRecoverySource.INSTANCE; // For primary and search replica
                }

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
                    RecoverySource etcdRecoverySource = determineRecoverySource(shardNum, indexMetadata, role);
                    String previousAllocationId = getPreviousAllocationId(indexMetadata.getIndex().getName(), shardNum);

                    shardRouting = ShardRouting.newUnassigned(
                        shardId,
                        role == ShardRole.PRIMARY,
                        role == ShardRole.SEARCH_REPLICA,
                        etcdRecoverySource, // Use recovery source based on ETCD data
                        unassignedInfo
                    );

                    shardRouting = shardRouting.initialize(
                        localNode.getId(),
                        previousAllocationId,
                        ShardRouting.UNAVAILABLE_EXPECTED_SHARD_SIZE
                    );

                    // ALLOCATION ID TRACKING: Log allocation ID changes for monitoring purposes
                    String currentAllocationId = shardRouting.allocationId() != null ? shardRouting.allocationId().getId() : "null";

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
            routingTableBuilder.add(indexRoutingTableBuilder);
            metadataBuilder.put(indexMetadataBuilder);
        }
        clusterStateBuilder.routingTable(routingTableBuilder.build());
        clusterStateBuilder.metadata(metadataBuilder);

        return clusterStateBuilder.build();
    }

}
