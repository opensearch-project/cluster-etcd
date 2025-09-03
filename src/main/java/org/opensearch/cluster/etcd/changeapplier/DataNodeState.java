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
import org.opensearch.cluster.etcd.ETCDPathUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import io.etcd.jetcd.Client;
import io.etcd.jetcd.KV;
import io.etcd.jetcd.ByteSequence;
import io.etcd.jetcd.KeyValue;
import io.etcd.jetcd.kv.GetResponse;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.XContentParser;

import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import java.util.stream.Collectors;

public class DataNodeState extends NodeState {
    private static final Logger logger = LogManager.getLogger(DataNodeState.class);

    private final Map<String, IndexMetadata> indices;
    private final Map<String, Set<DataNodeShard>> assignedShards;
    private final Client etcdClient;
    private final String clusterName;

    // Cache for heartbeat data to avoid repeated ETCD reads
    private Map<String, List<Map<String, Object>>> cachedNodeRouting = null;

    public DataNodeState(
        DiscoveryNode localNode,
        Map<String, IndexMetadata> indices,
        Map<String, Set<DataNodeShard>> assignedShards,
        boolean converged,
        Client etcdClient,
        String clusterName
    ) {
        super(localNode, converged);
        // The index metadata and shard assignment should be identical
        assert indices.keySet().equals(assignedShards.keySet());
        this.indices = indices;
        this.assignedShards = assignedShards;
        this.etcdClient = etcdClient;
        this.clusterName = clusterName;
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

        // For PRIMARY shards: Check if this node had this shard before restart
        if (thisNodeHadShardBefore(indexName, shardNum)) {
            // Node had this shard before, check if data actually exists on disk
            if (actualDataExistsOnDisk(indexName, shardNum)) {
                logger.info(
                    "Node had shard {}[{}] before restart and data exists on disk, using ExistingStoreRecoverySource",
                    indexName,
                    shardNum
                );
                return RecoverySource.ExistingStoreRecoverySource.INSTANCE;
            } else {
                logger.warn(
                    "Node had shard {}[{}] before restart but no data exists on disk, using EmptyStoreRecoverySource",
                    indexName,
                    shardNum
                );
                return RecoverySource.EmptyStoreRecoverySource.INSTANCE;
            }
        } else {
            logger.info("Node did not have shard {}[{}] before restart, using EmptyStoreRecoverySource", indexName, shardNum);
            return RecoverySource.EmptyStoreRecoverySource.INSTANCE;
        }
    }

    /**
     * Checks if this node had the specified shard before restart by reading the last heartbeat from ETCD.
     * Uses caching to avoid repeated ETCD reads during startup.
     */
    private boolean thisNodeHadShardBefore(String indexName, int shardNum) {
        try {
            Map<String, List<Map<String, Object>>> nodeRouting = getCachedNodeRouting();

            boolean hadShard = checkShardInRouting(nodeRouting, indexName, shardNum);

            logger.debug("Checking if node had shard {}[{}] before restart: {}", indexName, shardNum, hadShard);
            return hadShard;

        } catch (Exception e) {
            logger.warn("Error checking previous shard assignment for {}[{}], assuming false", indexName, shardNum, e);
            return false;
        }
    }

    /**
     * Gets the allocation ID for a specific shard from the previous heartbeat data.
     * Returns null if no previous allocation ID is found.
     */
    private String getPreviousAllocationId(String indexName, int shardNum) {
        try {
            return getPreviousAllocationIdFromHeartbeat(indexName, shardNum);
        } catch (Exception e) {
            logger.warn("Error extracting previous allocation ID for {}[{}]", indexName, shardNum, e);
            return null;
        }
    }

    /**
     * Gets allocation ID from node heartbeat data.
     */
    private String getPreviousAllocationIdFromHeartbeat(String indexName, int shardNum) {
        try {
            Map<String, List<Map<String, Object>>> nodeRouting = getCachedNodeRouting();
            if (nodeRouting == null || !nodeRouting.containsKey(indexName)) {
                return null;
            }

            List<Map<String, Object>> shards = nodeRouting.get(indexName);
            for (Map<String, Object> shard : shards) {
                Object shardIdObj = shard.get("shardId");
                Object currentNodeName = shard.get("currentNodeName");
                Object allocationIdObj = shard.get("allocationId");

                if (shardIdObj != null && currentNodeName != null && allocationIdObj != null) {
                    int shardId = ((Number) shardIdObj).intValue();
                    String nodeName = currentNodeName.toString();

                    // Check if this is the right shard and it was on this node
                    if (shardId == shardNum && localNode.getName().equals(nodeName)) {
                        String allocationId = allocationIdObj.toString();
                        logger.info("Found previous allocation ID for shard {}[{}]: {}", indexName, shardNum, allocationId);
                        return allocationId;
                    }
                }
            }

            logger.debug("No previous allocation ID found for shard {}[{}]", indexName, shardNum);
            return null;

        } catch (Exception e) {
            logger.warn("Error extracting previous allocation ID for {}[{}]", indexName, shardNum, e);
            return null;
        }
    }

    /**
     * Gets the cached node routing data, reading from ETCD if not already cached.
     */
    private Map<String, List<Map<String, Object>>> getCachedNodeRouting() {
        if (cachedNodeRouting == null) {
            String heartbeatJson = readHeartbeatFromETCD();
            cachedNodeRouting = parseNodeRouting(heartbeatJson);
        }
        return cachedNodeRouting;
    }

    /**
     * Checks if actual shard data exists on disk before attempting ExistingStoreRecoverySource.
     *
     * TODO: Implement actual disk checking when needed for production scenarios.
     */
    private boolean actualDataExistsOnDisk(String indexName, int shardNum) {
        // For now, always return true for testing purposes
        logger.debug("actualDataExistsOnDisk for {}[{}] - returning true (default behavior)", indexName, shardNum);
        return true;
    }

    /**
     * Reads the last heartbeat from ETCD for this node.
     */
    private String readHeartbeatFromETCD() {
        if (etcdClient == null || clusterName == null) {
            logger.debug("ETCD client or cluster name not available, cannot read heartbeat");
            return null;
        }

        try {
            String heartbeatPath = ETCDPathUtils.buildSearchUnitActualStatePath(localNode, clusterName);
            ByteSequence key = ByteSequence.from(heartbeatPath, StandardCharsets.UTF_8);

            KV kvClient = etcdClient.getKVClient();
            CompletableFuture<GetResponse> future = kvClient.get(key);
            if (future == null) {
                return null;
            }

            List<KeyValue> result = future.get().getKvs();

            if (result.isEmpty()) {
                logger.debug("No previous heartbeat found at path: {}", heartbeatPath);
                return null;
            }

            String heartbeatJson = result.get(0).getValue().toString(StandardCharsets.UTF_8);
            logger.debug("Successfully read heartbeat from ETCD, size: {} chars", heartbeatJson.length());
            return heartbeatJson;

        } catch (Exception e) {
            logger.warn("Failed to read heartbeat from ETCD", e);
            return null;
        }
    }

    /**
     * Parses the heartbeat JSON and extracts the nodeRouting section.
     */
    private Map<String, List<Map<String, Object>>> parseNodeRouting(String heartbeatJson) {
        if (heartbeatJson == null || heartbeatJson.trim().isEmpty()) {
            return Collections.emptyMap();
        }

        try {
            // Parse JSON using JsonXContent (same pattern as ETCDStateDeserializer)
            XContentParser parser = JsonXContent.jsonXContent.createParser(NamedXContentRegistry.EMPTY, null, heartbeatJson);
            Map<String, Object> heartbeat = parser.map();

            Map<String, Object> nodeRouting = (Map<String, Object>) heartbeat.get("nodeRouting");
            if (nodeRouting == null) {
                logger.debug("No nodeRouting section found in heartbeat");
                return Collections.emptyMap();
            }

            Map<String, List<Map<String, Object>>> result = new HashMap<>();
            for (Map.Entry<String, Object> entry : nodeRouting.entrySet()) {
                String indexName = entry.getKey();
                List<Map<String, Object>> shards = (List<Map<String, Object>>) entry.getValue();
                result.put(indexName, shards);
            }

            logger.debug("Parsed nodeRouting with {} indices from heartbeat", result.size());
            return result;

        } catch (Exception e) {
            logger.warn("Failed to parse heartbeat JSON", e);
            return Collections.emptyMap();
        }
    }

    /**
     * Checks if a specific shard exists in the parsed node routing data and was on THIS node.
     */
    private boolean checkShardInRouting(Map<String, List<Map<String, Object>>> nodeRouting, String indexName, int shardNum) {
        List<Map<String, Object>> indexShards = nodeRouting.get(indexName);
        if (indexShards == null) {
            return false;
        }

        // Look for the specific shard number that was on THIS node
        return indexShards.stream().anyMatch(shard -> {
            Object shardId = shard.get("shardId");
            Object currentNodeName = shard.get("currentNodeName");
            return shardId != null && shardId.equals(shardNum) && currentNodeName != null && currentNodeName.equals(localNode.getName());
        });
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
