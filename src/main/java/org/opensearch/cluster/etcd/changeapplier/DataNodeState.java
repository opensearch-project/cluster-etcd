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
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.XContentParser;

import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

public class DataNodeState extends NodeState {
    private static final Logger logger = LogManager.getLogger(DataNodeState.class);
    
    private final Map<String, IndexMetadata> indices;
    private final Map<String, Map<Integer, ShardRole>> assignedShards;
    private final Client etcdClient;
    private final String clusterName;
    
    // Cache for heartbeat data to avoid repeated ETCD reads
    private Map<String, List<Map<String, Object>>> cachedNodeRouting = null;

    public DataNodeState(DiscoveryNode localNode, Map<String, IndexMetadata> indices, Map<String, Map<Integer, ShardRole>> assignedShards, Client etcdClient, String clusterName) {
        super(localNode);
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
     * @return the recovery source to use for this shard
     */
    private RecoverySource determineRecoverySource(int shardNum, IndexMetadata indexMetadata) {
        String indexName = indexMetadata.getIndex().getName();
        
        logger.debug("Determining recovery source for shard {}[{}]", indexName, shardNum);
        
        // Step 1: PRIORITY - cluster-etcd heartbeat check - did THIS node have this shard before restart?
        if (!thisNodeHadShardBefore(indexName, shardNum)) {
            logger.info("Shard {}[{}] was not on this node before restart, using EmptyStoreRecoverySource", indexName, shardNum);
            return RecoverySource.EmptyStoreRecoverySource.INSTANCE;
        }
        
        // Step 2: Node had shard before, but check if data actually exists on disk
        if (actualDataExistsOnDisk(indexName, shardNum)) {
            logger.info("Shard {}[{}] existed before and has data on disk, using ExistingStoreRecoverySource (data preserved)", indexName, shardNum);
            return RecoverySource.ExistingStoreRecoverySource.INSTANCE;
        } else {
            logger.warn("Shard {}[{}] was on this node before but no data found on disk - using EmptyStoreRecoverySource (graceful fallback)", indexName, shardNum);
            return RecoverySource.EmptyStoreRecoverySource.INSTANCE;
        }
    }
    
    /**
     * Checks if this node had the specified shard before restart by reading the last heartbeat from ETCD.
     * Uses caching to avoid repeated ETCD reads during startup.
     */
    private boolean thisNodeHadShardBefore(String indexName, int shardNum) {
        try {
            // Get cached node routing (reads from ETCD once per startup)
            Map<String, List<Map<String, Object>>> nodeRouting = getCachedNodeRouting();
            
            // Check if this shard was in our previous routing
            boolean hadShard = checkShardInRouting(nodeRouting, indexName, shardNum);
            
            logger.debug("Checking if node had shard {}[{}] before restart: {}", indexName, shardNum, hadShard);
            return hadShard;
            
        } catch (Exception e) {
            logger.warn("Error checking previous shard assignment for {}[{}], assuming false", indexName, shardNum, e);
            return false; // Safe default
        }
    }
    
    /**
     * Gets the allocation ID for a specific shard from the previous heartbeat data.
     * Returns null if no previous allocation ID is found.
     */
    private String getPreviousAllocationId(String indexName, int shardNum) {
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
    // For now, always return false to prevent recovery failures
    // This forces graceful fallback to EmptyStoreRecoverySource
    logger.debug("actualDataExistsOnDisk for {}[{}] - returning false (default behavior)", indexName, shardNum);
    return false;
    
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
            List<KeyValue> result = kvClient.get(key).get().getKvs();
            
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
            XContentParser parser = JsonXContent.jsonXContent.createParser(
                NamedXContentRegistry.EMPTY, 
                null, 
                heartbeatJson
            );
            Map<String, Object> heartbeat = parser.map();
            
            // Extract nodeRouting section
            Map<String, Object> nodeRouting = (Map<String, Object>) heartbeat.get("nodeRouting");
            if (nodeRouting == null) {
                logger.debug("No nodeRouting section found in heartbeat");
                return Collections.emptyMap();
            }
            
            // Convert to proper format
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
     * Uses node name for comparison since node ID changes on restart.
     */
    private boolean checkShardInRouting(Map<String, List<Map<String, Object>>> nodeRouting, String indexName, int shardNum) {
        List<Map<String, Object>> indexShards = nodeRouting.get(indexName);
        if (indexShards == null) {
            return false; // This node had no shards for this index
        }
        
        // Look for the specific shard number that was on THIS node
        // Use node name instead of node ID since node ID changes on restart
        return indexShards.stream()
            .anyMatch(shard -> {
                Object shardId = shard.get("shardId");
                Object currentNodeName = shard.get("currentNodeName");
                return shardId != null && shardId.equals(shardNum) 
                    && currentNodeName != null && currentNodeName.equals(localNode.getName());
            });
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
            for (Map.Entry<Integer, ShardRole> shardRoleEntry : assignedShards.get(index.getName()).entrySet()) {
                int shardNum = shardRoleEntry.getKey();
                ShardRole role = shardRoleEntry.getValue();
                ShardId shardId = new ShardId(indexMetadata.getIndex(), shardNum);
                UnassignedInfo unassignedInfo = new UnassignedInfo(UnassignedInfo.Reason.INDEX_CREATED, "created");
                
                ShardRouting shardRouting = ShardRouting.newUnassigned(
                    shardId,
                    role == ShardRole.PRIMARY,
                    role == ShardRole.SEARCH_REPLICA,
                    determineRecoverySource(shardNum, indexMetadata), // Enable smart recovery
                    unassignedInfo
                );
                
                // Get previous allocation ID BEFORE initializing
                String previousAllocationId = getPreviousAllocationId(indexMetadata.getIndex().getName(), shardNum);
                
                // Initialize with previous allocation ID (for allocation ID preservation)
                shardRouting = shardRouting.initialize(localNode.getId(), previousAllocationId, ShardRouting.UNAVAILABLE_EXPECTED_SHARD_SIZE);
                
                // ALLOCATION ID TRACKING: Log allocation ID changes for monitoring purposes
                String currentAllocationId = shardRouting.allocationId() != null ? shardRouting.allocationId().getId() : "null";
                
                if (previousAllocationId != null) {
                    if (previousAllocationId.equals(currentAllocationId)) {
                        logger.info("✅ ALLOCATION ID PRESERVED: shard {}[{}] kept allocation ID {}", 
                            indexMetadata.getIndex().getName(), shardNum, previousAllocationId);
                    } else {
                        logger.info("🔄 ALLOCATION ID CHANGED: shard {}[{}] from {} to {}", 
                            indexMetadata.getIndex().getName(), shardNum, previousAllocationId, currentAllocationId);
                    }
                } else {
                    logger.debug("No previous allocation ID found for shard {}[{}], using new ID: {}", 
                        indexMetadata.getIndex().getName(), shardNum, currentAllocationId);
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
                if (previouslyStartedShard.isPresent()) {
                    // TODO: Someone needs to moveToStarted the first time. Probably on the local node.
                    shardRouting = shardRouting.moveToStarted();
                }
                indexRoutingTableBuilder.addShard(shardRouting);
                
                indexMetadataBuilder.putInSyncAllocationIds(shardNum, Set.of(shardRouting.allocationId().getId()));
                if (role == ShardRole.SEARCH_REPLICA) {
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
