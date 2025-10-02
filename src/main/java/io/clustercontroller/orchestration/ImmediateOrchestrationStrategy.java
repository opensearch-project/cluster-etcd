package io.clustercontroller.orchestration;

import io.clustercontroller.enums.NodeRole;
import io.clustercontroller.models.Index;
import io.clustercontroller.models.ShardAllocation;
import io.clustercontroller.models.SearchUnitGoalState;
import io.clustercontroller.store.MetadataStore;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 * Simple strategy that immediately updates all goal states
 */
@Component
@Slf4j
public class ImmediateOrchestrationStrategy implements GoalStateOrchestrationStrategy {
    
    private final MetadataStore metadataStore;
    
    public ImmediateOrchestrationStrategy(MetadataStore metadataStore) {
        this.metadataStore = metadataStore;
    }
    
    @Override
    public void orchestrate(String clusterId) {
        log.info("Starting immediate orchestration for cluster: {}", clusterId);
        
        try {
            // Get all index configs to iterate over indexes and shards
            List<Index> indexConfigs = metadataStore.getAllIndexConfigs(clusterId);
            
            // Outer loop: Iterate over indexes
            for (Index indexConfig : indexConfigs) {
                String indexName = indexConfig.getIndexName();
                int numberOfShards = indexConfig.getNumberOfShards();
                
                log.debug("Processing index: {} with {} shards", indexName, numberOfShards);
                
                // Inner loop: Iterate over shards
                for (int shardIndex = 0; shardIndex < numberOfShards; shardIndex++) {
                    String shardId = String.valueOf(shardIndex);
                    
                    try {
                        log.debug("Processing shard: {}/{}", indexName, shardId);
                        
                        // Get planned allocation for this specific index-shard
                        ShardAllocation planned = metadataStore.getPlannedAllocation(clusterId, indexName, shardId);
                        
                        if (planned == null) {
                            log.debug("No planned allocation found for shard {}/{}", indexName, shardId);
                            continue;
                        }
                        
                        // Process all nodes immediately
                        orchestrateIndexShard(indexName, shardId, planned, clusterId);
                        
                    } catch (Exception e) {
                        log.error("Failed to orchestrate shard {}/{}: {}", indexName, shardId, e.getMessage(), e);
                        // TODO: Add alert/notification system for orchestration failures
                    }
                }
            }
            
            log.info("Completed immediate orchestration for cluster: {}", clusterId);
        } catch (Exception e) {
            log.error("Failed to orchestrate goal states for cluster {}: {}", clusterId, e.getMessage(), e);
        }
    }
    
    private void orchestrateIndexShard(String indexName, String shardId, ShardAllocation planned, String clusterId) {
        // Process IngestSUs (Primary nodes) immediately
        List<String> ingestSUs = planned.getIngestSUs();
        if (!ingestSUs.isEmpty()) {
            log.info("Immediately updating {} PRIMARY nodes for shard {}/{}", ingestSUs.size(), indexName, shardId);
            updateNodeGroup(ingestSUs, indexName, shardId, planned, clusterId, NodeRole.PRIMARY);
        }
        
        // Process SearchSUs (Replica nodes) immediately
        List<String> searchSUs = planned.getSearchSUs();
        if (!searchSUs.isEmpty()) {
            log.info("Immediately updating {} REPLICA nodes for shard {}/{}", searchSUs.size(), indexName, shardId);
            updateNodeGroup(searchSUs, indexName, shardId, planned, clusterId, NodeRole.REPLICA);
        }
        
        if (ingestSUs.isEmpty() && searchSUs.isEmpty()) {
            log.debug("No nodes to orchestrate for shard {}/{}", indexName, shardId);
        }
    }
    
    private void updateNodeGroup(List<String> nodeGroup, String indexName, String shardId, ShardAllocation planned, String clusterId, NodeRole role) {
        for (String nodeId : nodeGroup) {
            try {
                updateNodeGoalState(nodeId, indexName, shardId, planned, clusterId);
                log.debug("Updated goal state for {} node {} with shard {}/{}", role, nodeId, indexName, shardId);
            } catch (Exception e) {
                log.error("Failed to update goal state for {} node {}: {}", role, nodeId, e.getMessage(), e);
            }
        }
    }
    
    private void updateNodeGoalState(String nodeId, String indexName, String shardId, ShardAllocation planned, String clusterId) throws Exception {
        try {
            // Get current goal state for the node
            SearchUnitGoalState currentGoalState = metadataStore.getSearchUnitGoalState(clusterId, nodeId);
            
            // If null, create new one
            if (currentGoalState == null) {
                currentGoalState = new SearchUnitGoalState();
            }
            
            // Determine role based on whether this node is in IngestSUs or SearchSUs
            String role = planned.getIngestSUs().contains(nodeId) ? NodeRole.PRIMARY.getValue() : NodeRole.REPLICA.getValue();
            
            // Update goal state with new shard allocation
            SearchUnitGoalState newGoalState = updateGoalState(currentGoalState, indexName, shardId, role);
            
            // Set new goal state in etcd
            metadataStore.setSearchUnitGoalState(clusterId, nodeId, newGoalState);
            
        } catch (Exception e) {
            log.error("Failed to update goal state for node {}: {}", nodeId, e.getMessage(), e);
            throw e;
        }
    }
    
    private SearchUnitGoalState updateGoalState(SearchUnitGoalState current, String indexName, String shardId, String role) {
        if (current == null) {
            current = new SearchUnitGoalState();
        }
        
        // Add new shard allocation using the localShards structure
        current.getLocalShards().computeIfAbsent(indexName, k -> new HashMap<>()).put(shardId, role);
        
        return current;
    }
}
