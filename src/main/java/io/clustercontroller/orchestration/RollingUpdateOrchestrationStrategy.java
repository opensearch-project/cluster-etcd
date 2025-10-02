package io.clustercontroller.orchestration;

import io.clustercontroller.enums.NodeRole;
import io.clustercontroller.models.Index;
import io.clustercontroller.models.IndexShardProgress;
import io.clustercontroller.models.ShardAllocation;
import io.clustercontroller.models.SearchUnitActualState;
import io.clustercontroller.models.SearchUnitGoalState;
import io.clustercontroller.store.MetadataStore;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Sophisticated strategy that implements rolling updates with configurable transit percentage
 * Maintains a maximum percentage of nodes in transit for any index-shard
 */
@Component
@Slf4j
public class RollingUpdateOrchestrationStrategy implements GoalStateOrchestrationStrategy {
    
    private final MetadataStore metadataStore;
    private final double maxTransitPercentage = 0.20; // TODO: Make configurable
    
    public RollingUpdateOrchestrationStrategy(MetadataStore metadataStore) {
        this.metadataStore = metadataStore;
    }
    
    @Override
    public void orchestrate(String clusterId) {
        log.info("Starting rolling update orchestration for cluster: {}", clusterId);
        
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
                        
                        // Process this index-shard with rolling update logic
                        orchestrateIndexShard(indexName, shardId, planned, clusterId);
                        
                    } catch (Exception e) {
                        log.error("Failed to orchestrate shard {}/{}: {}", indexName, shardId, e.getMessage(), e);
                        // TODO: Add alert/notification system for orchestration failures
                    }
                }
            }
            
            log.info("Completed rolling update orchestration for cluster: {}", clusterId);
        } catch (Exception e) {
            log.error("Failed to orchestrate goal states for cluster {}: {}", clusterId, e.getMessage(), e);
        }
    }
    
    private void orchestrateIndexShard(String indexName, String shardId, ShardAllocation planned, String clusterId) throws Exception {
        String indexShard = indexName + "/" + shardId;
        
        // Process IngestSUs (Primary nodes) separately
        List<String> ingestSUs = planned.getIngestSUs();
        if (!ingestSUs.isEmpty()) {
            log.debug("Processing {} IngestSUs (primary nodes) for shard {}/{}", ingestSUs.size(), indexName, shardId);
            orchestrateNodeGroup(indexName, shardId, ingestSUs, NodeRole.PRIMARY, planned, clusterId);
        }
        
        // Process SearchSUs (Replica nodes) separately
        List<String> searchSUs = planned.getSearchSUs();
        if (!searchSUs.isEmpty()) {
            log.debug("Processing {} SearchSUs (replica nodes) for shard {}/{}", searchSUs.size(), indexName, shardId);
            orchestrateNodeGroup(indexName, shardId, searchSUs, NodeRole.REPLICA, planned, clusterId);
        }
        
        if (ingestSUs.isEmpty() && searchSUs.isEmpty()) {
            log.debug("No nodes to orchestrate for shard {}/{}", indexName, shardId);
        }
    }
    
    private void orchestrateNodeGroup(String indexName, String shardId, List<String> nodeGroup, NodeRole role, ShardAllocation planned, String clusterId) throws Exception {
        String indexShard = indexName + "/" + shardId;
        
        // Check current progress for this node group
        IndexShardProgress progress = getIndexShardProgress(indexShard, nodeGroup, clusterId);
        
        // Calculate how many nodes can be updated (20% of this group)
        int availableSlots = progress.getAvailableSlots(maxTransitPercentage);
        
        if (availableSlots > 0) {
            // Get nodes that haven't been updated yet
            List<String> nodesToUpdate = getNodesToUpdate(nodeGroup, progress);
            
            // Update next batch
            int batchSize = Math.min(availableSlots, nodesToUpdate.size());
            List<String> nextBatch = nodesToUpdate.stream()
                    .limit(batchSize)
                    .collect(java.util.stream.Collectors.toList());
            
            log.info("Starting batch update for {} {} nodes for shard {}/{}: {} nodes ({}% in transit)", 
                    role, nodeGroup.size(), indexName, shardId, nextBatch.size(), 
                    (progress.getTransitNodesCount() + nextBatch.size()) * 100 / nodeGroup.size());
            
            for (String nodeId : nextBatch) {
                try {
                    updateNodeGoalState(nodeId, indexName, shardId, planned, clusterId);
                    log.debug("Started update for {} node {} with shard {}/{}", role, nodeId, indexName, shardId);
                } catch (Exception e) {
                    log.error("Failed to start update for {} node {}: {}", role, nodeId, e.getMessage(), e);
                }
            }
        } else {
            log.info("Index-shard {} {} group has {}% nodes in transit, waiting for convergence", 
                    indexShard, role, progress.getTransitPercentage() * 100);
        }
    }
    
    private IndexShardProgress getIndexShardProgress(String indexShard, List<String> allNodes, String clusterId) {
        int goalStateUpdatedCount = 0;
        int actualStateConvergedCount = 0;
        List<String> updatedNodes = new ArrayList<>();
        
        for (String nodeId : allNodes) {
            try {
                // Check if goal state is updated
                if (hasGoalStateUpdated(nodeId, indexShard, clusterId)) {
                    goalStateUpdatedCount++;
                    updatedNodes.add(nodeId);
                    
                    // Check if actual state has converged
                    if (hasActualStateConverged(nodeId, indexShard, clusterId)) {
                        actualStateConvergedCount++;
                    }
                }
            } catch (Exception e) {
                log.error("Failed to check progress for node {}: {}", nodeId, e.getMessage(), e);
            }
        }
        
        return new IndexShardProgress(indexShard, goalStateUpdatedCount, actualStateConvergedCount, allNodes.size(), updatedNodes);
    }
    
    private boolean hasGoalStateUpdated(String nodeId, String indexShard, String clusterId) {
        try {
            SearchUnitGoalState goalState = metadataStore.getSearchUnitGoalState(clusterId, nodeId);
            if (goalState == null) {
                return false;
            }
            
            String[] parts = indexShard.split("/");
            String indexName = parts[0];
            String shardId = parts[1];
            
            return goalState.getLocalShards().containsKey(indexName) && 
                   goalState.getLocalShards().get(indexName).containsKey(shardId);
        } catch (Exception e) {
            log.error("Failed to check goal state for node {}: {}", nodeId, e.getMessage(), e);
            return false;
        }
    }
    
    private boolean hasActualStateConverged(String nodeId, String indexShard, String clusterId) {
        try {
            SearchUnitActualState actualState = metadataStore.getSearchUnitActualState(clusterId, nodeId);
            if (actualState == null) {
                return false;
            }
            
            String[] parts = indexShard.split("/");
            String indexName = parts[0];
            String shardId = parts[1];
            
            // Check if the node has this shard in its nodeRouting
            Map<String, List<SearchUnitActualState.ShardRoutingInfo>> nodeRouting = actualState.getNodeRouting();
            if (nodeRouting == null || !nodeRouting.containsKey(indexName)) {
                return false;
            }
            
            List<SearchUnitActualState.ShardRoutingInfo> shards = nodeRouting.get(indexName);
            return shards.stream()
                    .anyMatch(shard -> String.valueOf(shard.getShardId()).equals(shardId));
        } catch (Exception e) {
            log.error("Failed to check actual state for node {}: {}", nodeId, e.getMessage(), e);
            return false;
        }
    }
    
    private List<String> getNodesToUpdate(List<String> allNodes, IndexShardProgress progress) {
        // Return nodes that haven't been updated yet
        return allNodes.stream()
                .filter(nodeId -> !progress.getUpdatedNodes().contains(nodeId))
                .collect(java.util.stream.Collectors.toList());
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
            SearchUnitGoalState newGoalState = updateGoalStateForIndexShard(currentGoalState, indexName, shardId, role);
            
            // Set new goal state in etcd
            metadataStore.setSearchUnitGoalState(clusterId, nodeId, newGoalState);
            
        } catch (Exception e) {
            log.error("Failed to update goal state for node {}: {}", nodeId, e.getMessage(), e);
            throw e;
        }
    }
    
    private SearchUnitGoalState updateGoalStateForIndexShard(SearchUnitGoalState current, String indexName, String shardId, String role) {
        if (current == null) {
            current = new SearchUnitGoalState();
        }
        
        // Add new shard allocation using the localShards structure
        current.getLocalShards().computeIfAbsent(indexName, k -> new HashMap<>()).put(shardId, role);
        
        return current;
    }
}
