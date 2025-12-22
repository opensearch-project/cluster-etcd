package io.clustercontroller.orchestration;

import io.clustercontroller.enums.NodeRole;
import io.clustercontroller.metrics.MetricsProvider;
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

import static io.clustercontroller.metrics.MetricsConstants.ROLLING_UPDATE_PROGRESS_PERCENTAGE_METRIC_NAME;
import static io.clustercontroller.metrics.MetricsConstants.ROLLING_UPDATE_TRANSIT_NODES_PERCENTAGE_METRIC_NAME;
import static io.clustercontroller.metrics.MetricsUtils.buildMetricsTagsByRole;

/**
 * Sophisticated strategy that implements rolling updates with configurable transit percentage
 * Maintains a maximum percentage of nodes in transit for any index-shard
 */
@Component
@Slf4j
public class RollingUpdateOrchestrationStrategy implements GoalStateOrchestrationStrategy {
    private final MetadataStore metadataStore;
    private final MetricsProvider metricsProvider;
    private final double maxTransitPercentage = 0.20; // TODO: Make configurable

    public RollingUpdateOrchestrationStrategy(MetadataStore metadataStore, MetricsProvider metricsProvider) {
        this.metadataStore = metadataStore;
        this.metricsProvider = metricsProvider;
    }
    
    @Override
    public void orchestrate(String clusterId) {
        log.info("Starting rolling update orchestration for cluster: {}", clusterId);
        
        try {
            // PHASE 1: Cleanup stale goal states (instant deletion, no orchestration)
            cleanupStaleGoalStates(clusterId);
            
            // PHASE 2: Orchestrate new goal states (rolling update)
            // Get all index configs to iterate over indexes and shards
            List<Index> indexConfigs = metadataStore.getAllIndexConfigs(clusterId);
            
            // Outer loop: Iterate over indexes
            for (Index indexConfig : indexConfigs) {
                String indexName = indexConfig.getIndexName();
                int numberOfShards = indexConfig.getSettings().getNumberOfShards();
                
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
    
    private void orchestrateIndexShard(String indexName, String shardId, ShardAllocation planned, String clusterId) {
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
    
    private void orchestrateNodeGroup(String indexName, String shardId, List<String> nodeGroup, NodeRole role, ShardAllocation planned, String clusterId) {
        String indexShard = indexName + "/" + shardId;
        
        // Check current progress for this node group
        IndexShardProgress progress = getIndexShardProgress(indexShard, nodeGroup, clusterId);
        int successfulUpdates = 0;
        
        // Calculate how many nodes can be updated (20% of this group)
        int availableSlots = progress.getAvailableSlots(maxTransitPercentage);

        // Get nodes that haven't been updated yet
        List<String> nodesToUpdate = getNodesToUpdate(nodeGroup, progress);

        if (availableSlots > 0) {
            // Update next batch
            int batchSize = Math.min(availableSlots, nodesToUpdate.size());
            List<String> nextBatch = nodesToUpdate.stream()
                    .limit(batchSize)
                    .toList();
            
            log.info("Starting batch update for {} {} nodes for shard {}/{}: {} nodes ({}% in transit)", 
                    role, nodeGroup.size(), indexName, shardId, nextBatch.size(), 
                    (progress.getTransitNodesCount() + nextBatch.size()) * 100 / nodeGroup.size());
            
            for (String nodeId : nextBatch) {
                try {
                    updateNodeGoalState(nodeId, indexName, shardId, planned, clusterId);
                    successfulUpdates++;
                    log.debug("Started update for {} node {} with shard {}/{}", role, nodeId, indexName, shardId);
                } catch (Exception e) {
                    log.error("Failed to start update for {} node {}: {}", role, nodeId, e.getMessage(), e);
                }
            }
        } else {
            double transitPercent = progress.getTransitPercentage() * 100;
            log.info("Index-shard {} {} group has {}% nodes in transit, waiting for convergence", 
                    indexShard, role, transitPercent);
            metricsProvider.gauge(
                ROLLING_UPDATE_TRANSIT_NODES_PERCENTAGE_METRIC_NAME,
                transitPercent,
                buildMetricsTagsByRole(clusterId, indexName, shardId, role)
            );
        }
        metricsProvider.gauge(
            ROLLING_UPDATE_PROGRESS_PERCENTAGE_METRIC_NAME,
            (progress.getGoalStateUpdatedCount() + successfulUpdates) * 100.0 / nodeGroup.size(),
            buildMetricsTagsByRole(clusterId, indexName, shardId, role)
        );
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
    
    /**
     * Cleanup phase: Remove stale goal states that are no longer in planned allocations.
     * This is executed instantly (no orchestration) for ALL nodes.
     * Logic:
     * 1. Get all nodes that have goal states
     * 2. For each node, check its goal state
     * 3. For each index/shard in the goal state:
     *    - Verify if this node is still in the planned allocation
     *    - If NOT, remove it immediately
     * 
     * @param clusterId the cluster ID to clean up
     */
    private void cleanupStaleGoalStates(String clusterId) {
        log.info("Starting goal state cleanup phase for cluster: {}", clusterId);
        
        try {
            // Get ALL nodes that have goal states (including decommissioned nodes without conf files)
            // This ensures we clean up orphaned goal states from deleted/decommissioned nodes
            List<String> allNodeNames = metadataStore.getAllNodesWithGoalStates(clusterId);
            
            int totalCleaned = 0;
            for (String nodeName : allNodeNames) {
                try {
                    int cleaned = cleanupNodeGoalState(clusterId, nodeName);
                    totalCleaned += cleaned;
                } catch (Exception e) {
                    log.error("Failed to cleanup goal state for node {}: {}", nodeName, e.getMessage(), e);
                }
            }
            
            if (totalCleaned > 0) {
                log.info("Cleanup phase completed: removed {} stale goal state entries", totalCleaned);
            } else {
                log.debug("Cleanup phase completed: no stale goal states found");
            }
            
        } catch (Exception e) {
            log.error("Failed to cleanup goal states for cluster {}: {}", clusterId, e.getMessage(), e);
        }
    }
    
    /**
     * Cleanup goal state for a single node
     * 
     * @param clusterId the cluster ID
     * @param nodeId the node ID to clean up
     * @return number of index/shard entries removed
     */
    private int cleanupNodeGoalState(String clusterId, String nodeId) throws Exception {
        // Get current goal state for the node
        SearchUnitGoalState goalState = metadataStore.getSearchUnitGoalState(clusterId, nodeId);
        
        if (goalState == null || goalState.getLocalShards().isEmpty()) {
            return 0; // No goal state to clean up
        }
        
        Map<String, Map<String, String>> localShards = goalState.getLocalShards();
        Map<String, Map<String, String>> shardsToKeep = new HashMap<>();
        int removedCount = 0;
        
        // Iterate through all index/shards in the current goal state
        for (Map.Entry<String, Map<String, String>> indexEntry : localShards.entrySet()) {
            String indexName = indexEntry.getKey();
            Map<String, String> shards = indexEntry.getValue();
            Map<String, String> shardsToKeepForIndex = new HashMap<>();
            
            for (Map.Entry<String, String> shardEntry : shards.entrySet()) {
                String shardId = shardEntry.getKey();
                String role = shardEntry.getValue();
                
                // Check if this node is still in the planned allocation for this index/shard
                ShardAllocation planned = metadataStore.getPlannedAllocation(clusterId, indexName, shardId);
                
                if (planned == null) {
                    // No planned allocation exists - remove from goal state
                    log.info("Removing stale goal state: node={}, index/shard={}/{} (no planned allocation)", 
                            nodeId, indexName, shardId);
                    removedCount++;
                    continue;
                }
                
                // Check if node is in the planned allocation based on role
                boolean isInPlannedAllocation = false;
                if (NodeRole.PRIMARY.getValue().equals(role)) {
                    isInPlannedAllocation = planned.getIngestSUs().contains(nodeId);
                } else if (NodeRole.REPLICA.getValue().equals(role)) {
                    isInPlannedAllocation = planned.getSearchSUs().contains(nodeId);
                }
                
                if (isInPlannedAllocation) {
                    // Keep this shard in goal state
                    shardsToKeepForIndex.put(shardId, role);
                } else {
                    // Remove from goal state
                    log.info("Removing stale goal state: node={}, index/shard={}/{}, role={} (not in planned allocation)", 
                            nodeId, indexName, shardId, role);
                    removedCount++;
                }
            }
            
            // Only keep index entry if it has shards
            if (!shardsToKeepForIndex.isEmpty()) {
                shardsToKeep.put(indexName, shardsToKeepForIndex);
            }
        }
        
        // If any changes were made, update the goal state
        if (removedCount > 0) {
            SearchUnitGoalState updatedGoalState = new SearchUnitGoalState();
            updatedGoalState.setLocalShards(shardsToKeep);
            metadataStore.setSearchUnitGoalState(clusterId, nodeId, updatedGoalState);
            log.debug("Updated goal state for node {} after cleanup: {} index/shard entries removed", 
                    nodeId, removedCount);
        }
        
        return removedCount;
    }
}
