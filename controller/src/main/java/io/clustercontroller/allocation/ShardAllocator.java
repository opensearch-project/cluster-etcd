package io.clustercontroller.allocation;

import io.clustercontroller.allocation.deciders.AllNodesDecider;
import io.clustercontroller.allocation.deciders.RoleDecider;
import io.clustercontroller.allocation.deciders.ShardPoolDecider;
import io.clustercontroller.enums.NodeRole;
import io.clustercontroller.models.Index;
import io.clustercontroller.models.ShardAllocation;
import io.clustercontroller.models.SearchUnit;
import io.clustercontroller.store.MetadataStore;
import lombok.extern.slf4j.Slf4j;

import java.time.Duration;
import java.util.List;

/**
 * Handles shard allocation decisions.
 * Internal component used by TaskManager.
 */
@Slf4j
public class ShardAllocator {
    
    private final MetadataStore metadataStore;
    private AllocationDecisionEngine allocationDecisionEngine;
    
    // TODO: Make this configurable via properties
    private static final Duration RECENT_ALLOCATION_THRESHOLD = Duration.ofMinutes(5);
    
    public ShardAllocator(MetadataStore metadataStore) {
        this.metadataStore = metadataStore;
        // Use bin-packing allocation engine with random group selection strategy
        this.allocationDecisionEngine = new GroupAwareBinPackingEngine();
    }
    
    /**
     * Setter for AllocationDecisionEngine (for testing purposes)
     */
    public void setAllocationDecisionEngine(AllocationDecisionEngine allocationDecisionEngine) {
        this.allocationDecisionEngine = allocationDecisionEngine;
    }
    
    /**
     * Plan shard allocation for all indexes in cluster
     */
    public void planShardAllocation(String clusterId, AllocationStrategy strategy) {
        log.info("Planning shard allocation for cluster {} with strategy: {}", clusterId, strategy);
        
        try {
            // Get all index configs from etcd
            List<Index> indexConfigs = metadataStore.getAllIndexConfigs(clusterId);
            if (indexConfigs.isEmpty()) {
                log.info("No index configs found for cluster {}", clusterId);
                return;
            }
            
            // For each index
            for (Index indexConfig : indexConfigs) {
                String indexName = indexConfig.getIndexName();
                int numberOfShards = indexConfig.getSettings().getNumberOfShards();
                List<Integer> shardReplicaCounts = indexConfig.getSettings().getShardReplicaCount();
                
                log.debug("Processing index {} with {} shards", indexName, numberOfShards);
                
                // For each shard in the index
                for (int shardIndex = 0; shardIndex < numberOfShards; shardIndex++) {
                    String shardIdStr = String.valueOf(shardIndex);
                    
                    // Validate and get replica count for RESPECT_REPLICA_COUNT strategy
                    int replicaCount = 0; // Default for USE_ALL_AVAILABLE_NODES
                    if (strategy == AllocationStrategy.RESPECT_REPLICA_COUNT) {
                        if (shardReplicaCounts == null || shardIndex >= shardReplicaCounts.size()) {
                            log.error("Missing replica count for shard {} in index {} - required for RESPECT_REPLICA_COUNT strategy", 
                                     shardIndex, indexName);
                            continue;
                        }
                        replicaCount = shardReplicaCounts.get(shardIndex);
                    }
                    
                    // Get current planned allocation and all nodes
                    ShardAllocation currentPlanned = metadataStore.getPlannedAllocation(clusterId, indexName, shardIdStr);
                    List<SearchUnit> allNodes = metadataStore.getAllSearchUnits(clusterId);
                    
                    // Handle IngestSUs first (primary allocation)
                    List<String> ingestNodes = planIngestAllocation(clusterId, indexName, shardIndex, indexConfig, allNodes, currentPlanned);
                    if (ingestNodes == null || ingestNodes.isEmpty()) {
                        log.warn("IngestSU allocation failed or empty for shard {}/{}", indexName, shardIndex);
                    }
                    
                    // Handle SearchSUs (replica allocation)
                    List<String> searchNodes = planSearchReplicaAllocation(clusterId, indexName, shardIndex, indexConfig, replicaCount, strategy, allNodes, currentPlanned);
                    if (searchNodes.isEmpty()) {
                        log.warn("SearchSU allocation empty for shard {}/{}", indexName, shardIndex);
                    }
                    
                    // Update planned allocation in etcd (only if we have valid allocations)
                    if ((ingestNodes == null || ingestNodes.isEmpty()) && searchNodes.isEmpty()) {
                        log.warn("Skipping planned allocation update for shard {}/{} - no valid allocations", indexName, shardIndex);
                        continue;
                    }
                    
                    updatePlannedAllocation(clusterId, indexName, shardIdStr, ingestNodes, searchNodes);
                    
                    log.info("Planned allocation for shard {}/{} - IngestSUs: {}, SearchSUs: {}", 
                             indexName, shardIndex, ingestNodes, searchNodes);
                }
            }
            
            log.info("Completed shard allocation planning for cluster {} with strategy: {}", clusterId, strategy);
            
        } catch (Exception e) {
            log.error("Failed to plan shard allocation for cluster {}: {}", clusterId, e.getMessage(), e);
        }
    }
    
    /**
     * Reallocate specific shard (TODO for later PR)
     * This will be used to support APIs such as reroute which is OS compatible.
     * Supports MOVE, ALLOCATE, CANCEL operations similar to OpenSearch reroute API.
     */
    public void reallocateShard(String clusterId, String indexName, String shardId, List<String> targetNodes) {
        log.info("Reallocating shard {}/{} to nodes: {}", indexName, shardId, targetNodes);
        // TODO: Implement reallocation logic
        // Steps:
        // 1. Validate target nodes through deciders
        // 2. Update planned allocation for specific shard
        // 3. Handle move/allocate/cancel operations
    }
    
    /**
     * Check if allocation is recent (within threshold)
     */
    private boolean isRecentAllocation(String clusterId, String indexName, String shardId) {
        try {
            ShardAllocation planned = metadataStore.getPlannedAllocation(clusterId, indexName, shardId);
            if (planned == null) {
                return false; // No existing allocation
            }
            
            long allocationTime = planned.getAllocationTimestamp();
            long currentTime = System.currentTimeMillis();
            long timeDiff = currentTime - allocationTime;
            
            return timeDiff < RECENT_ALLOCATION_THRESHOLD.toMillis();
        } catch (Exception e) {
            log.warn("Failed to check recent allocation for shard {}/{}: {}", indexName, shardId, e.getMessage());
            return false;
        }
    }
    
    /**
     * Plan IngestSU allocation (primary allocation).
     * 
     * Supports both single-writer (default) and multi-writer configurations.
     * The desired number of ingesters is determined by ingestGroupsAllocateCount config.
     */
    private List<String> planIngestAllocation(String clusterId, String indexName, int shardId, 
                                            Index indexConfig, List<SearchUnit> allNodes, ShardAllocation currentPlanned) {
        try {
            // Get desired number of ingesters from config (default: 1 for single-writer)
            int desiredIngestCount = getDesiredIngestGroupCount(indexConfig, shardId);
            
            // Get eligible ingest nodes from allocation engine
            List<SearchUnit> eligibleIngestNodes = allocationDecisionEngine
                .getAvailableNodesForAllocation(shardId, indexName, indexConfig, allNodes, NodeRole.PRIMARY, currentPlanned);
            
            // Validate: should not exceed desired count
            if (eligibleIngestNodes.size() > desiredIngestCount) {
                log.error("Too many IngestSUs ({}) for shard {}/{} - expected {}. Allocation constraint violated.",
                         eligibleIngestNodes.size(), indexName, shardId, desiredIngestCount);
                // TODO: Add alert/notification for constraint violation
                return null;
            }
            
            if (eligibleIngestNodes.isEmpty()) {
                log.warn("No eligible ingest nodes found for shard {}/{}", indexName, shardId);
                return List.of();
            }
            
            // Return all eligible nodes (allocation engine already selected the correct number)
            List<String> selectedIngesters = eligibleIngestNodes.stream()
                .map(SearchUnit::getName)
                .collect(java.util.stream.Collectors.toList());
            
            log.debug("IngestSU allocation for shard {}/{}: selected {} ingester(s) (desired: {})", 
                     indexName, shardId, selectedIngesters.size(), desiredIngestCount);
            
            return selectedIngesters;
            
        } catch (Exception e) {
            log.error("Failed to plan IngestSU allocation for shard {}/{}: {}", indexName, shardId, e.getMessage(), e);
            return null;
        }
    }
    
    /**
     * Get desired number of ingester groups for a shard.
     * 
     * @return Number of ingesters (default: 1 for single-writer)
     */
    private int getDesiredIngestGroupCount(Index indexConfig, int shardId) {
        // Default: 1 ingester per shard (single writer)
        int desiredCount = 1;
        
        if (indexConfig != null && indexConfig.getSettings() != null 
            && indexConfig.getSettings().getNumIngestGroupsPerShard() != null 
            && shardId < indexConfig.getSettings().getNumIngestGroupsPerShard().size()) {
            desiredCount = indexConfig.getSettings().getNumIngestGroupsPerShard().get(shardId);
        }
        
        return desiredCount;
    }
    
    /**
     * Plan SearchSU allocation (replica allocation)
     */
    private List<String> planSearchReplicaAllocation(String clusterId, String indexName, int shardId, 
                                            Index indexConfig, int replicaCount, AllocationStrategy strategy, 
                                            List<SearchUnit> allNodes, ShardAllocation currentPlanned) {
        try {
            String shardIdStr = String.valueOf(shardId);
            
            // Get eligible search nodes
            List<SearchUnit> eligibleSearchNodes = allocationDecisionEngine
                .getAvailableNodesForAllocation(shardId, indexName, indexConfig, allNodes, NodeRole.REPLICA, currentPlanned);
            
            if (eligibleSearchNodes.isEmpty()) {
                log.warn("No eligible search nodes found for shard {}/{}", indexName, shardId);
                return List.of();
            }
            
            // Apply strategy
            List<String> targetNodes = eligibleSearchNodes.stream()
                .map(SearchUnit::getName)
                .collect(java.util.stream.Collectors.toList());
            
            switch (strategy) {
                case RESPECT_REPLICA_COUNT:
                    // Use only the specified number of replicas
                    if (targetNodes.size() > replicaCount) {
                        targetNodes = targetNodes.subList(0, replicaCount);
                    }
                    log.debug("RESPECT_REPLICA_COUNT: Using {} search nodes out of {} eligible", targetNodes.size(), eligibleSearchNodes.size());
                    break;
                    
                case USE_ALL_AVAILABLE_NODES:
                    // Use all eligible nodes
                    log.debug("USE_ALL_AVAILABLE_NODES: Using all {} eligible search nodes", targetNodes.size());
                    break;
                    
                default:
                    log.warn("Unknown allocation strategy: {}", strategy);
                    break;
            }
            
            return targetNodes;
            
        } catch (Exception e) {
            log.error("Failed to plan SearchSU allocation for shard {}/{}: {}", indexName, shardId, e.getMessage(), e);
            return List.of();
        }
    }
    
    /**
     * Update planned allocation in etcd
     */
    private void updatePlannedAllocation(String clusterId, String indexName, String shardId, 
                                       List<String> ingestNodes, List<String> searchNodes) {
        try {
            // Create new planned allocation
            ShardAllocation plannedAllocation = new ShardAllocation();
            plannedAllocation.setShardId(shardId);
            plannedAllocation.setIndexName(indexName);
            plannedAllocation.setIngestSUs(ingestNodes);
            plannedAllocation.setSearchSUs(searchNodes);
            plannedAllocation.setAllocationTimestamp(System.currentTimeMillis());
            
            // Update in etcd
            metadataStore.setPlannedAllocation(clusterId, indexName, shardId, plannedAllocation);
            
            log.debug("Updated planned allocation for shard {}/{} - IngestSUs: {}, SearchSUs: {}", 
                     indexName, shardId, ingestNodes, searchNodes);
            
        } catch (Exception e) {
            log.error("Failed to update planned allocation for shard {}/{}: {}", indexName, shardId, e.getMessage(), e);
        }
    }
    
}
