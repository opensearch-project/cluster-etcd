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
        this.allocationDecisionEngine = new AllocationDecisionEngine();
        
        // Enable deciders for now (later configurable)
        allocationDecisionEngine.enableDecider(AllNodesDecider.class);
        allocationDecisionEngine.enableDecider(RoleDecider.class);
        allocationDecisionEngine.enableDecider(ShardPoolDecider.class);
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
                    String shardId = String.format("%02d", shardIndex);
                    
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
                    ShardAllocation currentPlanned = metadataStore.getPlannedAllocation(clusterId, indexName, shardId);
                    List<SearchUnit> allNodes = metadataStore.getAllSearchUnits(clusterId);
                    
                    // Handle IngestSUs first (primary allocation)
                    List<String> ingestNodes = planIngestAllocation(clusterId, indexName, shardId, allNodes, currentPlanned);
                    if (ingestNodes == null || ingestNodes.isEmpty()) {
                        log.warn("IngestSU allocation failed or empty for shard {}/{}", indexName, shardId);
                    }
                    
                    // Handle SearchSUs (replica allocation)
                    List<String> searchNodes = planSearchReplicaAllocation(clusterId, indexName, shardId, replicaCount, strategy, allNodes, currentPlanned);
                    if (searchNodes.isEmpty()) {
                        log.warn("SearchSU allocation empty for shard {}/{}", indexName, shardId);
                    }
                    
                    // Update planned allocation in etcd (only if we have valid allocations)
                    if ((ingestNodes == null || ingestNodes.isEmpty()) && searchNodes.isEmpty()) {
                        log.warn("Skipping planned allocation update for shard {}/{} - no valid allocations", indexName, shardId);
                        continue;
                    }
                    
                    updatePlannedAllocation(clusterId, indexName, shardId, ingestNodes, searchNodes);
                    
                    log.info("Planned allocation for shard {}/{} - IngestSUs: {}, SearchSUs: {}", 
                             indexName, shardId, ingestNodes, searchNodes);
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
     * Plan IngestSU allocation (primary allocation)
     */
    private List<String> planIngestAllocation(String clusterId, String indexName, String shardId, 
                                            List<SearchUnit> allNodes, ShardAllocation currentPlanned) {
        try {
            List<String> currentIngestSUs = (currentPlanned != null) ? currentPlanned.getIngestSUs() : List.of();
            
            // Get eligible ingest nodes
            List<SearchUnit> eligibleIngestNodes = allocationDecisionEngine
                .getAvailableNodesForAllocation(shardId, indexName, allNodes, NodeRole.PRIMARY);
            
            // Validate single writer constraint - fatal if current OR eligible has more than 1
            if (currentIngestSUs.size() > 1 || eligibleIngestNodes.size() > 1) {
                log.error("Multiple IngestSUs detected for shard {}/{} - Current: {}, Eligible: {}. " +
                         "This violates single writer constraint.", indexName, shardId, currentIngestSUs,
                         eligibleIngestNodes.stream().map(SearchUnit::getName).collect(java.util.stream.Collectors.toList()));
                // TODO: Add alert/notification for multiple IngestSUs violation
                return null;
            }
            
            if (eligibleIngestNodes.isEmpty()) {
                log.warn("No eligible ingest nodes found for shard {}/{}", indexName, shardId);
                return List.of();
            }
            
            // TODO: Use algorithm to pick a leader out of multiple ingester nodes
            // For now, just pick the first eligible node
            return List.of(eligibleIngestNodes.get(0).getName());
            
        } catch (Exception e) {
            log.error("Failed to plan IngestSU allocation for shard {}/{}: {}", indexName, shardId, e.getMessage(), e);
            return null;
        }
    }
    
    /**
     * Plan SearchSU allocation (replica allocation)
     */
    private List<String> planSearchReplicaAllocation(String clusterId, String indexName, String shardId, 
                                            int replicaCount, AllocationStrategy strategy, List<SearchUnit> allNodes, 
                                            ShardAllocation currentPlanned) {
        try {
            // Check if allocation is recent (within threshold) - only for replicas
            if (isRecentAllocation(clusterId, indexName, shardId)) {
                log.debug("Skipping replica allocation for shard {}/{} - recent allocation", indexName, shardId);
                // Return current planned search nodes if recent
                return (currentPlanned != null) ? currentPlanned.getSearchSUs() : List.of();
            }
            
            // Get eligible search nodes
            List<SearchUnit> eligibleSearchNodes = allocationDecisionEngine
                .getAvailableNodesForAllocation(shardId, indexName, allNodes, NodeRole.REPLICA);
            
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
