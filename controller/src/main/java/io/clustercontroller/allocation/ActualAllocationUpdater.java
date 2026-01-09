package io.clustercontroller.allocation;

import io.clustercontroller.enums.ShardState;
import io.clustercontroller.metrics.MetricsProvider;
import io.clustercontroller.models.Alias;
import io.clustercontroller.models.CoordinatorGoalState;
import io.clustercontroller.models.Index;
import io.clustercontroller.models.IndexSettings;
import io.clustercontroller.models.SearchUnit;
import io.clustercontroller.models.SearchUnitActualState;
import io.clustercontroller.models.SearchUnitGoalState;
import io.clustercontroller.models.ShardAllocation;
import io.clustercontroller.store.MetadataStore;
import lombok.extern.slf4j.Slf4j;
import io.clustercontroller.config.Constants;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static io.clustercontroller.metrics.MetricsConstants.UPDATE_ACTUAL_ALLOCATION_FAILURES_METRIC_NAME;
import static io.clustercontroller.metrics.MetricsUtils.buildMetricsTags;

/**
 * ActualAllocationUpdater - Responsible for aggregating search unit actual states into index shard actual allocations
 * <p>
 * This task:
 * 1. Reads actual states from /search-units/<su-name>/actual-state
 * 2. Aggregates this information by index and shard
 * 3. Updates actual allocations at /indices/<index-name>/<shard_id>/actual-allocation
 * 4. Provides coordinator nodes with a goal-state view of remote_shards used for routing/monitoring
 * <p>
 * Coordinator goal state specifics:
 * - For each index and shard, we compute a list of nodes with their role (primary/replica) flag: List<List<NodeRoutingInfo>>.
 * - A node is included only when it is converged for that index/shard.
 *   Converged means: (a) the node's goal state requests that index+shard (with a role), and
 *   (b) the node's actual state reports that shard in STARTED state.
 * - Primaries come from the current actual ingest set; replicas come from the current actual search set,
 *   and both are filtered by convergence before inclusion.
 * <p>
 * Example:
 * - Index "logs-2025" has 2 shards.
 *   - Shard 0: su-a (primary, STARTED, requested in goal) and su-b (replica, STARTED, requested in goal)
 *     -> shard[0] = [ { node: "su-a", primary: true }, { node: "su-b", primary: false } ]
 *   - Shard 1: su-c (primary, STARTED, requested in goal) and su-d (replica, STARTED, NOT requested in goal)
 *     -> shard[1] = [ { node: "su-c", primary: true } ]   // su-d excluded (not converged)
 */
@Slf4j
public class ActualAllocationUpdater {
    
    private final MetadataStore metadataStore;
    private final MetricsProvider metricsProvider;
    private static final long ONE_MINUTE_MS = 60 * 1000;
    
    public ActualAllocationUpdater(MetadataStore metadataStore, MetricsProvider metricsProvider) {
        this.metadataStore = metadataStore;
        this.metricsProvider = metricsProvider;
    }
    
    /**
     * Main entry point for actual allocation update task
     * @param clusterId The cluster to update allocations for
     */
    public void updateActualAllocations(String clusterId) throws Exception {
        log.info("ActualAllocationUpdater - Starting actual allocation update process for cluster {}", clusterId);
        
        // Get all search units to read their actual states
        List<SearchUnit> searchUnits = metadataStore.getAllSearchUnits(clusterId);
        if (searchUnits.isEmpty()) {
            log.info("ActualAllocationUpdater - No search units found for cluster {}", clusterId);
            return;
        }
        
        // Collect actual state information from all search units
        Map<String, Map<String, Set<String>>> actualAllocations = collectActualAllocations(clusterId, searchUnits);
        
        // Update actual allocation records for each index/shard combination
        int totalUpdates = updateActualAllocationRecords(clusterId, actualAllocations);
        
        // IMPORTANT: Clean up stale actual allocations that are no longer valid
        // This prevents replica duplication when replicas are moved between indices
        cleanupStaleActualAllocations(clusterId, actualAllocations);
        
        // NEW: Update coordinator goal states based on planned allocations
        int coordinatorUpdates = updateCoordinatorGoalStates(clusterId, searchUnits);
        
        log.info("ActualAllocationUpdater - Completed actual allocation update with {} updates and {} coordinator goal state updates", 
                    totalUpdates, coordinatorUpdates);
    }
    
    /**
     * Collect actual allocations from all search units
     * Returns: Map<indexName, Map<shardId, Set<unitNames>>>
     */
    private Map<String, Map<String, Set<String>>> collectActualAllocations(String clusterId, List<SearchUnit> searchUnits) {
        Map<String, Map<String, Set<String>>> actualAllocations = new HashMap<>();
        
        for (SearchUnit searchUnit : searchUnits) {
            String unitName = searchUnit.getName();
            
            try {
                // First check if the search unit itself is DRAINED or unhealthy in configuration 
                if (!Constants.ADMIN_STATE_NORMAL.equalsIgnoreCase(String.valueOf(searchUnit.getStateAdmin()))) {
                    log.debug("ActualAllocationUpdater - Skipping non-normal SU: {} (admin_state: {})", 
                        unitName, searchUnit.getStateAdmin());
                    continue;
                }
                
                // Skip coordinator nodes - they belong to a separate cluster
                if (isCoordinatorNode(searchUnit)) {
                    log.debug("ActualAllocationUpdater - Skipping coordinator node: {}", unitName);
                    continue;
                }
                
                SearchUnitActualState actualState = metadataStore.getSearchUnitActualState(clusterId, unitName);
                if (actualState == null) {
                    log.debug("ActualAllocationUpdater - No actual state found for SU: {}", unitName);
                    continue;
                }
                
                // Check if timestamp is recent (basic health check)
                if (!isNodeTimestampRecent(actualState, unitName)) {
                    continue;
                }
                
                // Process each index on this search unit using the new nodeRouting structure
                Map<String, List<SearchUnitActualState.ShardRoutingInfo>> nodeRouting = actualState.getNodeRouting();
                if (nodeRouting != null) {
                    for (Map.Entry<String, List<SearchUnitActualState.ShardRoutingInfo>> indexEntry : nodeRouting.entrySet()) {
                        String indexName = indexEntry.getKey();
                        List<SearchUnitActualState.ShardRoutingInfo> shards = indexEntry.getValue();
                        
                        // Process each shard for this index
                        if (shards != null) {
                            for (SearchUnitActualState.ShardRoutingInfo shard : shards) {
                                // Only include shards that are in STARTED state for this specific index/shard
                                if (!ShardState.STARTED.equals(shard.getState())) {
                                    log.debug("ActualAllocationUpdater - Skipping non-STARTED shard {}/{} on SU: {} (state: {})", 
                                        indexName, shard.getShardId(), unitName, shard.getState());
                                    continue;
                                }
                                
                                // Use integer shard ID directly as string
                                String shardId = String.valueOf(shard.getShardId());
                                actualAllocations
                                    .computeIfAbsent(indexName, k -> new HashMap<>())
                                    .computeIfAbsent(shardId, k -> new HashSet<>())
                                    .add(unitName);
                                
                                log.debug("ActualAllocationUpdater - Found STARTED shard {}/{} (primary: {}) on SU: {}", 
                                    indexName, shardId, shard.isPrimary(), unitName);
                            }
                        }
                    }
                }
                
            } catch (Exception e) {
                log.error("ActualAllocationUpdater - Error processing SU {}: {}", unitName, e.getMessage(), e);
                // Continue with other search units
            }
        }
        
        log.info("ActualAllocationUpdater - Collected actual allocations for {} indices", actualAllocations.size());
        return actualAllocations;
    }
    
    /**
     * Update actual allocation records based on collected data
     */
    private int updateActualAllocationRecords(String clusterId, Map<String, Map<String, Set<String>>> actualAllocations) {
        int totalUpdates = 0;
        
        for (Map.Entry<String, Map<String, Set<String>>> indexEntry : actualAllocations.entrySet()) {
            String indexName = indexEntry.getKey();
            
            for (Map.Entry<String, Set<String>> shardEntry : indexEntry.getValue().entrySet()) {
                String shardId = shardEntry.getKey();
                Set<String> allocatedUnits = shardEntry.getValue();
                
                try {
                    if (updateShardActualAllocation(clusterId, indexName, shardId, allocatedUnits)) {
                        totalUpdates++;
                    }
                } catch (Exception e) {
                    log.error("ActualAllocationUpdater - Error updating actual allocation for {}/{}: {}", 
                        indexName, shardId, e.getMessage(), e);
                    metricsProvider.counter(
                        UPDATE_ACTUAL_ALLOCATION_FAILURES_METRIC_NAME,
                        buildMetricsTags(clusterId, indexName, shardId)
                    ).increment();
                    // Continue with other shards
                }
            }
        }
        
        return totalUpdates;
    }
    
    /**
     * Update actual allocation for a specific shard
     */
    private boolean updateShardActualAllocation(String clusterId, String indexName, String shardId, Set<String> allocatedUnits) throws Exception {
        // Get current actual allocation
        ShardAllocation currentActual = metadataStore.getActualAllocation(clusterId, indexName, shardId);
        
        // Separate units by role
        List<String> ingestSUs = new ArrayList<>();
        List<String> searchSUs = new ArrayList<>();
        
        for (String unitName : allocatedUnits) {
            try {
                SearchUnit searchUnit = metadataStore.getSearchUnit(clusterId, unitName).orElse(null);
                if (searchUnit != null) {
                    String role = searchUnit.getRole() != null ? searchUnit.getRole().toUpperCase() : "UNKNOWN";
                    
                    switch (role) {
                        case "PRIMARY":
                            ingestSUs.add(unitName);
                            break;
                        case "SEARCH_REPLICA":
                        case "REPLICA":
                            searchSUs.add(unitName);
                            break;
                        case "COORDINATOR":
                            // Coordinators belong to a separate cluster and should not be included in allocations
                            log.debug("ActualAllocationUpdater - Skipping coordinator unit: {}", unitName);
                            break;
                        default:
                            log.warn("ActualAllocationUpdater - Unknown role '{}' for SU: {}", role, unitName);
                            searchSUs.add(unitName); // Default to search
                            break;
                    }
                } else {
                    log.warn("ActualAllocationUpdater - Search unit {} not found in configuration", unitName);
                    searchSUs.add(unitName); // Default to search
                }
            } catch (Exception e) {
                log.warn("ActualAllocationUpdater - Error getting search unit {}: {}", unitName, e.getMessage());
                searchSUs.add(unitName); // Default to search on error
            }
        }
        
        // Check if allocation has changed
        if (currentActual != null) {
            Set<String> currentIngestSUs = new HashSet<>(currentActual.getIngestSUs());
            Set<String> currentSearchSUs = new HashSet<>(currentActual.getSearchSUs());
            Set<String> newIngestSUs = new HashSet<>(ingestSUs);
            Set<String> newSearchSUs = new HashSet<>(searchSUs);
            
            if (currentIngestSUs.equals(newIngestSUs) && currentSearchSUs.equals(newSearchSUs)) {
                // No change needed
                log.debug("ActualAllocationUpdater - No change needed for actual allocation {}/{}", indexName, shardId);
                return false;
            }
        }
        
        // Create or update actual allocation
        ShardAllocation actualAllocation = new ShardAllocation(shardId, indexName);
        actualAllocation.setIngestSUs(ingestSUs);
        actualAllocation.setSearchSUs(searchSUs);
        
        metadataStore.setActualAllocation(clusterId, indexName, shardId, actualAllocation);
        
        log.info("ActualAllocationUpdater - Updated actual allocation for {}/{}: ingest={}, search={}", 
            indexName, shardId, ingestSUs, searchSUs);
        return true;
    }
    
    /**
     * Clean up actual allocations for shards that no longer exist on any search unit
     * Also cleans up actual allocations for deleted indices (indices without configs)
     * Uses the already-collected actual allocation data for efficiency
     */
    private void cleanupStaleActualAllocations(String clusterId, Map<String, Map<String, Set<String>>> currentActualAllocations) throws Exception {
        log.info("ActualAllocationUpdater - Starting cleanup of stale actual allocations");
        
        // Get all index configurations to know what indices exist
        List<Index> indexConfigs = metadataStore.getAllIndexConfigs(clusterId);
        Set<String> existingIndexNames = indexConfigs.stream()
            .map(Index::getIndexName)
            .collect(java.util.stream.Collectors.toSet());
        
        int totalCleanups = 0;
        
        // PHASE 1: Clean up stale allocations for EXISTING indices
        for (Index indexConfig : indexConfigs) {
            String indexName = indexConfig.getIndexName();
            
            try {
                // Get all stored actual allocations for this index
                List<ShardAllocation> storedActualAllocations = metadataStore.getAllActualAllocations(clusterId, indexName);
                
                for (ShardAllocation storedAllocation : storedActualAllocations) {
                    String shardId = storedAllocation.getShardId();
                    
                    // Check if this shard is still actually allocated according to current search unit states
                    boolean stillAllocated = currentActualAllocations.containsKey(indexName) && 
                                           currentActualAllocations.get(indexName).containsKey(shardId) &&
                                           !currentActualAllocations.get(indexName).get(shardId).isEmpty();
                    
                    if (!stillAllocated) {
                        // Clean up stale actual allocation by setting empty lists
                        ShardAllocation emptyAllocation = new ShardAllocation(shardId, indexName);
                        emptyAllocation.setIngestSUs(new ArrayList<>());
                        emptyAllocation.setSearchSUs(new ArrayList<>());
                        
                        metadataStore.setActualAllocation(clusterId, indexName, shardId, emptyAllocation);
                        totalCleanups++;
                        
                        log.info("ActualAllocationUpdater - Cleaned up stale actual allocation for {}/{}", indexName, shardId);
                    }
                }
                
            } catch (Exception e) {
                log.error("ActualAllocationUpdater - Error cleaning up actual allocations for index {}: {}", 
                    indexName, e.getMessage(), e);
                // Continue with other indexes
            }
        }
        
        // PHASE 2: Clean up actual allocations for DELETED indices (orphaned entries)
        // These are indices that have actual-allocations in etcd but no config (index was deleted)
        // NOTE: We need to check ALL indices with actual-allocations in etcd, not just those reported by search units
        Set<String> indicesWithActualAllocations = getAllIndicesWithActualAllocations(clusterId);
        
        for (String indexName : indicesWithActualAllocations) {
            if (!existingIndexNames.contains(indexName)) {
                // This index is deleted but still has actual-allocation entries (orphaned)
                log.info("ActualAllocationUpdater - Found orphaned actual-allocations for deleted index: {}", indexName);
                
                try {
                    // Get all actual allocations for this deleted index
                    List<ShardAllocation> orphanedAllocations = metadataStore.getAllActualAllocations(clusterId, indexName);
                    
                    for (ShardAllocation allocation : orphanedAllocations) {
                        String shardId = allocation.getShardId();
                        
                        // Delete the actual allocation entry completely
                        metadataStore.deleteActualAllocation(clusterId, indexName, shardId);
                        totalCleanups++;
                        
                        log.info("ActualAllocationUpdater - Deleted orphaned actual allocation for deleted index {}/{}", 
                            indexName, shardId);
                    }
                    
                } catch (Exception e) {
                    log.error("ActualAllocationUpdater - Error cleaning up orphaned actual allocations for deleted index {}: {}", 
                        indexName, e.getMessage(), e);
                    // Continue with other indexes
                }
            }
        }
        
        log.info("ActualAllocationUpdater - Completed cleanup, removed {} stale actual allocations", totalCleanups);
    }
    
    // (standalone cleanup wrapper removed; no external callers)
    
    /**
     * Get all indices that have actual-allocation entries in etcd.
     * This is needed for PHASE 2 cleanup to find orphaned allocations for deleted indices
     * that are no longer being reported by any search units.
     */
    private Set<String> getAllIndicesWithActualAllocations(String clusterId) throws Exception {
        return metadataStore.getAllIndicesWithActualAllocations(clusterId);
    }
    
    /**
     * Determines if a search unit is a coordinator node
     * Coordinator nodes belong to a separate cluster and are not part of shard allocation
     * Current logic: role="coordinator" OR name starts with "coordinator"
     * TODO: Use cluster_name information to properly identify coordinator nodes
     *       instead of relying on naming conventions. Coordinator nodes should have
     *       a different cluster_name value than data nodes.
     */
    private boolean isCoordinatorNode(SearchUnit unit) {
        if (unit == null) {
            return false;
        }
        
        // Check if role is explicitly set to coordinator
        String role = unit.getRole() != null ? unit.getRole().toLowerCase() : "";
        if ("coordinator".equals(role)) {
            return true;
        }
        
        // Check if name starts with "coordinator" prefix (for data nodes acting as coordinators)
        String name = unit.getName() != null ? unit.getName().toLowerCase() : "";
        return name.startsWith("coordinator");
    }
    
    /**
     * Update coordinator goal state based on convergence between node goal states and actual allocations
     * Builds a single remote_shards structure for the default coordinator group
     */
    public int updateCoordinatorGoalStates(String clusterId, List<SearchUnit> searchUnits) throws Exception {
        log.info("ActualAllocationUpdater - Starting coordinator goal state updates for coordinator group");
        
        // Get all index configurations that still exist
        List<Index> indexConfigs = metadataStore.getAllIndexConfigs(clusterId);
        
        // Get all alias configurations
        List<Alias> aliases = metadataStore.getAllAliases(clusterId);
        
        // Build new coordinator goal state from scratch
        CoordinatorGoalState coordinatorGoalState = new CoordinatorGoalState();
        
        // Ensure remote_shards structure exists (with null-safety guards)
        if (coordinatorGoalState.getRemoteShards() == null) {
            coordinatorGoalState.setRemoteShards(new CoordinatorGoalState.RemoteShards());
        }
        CoordinatorGoalState.RemoteShards remoteShards = coordinatorGoalState.getRemoteShards();
        
        // Initialize indices map (will be rebuilt)
        if (remoteShards.getIndices() == null) {
            remoteShards.setIndices(new HashMap<>());
        }
        Map<String, CoordinatorGoalState.IndexShardRouting> indices = remoteShards.getIndices();
        
        int totalUpdates = 0;
        List<String> indexesWithActualAllocations = new ArrayList<>();
        
        // Process all indexes that have configurations
        for (Index indexConfig : indexConfigs) {
            String indexName = indexConfig.getIndexName();
            IndexSettings settings = indexConfig.getSettings();
            if (settings == null || settings.getShardReplicaCount() == null) {
                log.warn("ActualAllocationUpdater - Index '{}' has no settings or shard replica count, skipping", indexName);
                continue;
            }
            
            List<Integer> shardReplicaCount = settings.getShardReplicaCount();
            
            // Check if this index has any actual allocations
            List<ShardAllocation> actualAllocations;
            try {
                actualAllocations = metadataStore.getAllActualAllocations(clusterId, indexName);
                if (actualAllocations.isEmpty()) {
                    log.debug("ActualAllocationUpdater - Index '{}' has no actual allocations, skipping", indexName);
                    continue;
                }
                indexesWithActualAllocations.add(indexName);
            } catch (Exception e) {
                log.warn("ActualAllocationUpdater - Error checking actual allocations for index '{}': {}", indexName, e.getMessage());
                continue;
            }
            
            log.debug("ActualAllocationUpdater - Processing coordinator routing for index '{}' with {} actual allocations", 
                indexName, actualAllocations.size());
            
            // Build shard routing array for this index
            List<List<CoordinatorGoalState.ShardNodeAssignment>> shardRouting = new ArrayList<>();
            
            for (int shardIndex = 0; shardIndex < shardReplicaCount.size(); shardIndex++) {
                String shardId = String.valueOf(shardIndex);
                
                // Get actual allocation for this shard
                ShardAllocation actual = getAggregatedActualAllocation(clusterId, indexName, shardId, searchUnits);
                if (actual.getIngestSUs().isEmpty() && actual.getSearchSUs().isEmpty()) {
                    log.debug("ActualAllocationUpdater - No actual allocation found for {}/{}", indexName, shardId);
                    // Add empty shard routing for this shard
                    shardRouting.add(new ArrayList<>());
                    continue;
                }
                
                // Build node routing info for this shard based on convergence rules
                List<CoordinatorGoalState.ShardNodeAssignment> shardNodes = new ArrayList<>();
                
                // Add primary nodes (if converged)
                for (String unitName : actual.getIngestSUs()) {
                    if (isNodeGoalStateConverged(clusterId, unitName, indexName, shardId)) {
                        CoordinatorGoalState.ShardNodeAssignment node = new CoordinatorGoalState.ShardNodeAssignment();
                        node.setNodeName(unitName);
                        node.setPrimary(true);
                        shardNodes.add(node);
                        log.debug("ActualAllocationUpdater - Including primary node '{}' in coordinator routing for {}/{}", 
                            unitName, indexName, shardId);
                    } else {
                        log.debug("ActualAllocationUpdater - Excluding primary node '{}' from coordinator routing for {}/{} (not converged)", 
                            unitName, indexName, shardId);
                    }
                }
                
                // Add search replica nodes (if converged)
                for (String unitName : actual.getSearchSUs()) {
                    if (isNodeGoalStateConverged(clusterId, unitName, indexName, shardId)) {
                        CoordinatorGoalState.ShardNodeAssignment node = new CoordinatorGoalState.ShardNodeAssignment();
                        node.setNodeName(unitName);
                        node.setPrimary(false);
                        shardNodes.add(node);
                        log.debug("ActualAllocationUpdater - Including search replica node '{}' in coordinator routing for {}/{}", 
                            unitName, indexName, shardId);
                    } else {
                        log.debug("ActualAllocationUpdater - Excluding search replica node '{}' from coordinator routing for {}/{} (not converged)", 
                            unitName, indexName, shardId);
                    }
                }
                
                shardRouting.add(shardNodes);
                totalUpdates++;
            }
            
            // Add this index to the coordinator goal state
            CoordinatorGoalState.IndexShardRouting indexShardRouting = new CoordinatorGoalState.IndexShardRouting();
            indexShardRouting.setShardRouting(shardRouting);
            indices.put(indexName, indexShardRouting);
        }
        
        // Rebuild aliases from alias configurations
        Map<String, Object> aliasMap = remoteShards.getAliases();
        if (aliasMap == null) {
            aliasMap = new HashMap<>();
            remoteShards.setAliases(aliasMap);
        }
        
        for (Alias alias : aliases) {
            String aliasName = alias.getAliasName();
            Object targetIndices = alias.getTargetIndices();
            aliasMap.put(aliasName, targetIndices);
            log.debug("ActualAllocationUpdater - Rebuilt alias '{}' -> {}", aliasName, targetIndices);
        }
        
        log.info("ActualAllocationUpdater - Rebuilt {} aliases in coordinator goal state", aliasMap.size());
        
        // Update the coordinator goal state in etcd
        try {
            metadataStore.setCoordinatorGoalState(clusterId, coordinatorGoalState);
            log.info("ActualAllocationUpdater - Updated coordinator goal state with {} indexes: {}", 
                indices.size(), indices.keySet());
        } catch (Exception e) {
            log.error("ActualAllocationUpdater - Failed to update coordinator goal state: {}", e.getMessage(), e);
            return 0;
        }
        
        log.info("ActualAllocationUpdater - Completed coordinator goal state updates: {} shard updates across {} indexes with actual allocations: {}", 
            totalUpdates, indexesWithActualAllocations.size(), indexesWithActualAllocations);
        return totalUpdates;
    }
    
    /**
     * Check if a node's goal state matches the given index/shard (i.e., converged)
     * Returns true if the node's goal state includes this index/shard, false otherwise
     */
    private boolean isNodeGoalStateConverged(String clusterId, String unitName, String indexName, String shardId) {
        try {
            SearchUnitGoalState goalState = metadataStore.getSearchUnitGoalState(clusterId, unitName);
            if (goalState == null) {
                log.debug("ActualAllocationUpdater - Node '{}' has no goal state, considering not converged for {}/{}", 
                    unitName, indexName, shardId);
                return false;
            }
            
            if (!goalState.hasIndex(indexName)) {
                log.debug("ActualAllocationUpdater - Node '{}' goal state doesn't include index '{}', not converged", 
                    unitName, indexName);
                return false;
            }
            
            List<String> goalShards = goalState.getShardsForIndex(indexName);
            boolean hasShardInGoal = goalState.getShardRole(indexName, shardId) != null;
            
            log.debug("ActualAllocationUpdater - Node '{}' goal state for index '{}': shards={}, contains {}? {}", 
                unitName, indexName, goalShards, shardId, hasShardInGoal);
            
            return hasShardInGoal;
            
        } catch (Exception e) {
            log.warn("ActualAllocationUpdater - Error checking goal state for node '{}': {}", unitName, e.getMessage());
            return false; // Assume not converged if we can't check
        }
    }
    
    /**
     * Get aggregated actual allocation for a shard by reading search unit actual states
     * This replicates the logic used for building actual allocations in the main update flow
     */
    private ShardAllocation getAggregatedActualAllocation(String clusterId, String indexName, String shardId, List<SearchUnit> searchUnits) {
        List<String> actualIngestSUs = new ArrayList<>();
        List<String> actualSearchSUs = new ArrayList<>();
        
        for (SearchUnit searchUnit : searchUnits) {
            // Skip coordinator nodes for actual allocation aggregation
            if (isCoordinatorNode(searchUnit)) {
                continue;
            }
            
            String unitName = searchUnit.getName();
            try {
                SearchUnitActualState actualState = metadataStore.getSearchUnitActualState(clusterId, unitName);
                if (actualState == null) {
                    continue;
                }
                
                if (!hasShardInActualState(actualState, indexName, shardId)) {
                    continue;
                }
                
                // Check if this unit is ingest or search based on its role and determine the shard info
                Map<String, List<SearchUnitActualState.ShardRoutingInfo>> nodeRouting = actualState.getNodeRouting();
                List<SearchUnitActualState.ShardRoutingInfo> indexShards = nodeRouting.get(indexName);
                if (indexShards != null) {
                    // Parse integer shard ID directly
                    int shardIdInt = Integer.parseInt(shardId);
                    SearchUnitActualState.ShardRoutingInfo shardInfo = indexShards.stream()
                        .filter(shard -> shard.getShardId() == shardIdInt)
                        .findFirst()
                        .orElse(null);
                    
                    if (shardInfo != null) {
                        // Determine if this unit is ingest or search based on shard's primary flag
                        if (shardInfo.isPrimary()) {
                            actualIngestSUs.add(unitName);
                        } else {
                            actualSearchSUs.add(unitName);
                        }
                    }
                }
            } catch (Exception e) {
                log.warn("ActualAllocationUpdater - Error reading actual state for unit {}: {}", unitName, e.getMessage());
            }
        }
        
        // Create aggregated actual allocation
        ShardAllocation actual = new ShardAllocation();
        actual.setIndexName(indexName);
        actual.setShardId(shardId);
        actual.setIngestSUs(actualIngestSUs);
        actual.setSearchSUs(actualSearchSUs);
        
        return actual;
    }
    
    /**
     * Helper method to check if a node has a specific shard in its actual state
     */
    private boolean hasShardInActualState(SearchUnitActualState actualState, String indexName, String shardId) {
        Map<String, List<SearchUnitActualState.ShardRoutingInfo>> nodeRouting = actualState.getNodeRouting();
        if (nodeRouting == null || !nodeRouting.containsKey(indexName)) {
            return false;
        }
        
        List<SearchUnitActualState.ShardRoutingInfo> shards = nodeRouting.get(indexName);
        if (shards == null) {
            return false;
        }
        
        // Parse integer shard ID directly
        int shardIdInt = Integer.parseInt(shardId);
        return shards.stream().anyMatch(shard -> shard.getShardId() == shardIdInt);
    }
    
    /**
     * Check if a node has a recent timestamp (< 1 minute old heartbeat)
     */
    private boolean isNodeTimestampRecent(SearchUnitActualState actualState, String unitName) {
        long currentTime = System.currentTimeMillis();
        long nodeTimestamp = actualState.getTimestamp();
        long timeDiff = currentTime - nodeTimestamp;
        
        if (timeDiff > ONE_MINUTE_MS) {
            log.debug("ActualAllocationUpdater - Skipping stale SU: {} (timestamp: {}, age: {}ms)", 
                unitName, nodeTimestamp, timeDiff);
            return false;
        }
        
        return true;
    }
    
}
