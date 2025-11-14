package io.clustercontroller.indices;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import io.clustercontroller.models.CoordinatorGoalState;
import io.clustercontroller.models.Alias;
import io.clustercontroller.store.MetadataStore;
import lombok.extern.slf4j.Slf4j;

import java.util.*;
import java.util.stream.Collectors;

/**
 * Manages index alias operations with multi-cluster support.
 * Provides methods for creating, deleting, and retrieving aliases for indices.
 * Aliases allow referring to one or more indices by a different name,
 * simplifying index management and reindexing operations.
 */
@Slf4j
public class AliasManager {
    private static final ObjectMapper objectMapper = new ObjectMapper()
            .registerModule(new JavaTimeModule())
            .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);

    private final MetadataStore metadataStore;

    public AliasManager(MetadataStore metadataStore) {
        this.metadataStore = metadataStore;
    }
    
    /**
     * Creates an alias that points to one or more indices
     * This is called from the API handler
     * 
     * @param clusterId The cluster ID
     * @param aliasName The alias name
     * @param indexName The index name (or comma-separated list)
     * @param aliasConfig Optional alias configuration
     * @throws Exception if alias creation fails
     */
    public void createAlias(String clusterId, String aliasName, String indexName, String aliasConfig) throws Exception {
        log.info("AliasManager - Creating alias '{}' for index '{}' in cluster '{}'", aliasName, indexName, clusterId);
        
        // Parse target indices from the indexName parameter (could be comma-separated)
        List<String> targetIndices = Arrays.stream(indexName.split(","))
            .map(String::trim)
            .filter(s -> !s.isEmpty())
            .collect(Collectors.toList());
        
        log.info("AliasManager - Parsed alias name: {}, target indices: {}", aliasName, targetIndices);
        
        validateAliasName(aliasName);
        validateTargetIndices(clusterId, targetIndices);
        
        // Store alias configuration for persistence and rebuilding
        Object targetValue = targetIndices.size() == 1 ? targetIndices.get(0) : targetIndices;
        Alias alias = new Alias(aliasName, targetValue);
        String timestamp = java.time.Instant.now().toString();
        alias.setCreatedAt(timestamp);
        alias.setUpdatedAt(timestamp);
        
        metadataStore.setAlias(clusterId, aliasName, alias);
        log.info("AliasManager - Stored alias config for '{}'", aliasName);
        
        // Immediately update coordinator goal state for instant availability
        // (ActualAllocationUpdater will rebuild periodically as backup)
        CoordinatorGoalState goalState = getOrCreateCoordinatorGoalState(clusterId);
        goalState.getRemoteShards().getAliases().put(aliasName, targetValue);
        saveCoordinatorGoalState(clusterId, goalState);
        
        log.info("AliasManager - Successfully created alias '{}' pointing to {} indices: {}", 
                 aliasName, targetIndices.size(), targetIndices);
    }
    
    /**
     * Deletes an alias from the coordinator goal state
     * 
     * @param clusterId The cluster ID
     * @param aliasName The alias name
     * @param indexName The index name (optional, for API compatibility)
     * @throws Exception if alias deletion fails
     */
    public void deleteAlias(String clusterId, String aliasName, String indexName) throws Exception {
        log.info("AliasManager - Deleting alias '{}' from cluster '{}'", aliasName, clusterId);
        
        validateAliasName(aliasName);
        
        // Delete alias from storage
        metadataStore.deleteAlias(clusterId, aliasName);
        log.info("AliasManager - Deleted alias config for '{}'", aliasName);
        
        // Immediately remove from coordinator goal state
        // (ActualAllocationUpdater will rebuild periodically as backup)
        CoordinatorGoalState goalState = metadataStore.getCoordinatorGoalState(clusterId);
        if (goalState != null && goalState.getRemoteShards().getAliases().containsKey(aliasName)) {
            goalState.getRemoteShards().getAliases().remove(aliasName);
            saveCoordinatorGoalState(clusterId, goalState);
        }
        
        log.info("AliasManager - Successfully deleted alias '{}'", aliasName);
    }
    
    /**
     * Get alias information
     */
    public String getAlias(String clusterId, String aliasName) throws Exception {
        log.info("AliasManager - Getting alias '{}' from cluster '{}'", aliasName, clusterId);
        
        CoordinatorGoalState goalState = metadataStore.getCoordinatorGoalState(clusterId);
        if (goalState == null) {
            throw new Exception("No coordinator goal state found for cluster '" + clusterId + "'");
        }
        
        Map<String, Object> aliases = goalState.getRemoteShards().getAliases();
        
        if (!aliases.containsKey(aliasName)) {
            throw new Exception("Alias '" + aliasName + "' not found in cluster '" + clusterId + "'");
        }
        
        // Return as JSON
        Map<String, Object> response = new HashMap<>();
        response.put(aliasName, aliases.get(aliasName));
        return objectMapper.writeValueAsString(response);
    }
    
    /**
     * Check if alias exists
     */
    public boolean aliasExists(String clusterId, String aliasName) {
        log.info("AliasManager - Checking if alias '{}' exists in cluster '{}'", aliasName, clusterId);
        try {
            CoordinatorGoalState goalState = metadataStore.getCoordinatorGoalState(clusterId);
            if (goalState == null) {
                return false;
            }
            return goalState.getRemoteShards().getAliases().containsKey(aliasName);
        } catch (Exception e) {
            log.error("AliasManager - Error checking if alias exists: {}", e.getMessage());
            return false;
        }
    }
    
    // =================================================================
    // VALIDATION METHODS
    // =================================================================
    
    /**
     * Helper method to validate alias name
     */
    private void validateAliasName(String aliasName) throws Exception {
        if (aliasName == null || aliasName.trim().isEmpty()) {
            throw new Exception("Alias name cannot be null or empty");
        }
        
        if (aliasName.contains(" ")) {
            throw new Exception("Alias name cannot contain spaces");
        }
    }
    
    /**
     * Helper method to validate target indices
     */
    private void validateTargetIndices(String clusterId, List<String> targetIndices) throws Exception {
        if (targetIndices == null || targetIndices.isEmpty()) {
            throw new Exception("Target indices cannot be null or empty");
        }
        
        // Validate that all target indices exist
        for (String targetIndex : targetIndices) {
            if (targetIndex == null || targetIndex.trim().isEmpty()) {
                throw new Exception("Target index cannot be null or empty");
            }
            if (!metadataStore.getIndexConfig(clusterId, targetIndex).isPresent()) {
                throw new Exception("Target index '" + targetIndex + "' does not exist in cluster '" + clusterId + "'");
            }
        }
    }
    
    // =================================================================
    // COORDINATOR GOAL STATE METHODS
    // =================================================================
    
    /**
     * Helper method to get or create coordinator goal state
     */
    private CoordinatorGoalState getOrCreateCoordinatorGoalState(String clusterId) throws Exception {
        CoordinatorGoalState goalState = metadataStore.getCoordinatorGoalState(clusterId);
        
        if (goalState != null) {
            return goalState;
        } else {
            goalState = new CoordinatorGoalState();
            log.info("AliasManager - Created new coordinator goal state for cluster '{}'", clusterId);
            return goalState;
        }
    }
    
    /**
     * Helper method to save coordinator goal state to etcd
     */
    private void saveCoordinatorGoalState(String clusterId, CoordinatorGoalState goalState) throws Exception {
        try {
            metadataStore.setCoordinatorGoalState(clusterId, goalState);
            log.info("AliasManager - Saved coordinator goal state for cluster '{}'", clusterId);
        } catch (Exception e) {
            log.error("AliasManager - Failed to save coordinator goal state: {}", e.getMessage(), e);
            throw new Exception("Failed to save coordinator goal state: " + e.getMessage(), e);
        }
    }
}
