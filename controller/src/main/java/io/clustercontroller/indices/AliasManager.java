package io.clustercontroller.indices;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import io.clustercontroller.api.models.requests.AliasAction;
import io.clustercontroller.api.models.responses.BulkAliasResponse;
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
        List<String> newIndices = parseIndicesFromString(indexName);
        log.info("AliasManager - Parsed alias name: {}, new indices to add: {}", aliasName, newIndices);
        
        validateAliasName(aliasName);
        validateTargetIndices(clusterId, newIndices);
        
        // Check if alias already exists and merge indices
        CoordinatorGoalState goalState = getOrCreateCoordinatorGoalState(clusterId);
        Object existingTarget = goalState.getRemoteShards().getAliases().get(aliasName);
        List<String> finalTargetIndices = mergeIndicesWithExisting(newIndices, existingTarget, aliasName);
        
        // Save the alias configuration
        saveAliasConfiguration(clusterId, aliasName, finalTargetIndices, goalState);
        
        log.info("AliasManager - Successfully created/updated alias '{}' pointing to {} indices: {}", 
                 aliasName, finalTargetIndices.size(), finalTargetIndices);
    }
    
    /**
     * Deletes an alias or removes it from a specific index
     * 
     * @param clusterId The cluster ID
     * @param aliasName The alias name
     * @param indexName The index name (if provided, removes alias only from this index)
     * @throws Exception if alias deletion fails
     */
    public void deleteAlias(String clusterId, String aliasName, String indexName) throws Exception {
        log.info("AliasManager - Deleting alias '{}' from cluster '{}'", aliasName, clusterId);
        
        validateAliasName(aliasName);
        
        // Get current alias configuration
        CoordinatorGoalState goalState = metadataStore.getCoordinatorGoalState(clusterId);
        if (goalState == null || !goalState.getRemoteShards().getAliases().containsKey(aliasName)) {
            throw new Exception("Alias '" + aliasName + "' not found in cluster '" + clusterId + "'");
        }
        
        // If no specific index provided, delete the entire alias
        if (indexName == null || indexName.trim().isEmpty()) {
            deleteEntireAlias(clusterId, aliasName, goalState);
            return;
        }
        
        // Remove specific index from alias
        Object currentTarget = goalState.getRemoteShards().getAliases().get(aliasName);
        List<String> targetIndices = getTargetIndicesAsList(currentTarget);
        
        if (!targetIndices.contains(indexName)) {
            throw new Exception("Alias '" + aliasName + "' does not point to index '" + indexName + "'");
        }
        
        targetIndices.remove(indexName);
        log.info("AliasManager - Removed index '{}' from alias '{}'. Remaining indices: {}", 
                 indexName, aliasName, targetIndices);
        
        // Update or delete alias based on remaining indices
        updateRemainingIndices(clusterId, aliasName, targetIndices, goalState);
        saveCoordinatorGoalState(clusterId, goalState);
        log.info("AliasManager - Successfully updated/deleted alias '{}'", aliasName);
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
    
    /**
     * Updates aliases using multiple actions (add/remove) in a single operation
     * 
     * @param clusterId The cluster ID
     * @param actions List of actions (add or remove)
     * @return Response with operation results
     * @throws Exception if applying actions fails
     */
    public BulkAliasResponse updateAliases(String clusterId, List<AliasAction> actions) throws Exception {
        log.info("AliasManager - Executing bulk alias operations for cluster '{}', {} actions", clusterId, actions.size());
        
        int completed = 0;
        int failed = 0;
        
        for (AliasAction action : actions) {
            try {
                if (action.getAdd() != null) {
                    String index = action.getAdd().getIndex();
                    String alias = action.getAdd().getAlias();
                    
                    log.debug("AliasManager - Bulk add: alias '{}' -> index '{}'", alias, index);
                    createAlias(clusterId, alias, index, "{}");
                    completed++;
                    
                } else if (action.getRemove() != null) {
                    String index = action.getRemove().getIndex();
                    String alias = action.getRemove().getAlias();
                    
                    log.debug("AliasManager - Bulk remove: alias '{}' from index '{}'", alias, index);
                    deleteAlias(clusterId, alias, index);
                    completed++;
                    
                } else {
                    log.warn("AliasManager - Unknown action type in bulk operation");
                    failed++;
                }
            } catch (Exception e) {
                log.error("AliasManager - Failed to execute bulk action: {}", e.getMessage());
                failed++;
            }
        }
        
        log.info("AliasManager - Bulk operation completed: {} succeeded, {} failed", completed, failed);
        
        return BulkAliasResponse.builder()
            .acknowledged(failed == 0)
            .actionsCompleted(completed)
            .actionsFailed(failed)
            .build();
    }
    
    /**
     * Helper method to update alias target indices in both etcd and coordinator goal state
     * 
     * @param clusterId The cluster ID
     * @param aliasName The alias name
     * @param targetIndices The target indices (String for single index, List for multiple)
     * @param goalState The coordinator goal state to update
     * @throws Exception if update fails
     */
    private void updateAliasTarget(String clusterId, String aliasName, Object targetIndices, CoordinatorGoalState goalState) throws Exception {
        Alias alias = new Alias();
        alias.setAliasName(aliasName);
        alias.setTargetIndices(targetIndices);
        metadataStore.setAlias(clusterId, aliasName, alias);
        goalState.getRemoteShards().getAliases().put(aliasName, targetIndices);
        log.debug("AliasManager - Updated alias target for '{}' to: {}", aliasName, targetIndices);
    }
    
    /**
     * Helper method to convert alias target to a list of indices
     * Handles both String (single index) and List (multiple indices) formats
     * 
     * @param target The alias target (String or List<String>)
     * @return List of target indices
     */
    private List<String> getTargetIndicesAsList(Object target) {
        if (target instanceof String) {
            return new ArrayList<>(Collections.singletonList((String) target));
        } else if (target instanceof List) {
            @SuppressWarnings("unchecked")
            List<String> list = (List<String>) target;
            return new ArrayList<>(list);
        }
        return new ArrayList<>();
    }
    
    /**
     * Helper method to parse comma-separated index names into a list
     * 
     * @param indexName Comma-separated index names
     * @return List of trimmed, non-empty index names
     */
    private List<String> parseIndicesFromString(String indexName) {
        return Arrays.stream(indexName.split(","))
            .map(String::trim)
            .filter(s -> !s.isEmpty())
            .collect(Collectors.toList());
    }
    
    /**
     * Helper method to merge new indices with existing alias targets
     * Avoids duplicates and logs merge operations
     * 
     * @param newIndices New indices to add
     * @param existingTarget Existing alias target (String or List)
     * @param aliasName Alias name for logging
     * @return Merged list of target indices
     */
    private List<String> mergeIndicesWithExisting(List<String> newIndices, Object existingTarget, String aliasName) {
        List<String> finalTargetIndices = new ArrayList<>();
        
        if (existingTarget != null) {
            // Alias exists - merge with existing indices
            log.info("AliasManager - Alias '{}' already exists, adding new indices to it", aliasName);
            finalTargetIndices.addAll(getTargetIndicesAsList(existingTarget));
            
            // Add new indices (avoid duplicates)
            for (String newIndex : newIndices) {
                if (!finalTargetIndices.contains(newIndex)) {
                    finalTargetIndices.add(newIndex);
                    log.info("AliasManager - Adding index '{}' to existing alias '{}'", newIndex, aliasName);
                } else {
                    log.info("AliasManager - Index '{}' already in alias '{}', skipping", newIndex, aliasName);
                }
            }
        } else {
            // New alias - use new indices
            log.info("AliasManager - Creating new alias '{}'", aliasName);
            finalTargetIndices.addAll(newIndices);
        }
        
        return finalTargetIndices;
    }
    
    /**
     * Helper method to save alias configuration to etcd and coordinator goal state
     * 
     * @param clusterId The cluster ID
     * @param aliasName The alias name
     * @param targetIndices List of target indices
     * @param goalState The coordinator goal state to update
     * @throws Exception if save fails
     */
    private void saveAliasConfiguration(String clusterId, String aliasName, List<String> targetIndices, 
                                       CoordinatorGoalState goalState) throws Exception {
        // Convert to single string if only one index, otherwise keep as list
        Object targetValue = targetIndices.size() == 1 ? targetIndices.get(0) : targetIndices;
        
        // Store alias configuration for persistence and rebuilding
        Alias alias = new Alias();
        alias.setAliasName(aliasName);
        alias.setTargetIndices(targetValue);
        
        metadataStore.setAlias(clusterId, aliasName, alias);
        log.info("AliasManager - Stored alias config for '{}'", aliasName);
        
        // Update coordinator goal state for instant availability
        goalState.getRemoteShards().getAliases().put(aliasName, targetValue);
        saveCoordinatorGoalState(clusterId, goalState);
    }
    
    /**
     * Helper method to delete an entire alias from the cluster
     * 
     * @param clusterId The cluster ID
     * @param aliasName The alias name
     * @param goalState The coordinator goal state to update
     * @throws Exception if deletion fails
     */
    private void deleteEntireAlias(String clusterId, String aliasName, CoordinatorGoalState goalState) throws Exception {
        log.info("AliasManager - Deleting entire alias '{}' (no specific index provided)", aliasName);
        metadataStore.deleteAlias(clusterId, aliasName);
        goalState.getRemoteShards().getAliases().remove(aliasName);
        saveCoordinatorGoalState(clusterId, goalState);
        log.info("AliasManager - Successfully deleted alias '{}'", aliasName);
    }
    
    /**
     * Helper method to update alias after removing an index
     * Handles three cases: empty (delete), single index (convert to string), multiple indices (keep as list)
     * 
     * @param clusterId The cluster ID
     * @param aliasName The alias name
     * @param targetIndices Remaining target indices after removal
     * @param goalState The coordinator goal state to update
     * @throws Exception if update fails
     */
    private void updateRemainingIndices(String clusterId, String aliasName, List<String> targetIndices, 
                                       CoordinatorGoalState goalState) throws Exception {
        if (targetIndices.isEmpty()) {
            // No indices left, delete the entire alias
            log.info("AliasManager - No indices left for alias '{}', deleting entirely", aliasName);
            metadataStore.deleteAlias(clusterId, aliasName);
            goalState.getRemoteShards().getAliases().remove(aliasName);
        } else if (targetIndices.size() == 1) {
            // Only one index remains, store as string
            String remainingIndex = targetIndices.get(0);
            updateAliasTarget(clusterId, aliasName, remainingIndex, goalState);
            log.info("AliasManager - Alias '{}' now points to single index: {}", aliasName, remainingIndex);
        } else {
            // Multiple indices remain, keep as list
            updateAliasTarget(clusterId, aliasName, targetIndices, goalState);
            log.info("AliasManager - Alias '{}' now points to {} indices", aliasName, targetIndices.size());
        }
    }
}
