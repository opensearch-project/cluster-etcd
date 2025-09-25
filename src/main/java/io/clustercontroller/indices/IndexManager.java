package io.clustercontroller.indices;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.clustercontroller.models.Index;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.clustercontroller.models.SearchUnit;
import io.clustercontroller.models.SearchUnitGoalState;
import io.clustercontroller.store.MetadataStore;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Manages index lifecycle operations.
 * Internal component used by TaskManager.
 */
@Slf4j
public class IndexManager {
    
    private final MetadataStore metadataStore;
    private final ObjectMapper objectMapper;
    
    public IndexManager(MetadataStore metadataStore) {
        this.metadataStore = metadataStore;
        this.objectMapper = new ObjectMapper();
    }
    
    public void createIndex(String clusterId, String indexName, String indexConfig) throws Exception {
        log.info("Creating index {} in cluster {} with config: {}", indexName, clusterId, indexConfig);
        
        // Parse the JSON input to extract index configuration (settings and mappings only)
        CreateIndexRequest request = parseCreateIndexRequest(indexConfig);
        
        log.info("CreateIndex - Parsed index name: {}", indexName);
        
        // Validate the parsed input
        if (indexName == null || indexName.isEmpty()) {
            throw new Exception("Index name cannot be null or empty");
        }
        
        // Check if index already exists
        if (metadataStore.getIndexConfig(clusterId, indexName).isPresent()) {
            log.info("CreateIndex - Index '{}' already exists, skipping creation", indexName);
            return;
        }
        
        // Extract number of shards from settings, defaulting to 1 if not specified
        int numberOfShards = extractNumberOfShards(request.getSettings());
        
        // Extract number of replicas from settings, defaulting to 1 if not specified
        int numberOfReplicas = extractNumberOfReplicas(request.getSettings());
        
        // Create shard replica count list based on actual settings
        List<Integer> shardReplicaCount = new ArrayList<>();
        for (int i = 0; i < numberOfShards; i++) {
            shardReplicaCount.add(numberOfReplicas);
        }
        
        log.info("CreateIndex - Using {} shards with replica count: {}", numberOfShards, shardReplicaCount);
        
        // Create the new Index configuration
        Index newIndex = new Index();
        newIndex.setIndexName(indexName);
        newIndex.setNumberOfShards(numberOfShards);
        newIndex.setShardReplicaCount(shardReplicaCount);
        newIndex.setNumShards(numberOfShards);
        
        // Store the index configuration
        String indexConfigJson = objectMapper.writeValueAsString(newIndex);
        String documentId = metadataStore.createIndexConfig(clusterId, indexName, indexConfigJson);
        log.info("CreateIndex - Successfully created index configuration for '{}' with document ID: {}", 
            newIndex.getIndexName(), documentId);
        
        // Store mappings if provided
        if (request.getMappings() != null && !request.getMappings().isEmpty()) {
            String mappingsJson = objectMapper.writeValueAsString(request.getMappings());
            metadataStore.setIndexMappings(clusterId, indexName, mappingsJson);
            log.info("CreateIndex - Set mappings for index '{}'", indexName);
        }
        
        // Store settings if provided
        if (request.getSettings() != null && !request.getSettings().isEmpty()) {
            String settingsJson = objectMapper.writeValueAsString(request.getSettings());
            metadataStore.setIndexSettings(clusterId, indexName, settingsJson);
            log.info("CreateIndex - Set settings for index '{}'", indexName);
        }
    }
    
    public void deleteIndex(String clusterId, String indexName) throws Exception {
        log.info("DeleteIndex - Starting deletion of index '{}' from cluster '{}'", indexName, clusterId);

        // Validate input parameters
        if (indexName == null || indexName.trim().isEmpty()) {
            throw new Exception("Index name cannot be null or empty");
        }
        if (clusterId == null || clusterId.trim().isEmpty()) {
            throw new Exception("Cluster ID cannot be null or empty");
        }

        // Check if index exists
        if (!metadataStore.getIndexConfig(clusterId, indexName).isPresent()) {
            log.warn("DeleteIndex - Index '{}' not found in cluster '{}', nothing to delete", indexName, clusterId);
            return;
        }

        try {
            // Step 1: Remove all planned allocations for this index
            deleteAllPlannedAllocationsFromIndex(clusterId, indexName);

            // Step 2: Delete the index configuration from etcd
            metadataStore.deleteIndexConfig(clusterId, indexName);
            log.info("DeleteIndex - Successfully deleted index configuration for '{}' from cluster '{}'",
                indexName, clusterId);

            // Step 3: Delete index settings and mappings
            try {
                metadataStore.deleteIndexSettings(clusterId, indexName);
                log.info("DeleteIndex - Successfully deleted index settings for '{}' from cluster '{}'",
                    indexName, clusterId);
            } catch (Exception e) {
                log.warn("DeleteIndex - Failed to delete settings for '{}': {}", indexName, e.getMessage());
            }

            try {
                metadataStore.deleteIndexMappings(clusterId, indexName);
                log.info("DeleteIndex - Successfully deleted index mappings for '{}' from cluster '{}'",
                    indexName, clusterId);
            } catch (Exception e) {
                log.warn("DeleteIndex - Failed to delete mappings for '{}': {}", indexName, e.getMessage());
            }

            // Step 4: Clean up goal states for this deleted index
            cleanupGoalStatesForDeletedIndex(clusterId, indexName);

        } catch (Exception e) {
            log.error("DeleteIndex - Failed to delete index '{}' from cluster '{}': {}",
                indexName, clusterId, e.getMessage(), e);
            throw new Exception("Failed to delete index '" + indexName + "' from cluster '" + clusterId + "'", e);
        }

        log.info("DeleteIndex - Index '{}' deletion completed successfully from cluster '{}'", indexName, clusterId);
    }
    
    /**
     * Get index information.
     */
    public String getIndex(String clusterId, String indexName) {
        log.info("Getting index information for: {}", indexName);
        // TODO: Implement get index logic
        throw new UnsupportedOperationException("Get index not yet implemented");
    }
    
    /**
     * Check if index exists.
     */
    public boolean indexExists(String clusterId, String indexName) {
        log.info("Checking if index exists: {}", indexName);
        // TODO: Implement index existence check
        return false;
    }
    
    /**
     * Get index settings.
     */
    public String getSettings(String clusterId, String indexName) {
        log.info("Getting settings for index: {}", indexName);
        // TODO: Implement get settings logic
        throw new UnsupportedOperationException("Get settings not yet implemented");
    }
    
    /**
     * Update index settings.
     */
    public void updateSettings(String clusterId, String indexName, String settingsJson) {
        log.info("Updating settings for index '{}' with: {}", indexName, settingsJson);
        // TODO: Implement update settings logic
        throw new UnsupportedOperationException("Update settings not yet implemented");
    }
    
    /**
     * Get index mappings.
     */
    public String getMapping(String clusterId, String indexName) {
        log.info("Getting mapping for index: {}", indexName);
        // TODO: Implement get mapping logic
        throw new UnsupportedOperationException("Get mapping not yet implemented");
    }
    
    /**
     * Update index mappings.
     */
    public void updateMapping(String clusterId, String indexName, String mappingsJson) {
        log.info("Updating mapping for index '{}' with: {}", indexName, mappingsJson);
        // TODO: Implement update mapping logic
        throw new UnsupportedOperationException("Update mapping not yet implemented");
    }
    

    private CreateIndexRequest parseCreateIndexRequest(String input) throws Exception {
        return objectMapper.readValue(input, CreateIndexRequest.class);
    }

    /**
     * Extract the number of shards from the settings map.
     * Returns 1 as default if not specified or if parsing fails.
     */
    private int extractNumberOfShards(Map<String, Object> settings) {
        if (settings == null || settings.isEmpty()) {
            log.debug("No settings provided, using default number of shards: 1");
            return 1;
        }
        
        try {
            Object shardsObj = settings.get("number_of_shards");
            if (shardsObj != null) {
                int shards = ((Number) shardsObj).intValue();
                log.debug("Extracted number_of_shards from settings: {}", shards);
                return shards;
            } else {
                log.debug("number_of_shards not found in settings, using default: 1");
                return 1;
            }
        } catch (Exception e) {
            log.warn("Failed to extract number_of_shards from settings, using default: 1. Error: {}", e.getMessage());
            return 1;
        }
    }

    /**
     * Extract the number of replicas from the settings map.
     * Returns 1 as default if not specified or if parsing fails.
     */
    private int extractNumberOfReplicas(Map<String, Object> settings) {
        if (settings == null || settings.isEmpty()) {
            log.debug("No settings provided, using default number of replicas: 1");
            return 1;
        }
        
        try {
            Object replicasObj = settings.get("number_of_replicas");
            if (replicasObj != null) {
                int replicas = ((Number) replicasObj).intValue();
                log.debug("Extracted number_of_replicas from settings: {}", replicas);
                return replicas;
            } else {
                log.debug("number_of_replicas not found in settings, using default: 1");
                return 1;
            }
        } catch (Exception e) {
            log.warn("Failed to extract number_of_replicas from settings, using default: 1. Error: {}", e.getMessage());
            return 1;
        }
    }

    /**
     * Delete all planned allocations from an index
     */
    private void deleteAllPlannedAllocationsFromIndex(String clusterId, String indexName) throws Exception {
        log.info("DeleteIndex - Cleaning up ALL planned allocations from index '{}'", indexName);

        try {
            // Get the index configuration to determine number of shards
            Optional<String> indexConfigOpt = metadataStore.getIndexConfig(clusterId, indexName);
            if (!indexConfigOpt.isPresent()) {
                log.warn("DeleteIndex - Index '{}' not found, skipping planned allocation cleanup", indexName);
                return;
            }

            // Parse the index configuration to get number of shards
            Index index = objectMapper.readValue(indexConfigOpt.get(), Index.class);
            int numShards = index.getNumberOfShards();

            log.info("DeleteIndex - Deleting planned allocations for {} shards from index '{}'", numShards, indexName);

            // Delete planned allocations for shards 0 to numShards-1
            for (int shardId = 0; shardId < numShards; shardId++) {
                try {
                    metadataStore.deletePlannedAllocation(clusterId, indexName, String.valueOf(shardId));
                } catch (Exception e) {
                    log.warn("DeleteIndex - Failed to delete planned allocation {}/{}: {}", 
                        indexName, shardId, e.getMessage());
                }
            }

            log.info("DeleteIndex - Cleaned up planned allocations for {} shards from index '{}'", 
                numShards, indexName);

        } catch (Exception e) {
            log.error("DeleteIndex - Failed to cleanup planned allocations from index '{}': {}", 
                indexName, e.getMessage());
            throw e;
        }
    }

    /**
     * Cleans up goal states (local shards) for an index from all search units
     */
    private void cleanupGoalStatesForDeletedIndex(String clusterId, String deletedIndexName) throws Exception {
        log.info("DeleteIndex - Starting immediate goal state cleanup for deleted index '{}'", deletedIndexName);
        try {
            // Get all search units to check their goal states
            List<SearchUnit> allSearchUnits = metadataStore.getAllSearchUnits(clusterId);
            for (SearchUnit searchUnit : allSearchUnits) {
                String unitName = searchUnit.getName();
                try {
                    // Get current goal state for this unit
                    Optional<SearchUnitGoalState> goalStateOpt = metadataStore.getSearchUnitGoalState(clusterId, unitName);
                    if (!goalStateOpt.isPresent()) {
                        log.debug("DeleteIndex - No goal state found for unit '{}'", unitName);
                        continue;
                    }
                    SearchUnitGoalState goalState = goalStateOpt.get();
                    // Check if this unit has the deleted index in its goal state
                    if (!goalState.getLocalShards().containsKey(deletedIndexName)) {
                        log.debug("DeleteIndex - Unit '{}' does not have deleted index '{}' in goal state", unitName, deletedIndexName);
                        continue;
                    }
                    // Remove the deleted index from goal state
                    Map<String, Map<String, String>> localShards = goalState.getLocalShards();
                    localShards.remove(deletedIndexName);
                    // Save updated goal state
                    metadataStore.updateSearchUnitGoalState(clusterId, unitName, goalState);
                    log.info("DeleteIndex - Removed deleted index '{}' from goal state of unit '{}'", deletedIndexName, unitName);
                } catch (Exception e) {
                    log.error("DeleteIndex - Failed to cleanup goal state for unit '{}': {}", unitName, e.getMessage(), e);
                }
            }
        } catch (Exception e) {
            log.error("DeleteIndex - Failed to get search units for goal state cleanup: {}", e.getMessage(), e);
            throw e;
        }
        log.info("DeleteIndex - Completed immediate goal state cleanup for deleted index '{}'", deletedIndexName);
    }

    /**
     * Data class to hold parsed create index request
     */
    @Data
    @NoArgsConstructor
    private static class CreateIndexRequest {
        @JsonProperty("mappings")
        private Map<String, Object> mappings; // Optional mappings JSON
        
        @JsonProperty("settings")
        private Map<String, Object> settings; // Optional settings JSON
    }
}