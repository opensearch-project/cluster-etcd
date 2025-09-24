package io.clustercontroller.indices;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.clustercontroller.models.Index;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.clustercontroller.models.SearchUnit;
import io.clustercontroller.store.MetadataStore;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

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
        
        // TODO: Calculate maximum replicas per shard based on available search units
        List<Integer> shardReplicaCount = new ArrayList<>();
        shardReplicaCount.add(1);
        
        log.info("CreateIndex - Using {} shards with replica count: {}", numberOfShards, shardReplicaCount);
        
        // Create the new Index configuration
        Index newIndex = new Index();
        newIndex.setIndexName(indexName);
        newIndex.setShardReplicaCount(shardReplicaCount);
        
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
    
    public void deleteIndex(String clusterId, String indexName) {
        log.info("Deleting index {} from cluster {}", indexName, clusterId);
        // TODO: Implement index deletion logic
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