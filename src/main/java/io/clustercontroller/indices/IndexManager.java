package io.clustercontroller.indices;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.clustercontroller.models.Index;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.clustercontroller.models.ShardData;
import io.clustercontroller.models.SearchUnit;
import io.clustercontroller.store.MetadataStore;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.List;

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
        
        // Get all available search units for allocation planning
        List<SearchUnit> availableUnits = metadataStore.getAllSearchUnits(clusterId);
        
        if (availableUnits.isEmpty()) {
            throw new Exception("No search units available for index allocation");
        }
        
        // TODO: Determine number of shards from settings, adding default 1 shard for now
        int numberOfShards = 1;
        
        // TODO: Calculate maximum replicas per shard based on available search units
        List<Integer> shardReplicaCount = new ArrayList<>();
        shardReplicaCount.add(1);
        
        log.info("CreateIndex - Using {} shards with replica count: {}", numberOfShards, shardReplicaCount);
        
        // TODO: Create allocation plan
        List<ShardData> allocationPlan = new ArrayList<>();
        
        // Create the new Index configuration
        Index newIndex = new Index();
        newIndex.setIndexName(indexName);
        newIndex.setShardReplicaCount(shardReplicaCount);
        newIndex.setAllocationPlan(allocationPlan);
        
        // Store the index configuration
        String indexConfigJson = objectMapper.writeValueAsString(newIndex);
        String documentId = metadataStore.createIndexConfig(clusterId, indexName, indexConfigJson);
        log.info("CreateIndex - Successfully created index configuration for '{}' with document ID: {}", 
            newIndex.getIndexName(), documentId);
        
        // Store mappings if provided
        if (request.getMappings() != null && !request.getMappings().trim().isEmpty()) {
            metadataStore.setIndexMappings(clusterId, indexName, request.getMappings());
            log.info("CreateIndex - Set mappings for index '{}'", indexName);
        }
        
        // Store settings if provided
        if (request.getSettings() != null && !request.getSettings().trim().isEmpty()) {
            metadataStore.setIndexSettings(clusterId, indexName, request.getSettings());
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
    
    public void planShardAllocation() throws Exception {
        log.info("Planning shard allocation");
        // TODO: Implement shard allocation planning logic
    }

    private CreateIndexRequest parseCreateIndexRequest(String input) throws Exception {
        return objectMapper.readValue(input, CreateIndexRequest.class);
    }

    /**
     * Data class to hold parsed create index request
     */
    @Data
    @NoArgsConstructor
    private static class CreateIndexRequest {
        @JsonProperty("mappings")
        private String mappings; // Optional mappings JSON
        
        @JsonProperty("settings")
        private String settings; // Optional settings JSON
    }
}