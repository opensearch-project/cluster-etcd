package io.clustercontroller.indices;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.clustercontroller.models.Index;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.clustercontroller.models.Template;
import io.clustercontroller.store.EtcdPathResolver;
import io.clustercontroller.store.MetadataStore;
import io.clustercontroller.models.IndexSettings;
import io.clustercontroller.templates.TemplateManager;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Manages index lifecycle operations.
 * Internal component used by TaskManager.
 * 
 * Automatically applies matching index templates during index creation,
 * mirroring OpenSearch behavior where templates are matched by pattern
 * and applied based on priority.
 */
@Slf4j
public class IndexManager {
    
    private final MetadataStore metadataStore;
    private final ObjectMapper objectMapper;
    private final EtcdPathResolver pathResolver;
    private final TemplateManager templateManager;
    
    public IndexManager(MetadataStore metadataStore, TemplateManager templateManager) {
        this.metadataStore = metadataStore;
        this.templateManager = templateManager;
        this.objectMapper = new ObjectMapper();
        this.pathResolver = EtcdPathResolver.getInstance();
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
        
        // Step 1: Find and apply matching templates (mirrors OpenSearch behavior)
        Map<String, Object> finalSettings = new HashMap<>();
        Map<String, Object> finalMappings = new HashMap<>();
        Map<String, Object> finalAliases = new HashMap<>();
        
        try {
            List<Template> matchingTemplates = templateManager.findMatchingTemplates(clusterId, indexName);
            if (!matchingTemplates.isEmpty()) {
                log.info("CreateIndex - Applying {} matching template(s) to index '{}'", 
                    matchingTemplates.size(), indexName);
                
                // Merge all matching templates according to priority
                Template.TemplateDefinition mergedTemplate = templateManager.mergeTemplates(matchingTemplates);
                
                // Apply template settings, mappings, and aliases
                if (mergedTemplate.getSettings() != null) {
                    finalSettings.putAll(mergedTemplate.getSettings());
                }
                if (mergedTemplate.getMappings() != null) {
                    finalMappings.putAll(mergedTemplate.getMappings());
                }
                if (mergedTemplate.getAliases() != null) {
                    finalAliases.putAll(mergedTemplate.getAliases());
                }
                
                log.info("CreateIndex - Template settings: {}", finalSettings);
                log.info("CreateIndex - Template mappings: {}", finalMappings);
                log.info("CreateIndex - Template aliases: {}", finalAliases);
            } else {
                log.info("CreateIndex - No matching templates found for index '{}'", indexName);
            }
        } catch (Exception e) {
            log.warn("CreateIndex - Failed to apply templates for index '{}': {}. Continuing with user-provided config.", 
                indexName, e.getMessage());
        }
        
        // Step 2: Merge user-provided settings (user settings override template settings)
        if (request.getSettings() != null && !request.getSettings().isEmpty()) {
            log.info("CreateIndex - Merging user-provided settings with template settings");
            deepMerge(finalSettings, request.getSettings());
        }
        
        // Step 3: Merge user-provided mappings (user mappings override template mappings)
        if (request.getMappings() != null && !request.getMappings().isEmpty()) {
            log.info("CreateIndex - Merging user-provided mappings with template mappings");
            deepMerge(finalMappings, request.getMappings());
        }
        
        // Step 4: Merge user-provided aliases (user aliases override template aliases)
        if (request.getAliases() != null && !request.getAliases().isEmpty()) {
            log.info("CreateIndex - Merging user-provided aliases with template aliases");
            finalAliases.putAll(request.getAliases());
        }
        
        // Extract number of shards from final merged settings, defaulting to 1 if not specified
        int numberOfShards = extractNumberOfShards(finalSettings);
        
        // Extract number of replicas from final merged settings, defaulting to 1 if not specified
        int numberOfReplicas = extractNumberOfReplicas(finalSettings);
        
        // Create shard replica count list based on actual settings
        List<Integer> shardReplicaCount = new ArrayList<>();
        for (int i = 0; i < numberOfShards; i++) {
            shardReplicaCount.add(numberOfReplicas);
        }
        
        log.info("CreateIndex - Using {} shards with replica count: {}", numberOfShards, shardReplicaCount);
        
        // Create the new Index configuration
        Index newIndex = new Index();
        newIndex.setIndexName(indexName);

        newIndex.setSettings(new IndexSettings());
        newIndex.getSettings().setNumberOfShards(numberOfShards);
        newIndex.getSettings().setShardReplicaCount(shardReplicaCount);
        
        // Store the index configuration
        String indexConfigJson = objectMapper.writeValueAsString(newIndex);
        String documentId = metadataStore.createIndexConfig(clusterId, indexName, indexConfigJson);
        log.info("CreateIndex - Successfully created index configuration for '{}' with document ID: {}", 
            newIndex.getIndexName(), documentId);
        
        // Store mappings (merged from templates and user request)
        if (!finalMappings.isEmpty()) {
            String mappingsJson = objectMapper.writeValueAsString(finalMappings);
            metadataStore.setIndexMappings(clusterId, indexName, mappingsJson);
            log.info("CreateIndex - Set mappings for index '{}'", indexName);
        }
        
        // Store settings (merged from templates and user request)
        if (!finalSettings.isEmpty()) {
            String settingsJson = objectMapper.writeValueAsString(finalSettings);
            metadataStore.setIndexSettings(clusterId, indexName, settingsJson);
            log.info("CreateIndex - Set settings for index '{}'", indexName);
        }
        
        // TODO: Handle aliases when alias support is implemented
        if (!finalAliases.isEmpty()) {
            log.info("CreateIndex - Aliases defined for index '{}': {} (alias creation not yet implemented)", 
                indexName, finalAliases.keySet());
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
            // Delete all index data using prefix delete
            // This will remove: conf, settings, mappings, and all planned allocations
            String indexPrefix = pathResolver.getIndexPrefix(clusterId, indexName);
            metadataStore.deletePrefix(clusterId, indexPrefix);
            log.info("DeleteIndex - Successfully deleted all index data for '{}' from cluster '{}'",
                indexName, clusterId);

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
    public String getSettings(String clusterId, String indexName) throws Exception {
        log.info("Getting settings for index: {}", indexName);
        
        // Validate input parameters
        if (clusterId == null || clusterId.trim().isEmpty()) {
            throw new IllegalArgumentException("Cluster ID cannot be null or empty");
        }
        if (indexName == null || indexName.trim().isEmpty()) {
            throw new IllegalArgumentException("Index name cannot be null or empty");
        }
        
        // Get settings from metadata store
       IndexSettings settings = metadataStore.getIndexSettings(clusterId, indexName);
        if (settings == null) {
            throw new IllegalArgumentException("Index '" + indexName + "' does not exist in cluster '" + clusterId + "'");
        }
        
        return objectMapper.writeValueAsString(settings);
    }
    
    /**
     * Update index settings. This method merges the existing settings with the new settings.
     */
    public void updateSettings(String clusterId, String indexName, String settingsJson) throws Exception {
        log.info("Updating settings for index '{}' with: {}", indexName, settingsJson);

        // Validate input parameters
        if (clusterId == null || clusterId.trim().isEmpty()) {
            throw new IllegalArgumentException("Cluster ID cannot be null or empty");
        }
        if (indexName == null || indexName.trim().isEmpty()) {
            throw new IllegalArgumentException("Index name cannot be null or empty");
        }
        if (settingsJson == null || settingsJson.trim().isEmpty()) {
            throw new IllegalArgumentException("Settings JSON cannot be null or empty");
        }

        // Check if index exists
        if (!metadataStore.getIndexConfig(clusterId, indexName).isPresent()) {
            throw new IllegalArgumentException("Index '" + indexName + "' does not exist in cluster '" + clusterId + "'");
        }

        // Parse and validate the new settings JSON
        IndexSettings newSettings;
        try {
            newSettings = objectMapper.readValue(settingsJson, IndexSettings.class);
            log.debug("Successfully parsed new settings JSON for index '{}': {}", indexName, newSettings);
        } catch (Exception e) {
            throw new IllegalArgumentException("Invalid JSON format for settings: " + e.getMessage(), e);
        }

        // Validate that new settings is not empty
        if (newSettings == null) {
            throw new IllegalArgumentException("Settings cannot be empty");
        }

        // Get existing settings and merge with new settings
        IndexSettings existingSettings = null;
        try {
            existingSettings = metadataStore.getIndexSettings(clusterId, indexName);
            log.debug("Retrieved existing settings for index '{}': {}", indexName, existingSettings);
        } catch (Exception e) {
            log.warn("Failed to retrieve existing settings for index '{}', will create new settings: {}", indexName, e.getMessage());
        }

        // Merge existing settings with new settings (new settings override existing ones)
        IndexSettings mergedSettings = mergeIndexSettings(existingSettings, newSettings);
        
        log.info("Merged settings for index '{}': existing={}, new={}, merged={}", 
            indexName, existingSettings, newSettings, mergedSettings);

        // Update the settings in the metadata store with merged settings
        try {
            String mergedSettingsJson = objectMapper.writeValueAsString(mergedSettings);
            metadataStore.setIndexSettings(clusterId, indexName, mergedSettingsJson);
            log.info("Successfully updated settings for index '{}' in cluster '{}'", indexName, clusterId);
        } catch (Exception e) {
            log.error("Failed to update settings for index '{}' in cluster '{}': {}", indexName, clusterId, e.getMessage());
            throw new Exception("Failed to update settings for index '" + indexName + "': " + e.getMessage(), e);
        }
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
     * Merge two IndexSettings objects, with the second object's values taking precedence.
     * Only non-null fields from newSettings are applied to oldSettings.
     */
    private IndexSettings mergeIndexSettings(IndexSettings oldSettings, IndexSettings newSettings) {
        // If both are null, return empty settings
        if (oldSettings == null && newSettings == null) {
            return new IndexSettings();
        }
        
        // If old is null, return new
        if (oldSettings == null) {
            return newSettings;
        }
        
        // If new is null, return old
        if (newSettings == null) {
            return oldSettings;
        }
        
        // Update fields in oldSettings with non-null values from newSettings
        if (newSettings.getNumberOfShards() != null) {
            oldSettings.setNumberOfShards(newSettings.getNumberOfShards());
            log.debug("Updating number_of_shards to {}", newSettings.getNumberOfShards());
        }
        
        if (newSettings.getShardReplicaCount() != null) {
            oldSettings.setShardReplicaCount(newSettings.getShardReplicaCount());
            log.debug("Updating shard_replica_count");
        }
        
        if (newSettings.getPausePullIngestion() != null) {
            oldSettings.setPausePullIngestion(newSettings.getPausePullIngestion());
            log.debug("Updating pause_pull_ingestion to {}", newSettings.getPausePullIngestion());
        }
        
        log.debug("Merged IndexSettings: result={}", oldSettings);
        
        return oldSettings;
    }

    /**
     * Deep merge source map into target map.
     * For nested maps, recursively merge. For other values, source overrides target.
     * This allows user-provided settings to override template settings.
     */
    @SuppressWarnings("unchecked")
    private void deepMerge(Map<String, Object> target, Map<String, Object> source) {
        for (Map.Entry<String, Object> entry : source.entrySet()) {
            String key = entry.getKey();
            Object sourceValue = entry.getValue();
            
            if (sourceValue instanceof Map && target.get(key) instanceof Map) {
                // Both are maps, recursively merge
                deepMerge((Map<String, Object>) target.get(key), (Map<String, Object>) sourceValue);
            } else {
                // Override with source value
                target.put(key, sourceValue);
            }
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
        
        @JsonProperty("aliases")
        private Map<String, Object> aliases; // Optional aliases JSON
    }
}