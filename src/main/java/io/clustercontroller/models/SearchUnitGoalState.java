package com.uber.esdock.osgateway.entities;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.HashMap;

@JsonIgnoreProperties(ignoreUnknown = true)
public class SearchUnitGoalState {
    // Data node expects "local_shards" structure: index-name -> { shard-id -> "PRIMARY" | "SEARCH_REPLICA" }
    @JsonProperty("local_shards")
    private Map<String, Map<String, String>> localShards; // index-name -> { shard-id -> role }
    
    @JsonProperty("last_updated")
    private String lastUpdated;
    
    @JsonProperty("version")
    private long version;
    
    public SearchUnitGoalState() {
        this.localShards = new HashMap<>();
        this.version = 1;
    }
    
    // ========== GETTERS ==========
    
    public Map<String, Map<String, String>> getLocalShards() {
        return localShards;
    }
    
    public String getLastUpdated() {
        return lastUpdated;
    }
    
    public long getVersion() {
        return version;
    }
    
    // ========== SETTERS ==========
    
    public void setLocalShards(Map<String, Map<String, String>> localShards) {
        this.localShards = localShards != null ? localShards : new HashMap<>();
    }
    
    public void setLastUpdated(String lastUpdated) {
        this.lastUpdated = lastUpdated;
    }
    
    public void setVersion(long version) {
        this.version = version;
    }
    
    // ========== UTILITY METHODS ==========
    
    /**
     * Add a shard with a specific role to an index
     * @param indexName the index name
     * @param shardId the shard id (e.g., "0", "1")
     * @param role the shard role ("PRIMARY" or "SEARCH_REPLICA")
     */
    public void addShardToIndex(String indexName, String shardId, String role) {
        localShards.computeIfAbsent(indexName, k -> new HashMap<>()).put(shardId, role);
    }
    
    /**
     * Remove a shard from an index
     */
    public void removeShardFromIndex(String indexName, String shardId) {
        Map<String, String> shards = localShards.get(indexName);
        if (shards != null) {
            shards.remove(shardId);
            if (shards.isEmpty()) {
                localShards.remove(indexName);
            }
        }
    }
    
    /**
     * Get the role of a specific shard on this node
     */
    public String getShardRole(String indexName, String shardId) {
        Map<String, String> indexShards = localShards.get(indexName);
        return indexShards != null ? indexShards.get(shardId) : null;
    }
    
    /**
     * Check if this node has any shards for the given index
     */
    public boolean hasIndex(String indexName) {
        Map<String, String> indexShards = localShards.get(indexName);
        return indexShards != null && !indexShards.isEmpty();
    }
    
    /**
     * Get all shard IDs for a given index (regardless of role)
     */
    public List<String> getShardsForIndex(String indexName) {
        Map<String, String> indexShards = localShards.get(indexName);
        return indexShards != null ? new ArrayList<>(indexShards.keySet()) : new ArrayList<>();
    }
    
    @Override
    public String toString() {
        return "SearchUnitGoalState{" +
                "localShards=" + localShards +
                ", lastUpdated='" + lastUpdated + '\'' +
                ", version=" + version +
                '}';
    }
} 