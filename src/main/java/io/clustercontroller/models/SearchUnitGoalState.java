package io.clustercontroller.models;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.HashMap;
import java.util.Map;

/**
 * Goal state for data node search units.
 * Specifies which shards should be assigned to this node and their roles.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class SearchUnitGoalState {
    
    /**
     * Data node local shards: index-name -> shard-id -> role
     * Role is a simple string: "PRIMARY" or "SEARCH_REPLICA"
     */
    @JsonProperty("local_shards")
    private Map<String, Map<String, String>> localShards;
    
    @JsonProperty("last_updated")
    private String lastUpdated;
    
    @JsonProperty("version")
    private long version;
    
    public SearchUnitGoalState() {
        this.localShards = new HashMap<>();
        this.version = 1;
    }
    
    public Map<String, Map<String, String>> getLocalShards() {
        return localShards;
    }
    
    public void setLocalShards(Map<String, Map<String, String>> localShards) {
        this.localShards = localShards != null ? localShards : new HashMap<>();
    }
    
    public String getLastUpdated() {
        return lastUpdated;
    }
    
    public void setLastUpdated(String lastUpdated) {
        this.lastUpdated = lastUpdated;
    }
    
    public long getVersion() {
        return version;
    }
    
    public void setVersion(long version) {
        this.version = version;
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
