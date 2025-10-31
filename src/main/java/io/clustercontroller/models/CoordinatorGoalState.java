package io.clustercontroller.models;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Goal state for coordinator nodes.
 * Specifies which indices, shards, aliases, and remote clusters this coordinator should be aware of.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class CoordinatorGoalState {
    
    @JsonProperty("remote_shards")
    private RemoteShards remoteShards;
    
    @JsonProperty("last_updated")
    private String lastUpdated;
    
    @JsonProperty("version")
    private long version;
    
    public CoordinatorGoalState() {
        this.remoteShards = new RemoteShards();
        this.version = 1;
    }
    
    public RemoteShards getRemoteShards() {
        return remoteShards;
    }
    
    public void setRemoteShards(RemoteShards remoteShards) {
        this.remoteShards = remoteShards != null ? remoteShards : new RemoteShards();
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
    
    /**
     * Remote shards structure for coordinator nodes
     */
    @Data
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class RemoteShards {
        /**
         * Map of indices with their shard routing information
         * index-name -> shard routing configuration
         */
        @JsonProperty("indices")
        private Map<String, IndexShardRouting> indices;
        
        public RemoteShards() {
            this.indices = new HashMap<>();
        }
    }
    
    /**
     * Shard routing information for an index
     */
    @Data
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class IndexShardRouting {
        /**
         * Array of shard replicas, where each element is an array of node assignments
         * Each inner array represents all replicas for that shard number
         */
        @JsonProperty("shard_routing")
        private List<List<ShardNodeAssignment>> shardRouting;
    }
    
    
    
    /**
     * Node assignment for a specific shard replica
     */
    @Data
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class ShardNodeAssignment {
        @JsonProperty("node_name")
        private String nodeName;
        
        @JsonProperty("primary")
        private Boolean primary; // null/absent = search replica, true = primary
    }
    
    @Override
    public String toString() {
        return "CoordinatorGoalState{" +
                "remoteShards=" + remoteShards +
                ", lastUpdated='" + lastUpdated + '\'' +
                ", version=" + version +
                '}';
    }
}

