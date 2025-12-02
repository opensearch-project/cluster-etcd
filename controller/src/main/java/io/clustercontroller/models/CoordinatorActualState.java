package io.clustercontroller.models;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;


/**
 * Represents the actual state reported by a coordinator group
 * Contains remote_shards with actual routing information for all indices
 */
@Data
@JsonIgnoreProperties(ignoreUnknown = true)
public class CoordinatorActualState {
    
    /**
     * Remote shards information that this coordinator is currently aware of.
     */
    @JsonProperty("remote_shards")
    private CoordinatorGoalState.RemoteShards remoteShards;
    
    @JsonProperty("last_updated")
    private String lastUpdated;
    
    @JsonProperty("version")
    private long version;
    
    public CoordinatorActualState() {
        this.remoteShards = new CoordinatorGoalState.RemoteShards();
        this.version = 1;
    }
    
    public CoordinatorActualState(CoordinatorGoalState.RemoteShards remoteShards) {
        this.remoteShards = remoteShards != null ? remoteShards : new CoordinatorGoalState.RemoteShards();
        this.version = 1;
    }
}

