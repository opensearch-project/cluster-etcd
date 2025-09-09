package io.clustercontroller.models;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;

import java.util.Map;
import java.util.HashMap;

@Data
@JsonIgnoreProperties(ignoreUnknown = true)
public class SearchUnitGoalState {
    // Data node expects "local_shards" structure: index-name -> { shard-id -> "PRIMARY" | "SEARCH_REPLICA" }
    @JsonProperty("local_shards")
    private Map<String, Map<String, String>> localShards; // index-name -> { shard-id -> role }
    
    public SearchUnitGoalState() {
        this.localShards = new HashMap<>();
    }
} 