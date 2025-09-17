package io.clustercontroller.models;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;
import lombok.NoArgsConstructor;
import java.util.ArrayList;
import java.util.List;

/**
 * ShardData represents shard allocation data
 */
@Data
@NoArgsConstructor
public class ShardData {
    @JsonProperty("shard_index_name")
    private String shardIndexName;
    
    @JsonProperty("shard_id")
    private String shardId;
    
    @JsonProperty("ingest_units")
    private List<String> ingestUnits = new ArrayList<>();
    
    @JsonProperty("search_units")
    private List<String> searchUnits = new ArrayList<>();
    
    @JsonProperty("status")
    private String status;
    
    public ShardData(String shardIndexName, String shardId) {
        this.shardIndexName = shardIndexName;
        this.shardId = shardId;
    }
    
    // Custom setters to maintain null safety
    public void setIngestUnits(List<String> ingestUnits) { 
        this.ingestUnits = ingestUnits != null ? ingestUnits : new ArrayList<>(); 
    }
    
    public void setSearchUnits(List<String> searchUnits) { 
        this.searchUnits = searchUnits != null ? searchUnits : new ArrayList<>(); 
    }
}