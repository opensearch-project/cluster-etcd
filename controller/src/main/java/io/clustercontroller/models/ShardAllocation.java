package io.clustercontroller.models;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.ArrayList;
import java.util.List;

/**
 * ShardAllocation represents planned shard allocation stored in etcd at:
 * <cluster-name>/indices/<index-name>/<shard_id>/planned-allocation
 */
@Data
@NoArgsConstructor
public class ShardAllocation {
    @JsonProperty("shard_id")
    private String shardId;
    
    @JsonProperty("index_name")
    private String indexName;
    
    @JsonProperty("ingest_sus")
    private List<String> ingestSUs = new ArrayList<>();
    
    @JsonProperty("search_sus")
    private List<String> searchSUs = new ArrayList<>();
    
    @JsonProperty("allocation_timestamp")
    private long allocationTimestamp;
    
    public ShardAllocation(String shardId, String indexName) {
        this.shardId = shardId;
        this.indexName = indexName;
        this.allocationTimestamp = System.currentTimeMillis();
    }
    
    // Custom setters to maintain null safety
    public void setIngestSUs(List<String> ingestSUs) {
        this.ingestSUs = ingestSUs != null ? ingestSUs : new ArrayList<>();
    }
    
    public void setSearchSUs(List<String> searchSUs) {
        this.searchSUs = searchSUs != null ? searchSUs : new ArrayList<>();
    }
}