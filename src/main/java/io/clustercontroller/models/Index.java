package io.clustercontroller.models;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;
import lombok.NoArgsConstructor;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Index represents index configuration stored in etcd at:
 * <cluster-name>/indices/<index-name>/conf
 */
@Data
@NoArgsConstructor
public class Index {
    @JsonProperty("id")
    private String id;
    
    @JsonProperty("index_name")
    private String indexName;
    
    @JsonProperty("number_of_shards")
    private int numberOfShards;
    
    @JsonProperty("shard_replica_count")
    private List<Integer> shardReplicaCount = new ArrayList<>();
    
    
    @JsonProperty("created_at")
    private String createdAt = java.time.OffsetDateTime.now().toString(); // ISO timestamp for proper ordering
    
    public Index(String indexName, List<Integer> shardReplicaCount) {
        this.indexName = indexName;
        this.shardReplicaCount = shardReplicaCount != null ? shardReplicaCount : new ArrayList<>();
        this.numberOfShards = this.shardReplicaCount.size();
    }
    
    // Custom setters to maintain null safety
    public void setShardReplicaCount(List<Integer> shardReplicaCount) { 
        this.shardReplicaCount = shardReplicaCount != null ? shardReplicaCount : new ArrayList<>(); 
    }
} 