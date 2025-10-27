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
    
    @JsonProperty("shard_groups_allocate_count")
    private List<Integer> shardGroupsAllocateCount = new ArrayList<>();
    
    @JsonProperty("num_shards")
    private Integer numShards = 1; // default to 1 shard
    
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
    
    public void setShardGroupsAllocateCount(List<Integer> shardGroupsAllocateCount) {
        this.shardGroupsAllocateCount = shardGroupsAllocateCount != null ? shardGroupsAllocateCount : new ArrayList<>();
    }
} 