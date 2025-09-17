package io.clustercontroller.models;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.clustercontroller.models.ShardData;
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
    
    @JsonProperty("shard_replica_count")
    private List<Integer> shardReplicaCount = new ArrayList<>();
    
    @JsonProperty("settings")
    private Map<String, Object> settings;
    
    @JsonProperty("mappings")
    private Map<String, Object> mappings;
    
    @JsonProperty("allocation_plan")
    private List<ShardData> allocationPlan = new ArrayList<>();
    
    @JsonProperty("created_at")
    private String createdAt = java.time.OffsetDateTime.now().toString(); // ISO timestamp for proper ordering
    
    public Index(String indexName, List<Integer> shardReplicaCount) {
        this.indexName = indexName;
        this.shardReplicaCount = shardReplicaCount != null ? shardReplicaCount : new ArrayList<>();
        this.allocationPlan = new ArrayList<>();
    }
    
    // Custom setters to maintain null safety
    public void setShardReplicaCount(List<Integer> shardReplicaCount) { 
        this.shardReplicaCount = shardReplicaCount != null ? shardReplicaCount : new ArrayList<>(); 
    }
    
    public void setAllocationPlan(List<ShardData> allocationPlan) { 
        this.allocationPlan = allocationPlan != null ? allocationPlan : new ArrayList<>(); 
    }
} 