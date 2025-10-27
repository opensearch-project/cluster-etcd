package io.clustercontroller.models;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;
import lombok.NoArgsConstructor;
import java.util.ArrayList;
import java.util.List;

/**
 * IndexSettings represents the settings for an index.
 */
@Data
@NoArgsConstructor
public class IndexSettings {
    @JsonProperty("number_of_shards")
    private Integer numberOfShards;
    
    @JsonProperty("shard_replica_count")
    private List<Integer> shardReplicaCount;
    
    @JsonProperty("shard_groups_allocate_count")
    private List<Integer> shardGroupsAllocateCount;

    @JsonProperty("pause_pull_ingestion")
    private Boolean pausePullIngestion;
}