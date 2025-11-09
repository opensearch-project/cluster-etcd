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
    
    @JsonProperty("num_groups_per_shard")
    private List<Integer> numGroupsPerShard;
    
    @JsonProperty("num_ingest_groups_per_shard")
    private List<Integer> numIngestGroupsPerShard;

    @JsonProperty("pause_pull_ingestion")
    private Boolean pausePullIngestion;
}