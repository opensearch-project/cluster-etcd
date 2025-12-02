package io.clustercontroller.models;

import io.clustercontroller.enums.HealthState;
import io.clustercontroller.enums.ShardState;
import lombok.Data;
import lombok.NoArgsConstructor;
import java.util.HashMap;
import java.util.Map;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
* Health information for a specific index
*/
@Data
@NoArgsConstructor
public class IndexHealthInfo {
    @JsonProperty("status")
    private HealthState status;

    @JsonProperty("number_of_shards")
    private int numberOfShards;

    @JsonProperty("number_of_replicas")
    private int numberOfReplicas;

    @JsonProperty("active_shards")
    private int activeShards;

    @JsonProperty("relocating_shards")
    private int relocatingShards;

    @JsonProperty("initializing_shards")
    private int initializingShards;

    @JsonProperty("unassigned_shards")
    private int unassignedShards;

    @JsonProperty("shards")
    private Map<String, ShardHealthInfo> shards = new HashMap<>(); // Only populated for shard-level detail
}    