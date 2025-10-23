package io.clustercontroller.models;

import io.clustercontroller.enums.HealthState;
import io.clustercontroller.enums.ShardState;
import lombok.Data;
import lombok.NoArgsConstructor;
import com.fasterxml.jackson.annotation.JsonProperty;


/**
* Health information for a specific shard
*/
@Data
@NoArgsConstructor
public class ShardHealthInfo {

    @JsonProperty("shard_id")
    private int shardId;

    @JsonProperty("status")
    private HealthState status;

    @JsonProperty("primary_active")
    private boolean primaryActive;

    @JsonProperty("active_replicas")
    private int activeReplicas;

    @JsonProperty("relocating_replicas")
    private int relocatingReplicas;

    @JsonProperty("initializing_replicas")
    private int initializingReplicas;

    @JsonProperty("unassigned_replicas")
    private int unassignedReplicas;
}