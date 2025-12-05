package io.clustercontroller.models;

import io.clustercontroller.enums.HealthState;
import lombok.Data;
import lombok.NoArgsConstructor;
import java.util.HashMap;
import java.util.Map;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
    * Response class for cluster health information
    */
@Data
@NoArgsConstructor
   public class ClusterHealthInfo {
       @JsonProperty("cluster_name")
       private String clusterName;

       @JsonProperty("status")
       private HealthState status;

       @JsonProperty("timed_out")
       private boolean timedOut;

       @JsonProperty("number_of_nodes")
       private int numberOfNodes;

       @JsonProperty("number_of_data_nodes")
       private int numberOfDataNodes;

       @JsonProperty("number_of_coordinator_nodes")
       private int numberOfCoordinatorNodes;

       @JsonProperty("active_nodes")
       private int activeNodes;

       @JsonProperty("number_of_indices")
       private int numberOfIndices;

       @JsonProperty("active_primary_shards")
       private int activePrimaryShards;

       @JsonProperty("active_shards")
       private int activeShards;

       @JsonProperty("relocating_shards")
       private int relocatingShards;

       @JsonProperty("initializing_shards")
       private int initializingShards;

       @JsonProperty("unassigned_shards")
       private int unassignedShards;

       @JsonProperty("delayed_unassigned_shards")
       private int delayedUnassignedShards;

       @JsonProperty("failed_shards")
       private int failedShards;

       @JsonProperty("total_shards")
       private int totalShards;

       @JsonProperty("active_shards_percent_as_number")
       private int activeShardsPercentAsNumber;
       
       @JsonProperty("number_of_pending_tasks")
       private int numberOfPendingTasks;

       @JsonProperty("number_of_in_flight_fetch")
       private int numberOfInFlightFetch;

       @JsonProperty("task_max_waiting_in_queue_millis")
       private int taskMaxWaitingInQueueMillis;

       @JsonProperty("nodes_by_health")
       private Map<HealthState, Integer> nodesByHealth = new HashMap<>();

       @JsonProperty("indices")
       private Map<String, IndexHealthInfo> indices = new HashMap<>();
       // Only populated for detailed levels
}
