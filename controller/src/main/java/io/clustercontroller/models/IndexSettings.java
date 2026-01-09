package io.clustercontroller.models;

import com.fasterxml.jackson.annotation.JsonAnyGetter;
import com.fasterxml.jackson.annotation.JsonAnySetter;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AccessLevel;
import lombok.Data;
import lombok.Getter;
import lombok.NoArgsConstructor;

import java.util.HashMap;
import java.util.List;
import java.util.ArrayList;
import java.util.Map;

/**
 * IndexSettings represents the settings for an index.
 * Known fields are typed, unknown fields are captured in additionalProperties.
 */
@Data
@NoArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
public class IndexSettings {
    @JsonProperty("number_of_shards")
    private Integer numberOfShards;
    
    @JsonProperty("shard_replica_count")
    private List<Integer> shardReplicaCount = new ArrayList<>();
    
    @JsonProperty("num_groups_per_shard")
    private List<Integer> numGroupsPerShard = new ArrayList<>();
    
    @JsonProperty("num_ingest_groups_per_shard")
    private List<Integer> numIngestGroupsPerShard = new ArrayList<>();

    @JsonProperty("pause_pull_ingestion")
    private Boolean pausePullIngestion = false;

    @JsonProperty("refresh_interval")
    private String refreshInterval;

    @JsonProperty("number_of_replicas")
    private Integer numberOfReplicas;

    // Capture all unknown fields (e.g., ingestion_source, knn, replication.type, etc.)
    @JsonIgnore
    @Getter(AccessLevel.NONE)
    private Map<String, Object> additionalProperties = new HashMap<>();

    @JsonAnyGetter
    public Map<String, Object> getAdditionalProperties() {
        return additionalProperties;
    }

    @JsonAnySetter
    public void setAdditionalProperty(String key, Object value) {
        additionalProperties.put(key, value);
    }
}