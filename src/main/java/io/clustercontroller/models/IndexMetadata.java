package io.clustercontroller.models;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.AllArgsConstructor;
import lombok.Builder;

import java.util.List;

/**
 * Index metadata configuration including template type, aliases, and ingestion sources
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
@JsonIgnoreProperties(ignoreUnknown = true)
public class IndexMetadata {
    
    @JsonProperty("is_index_template_type")
    @Builder.Default
    private boolean isIndexTemplateType = false;
    
    @JsonProperty("aliases")
    private List<AliasConfig> aliases;
    
    @JsonProperty("id_field")
    private String idField;
    
    @JsonProperty("version_field")
    private String versionField;
    
    @JsonProperty("batch_ingestion_source")
    private BatchIngestionSource batchIngestionSource;
    
    @JsonProperty("live_ingestion_source")
    private LiveIngestionSource liveIngestionSource;
    
    /**
     * Alias configuration with name and match strategy
     */
    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    @Builder
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class AliasConfig {
        @JsonProperty("name")
        private String name;
        
        @JsonProperty("match_strategy")
        private String matchStrategy;  // LATEST, PREVIOUS
    }
    
    /**
     * Batch ingestion source configuration for Hive-based ingestion
     */
    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    @Builder
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class BatchIngestionSource {
        @JsonProperty("hive_table")
        private String hiveTable;
        
        @JsonProperty("partition_keys")
        private List<String> partitionKeys;
        
        @JsonProperty("sql")
        private String sql;

        @JsonProperty("primary_key")
        private String primaryKey;
    }
    
    /**
     * Live ingestion source configuration for Kafka-based ingestion
     */
    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    @Builder
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class LiveIngestionSource {
        @JsonProperty("kafka_topic")
        private String kafkaTopic;
        
        @JsonProperty("cluster")
        private String cluster;
    }
}

