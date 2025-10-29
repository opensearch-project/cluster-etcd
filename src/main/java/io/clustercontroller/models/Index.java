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
   
    @JsonProperty("created_at")
    private String createdAt = java.time.OffsetDateTime.now().toString(); // ISO timestamp for proper ordering
    
    @JsonProperty("settings")
    private IndexSettings settings;
} 
