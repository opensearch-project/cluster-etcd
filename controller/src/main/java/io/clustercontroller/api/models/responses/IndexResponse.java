package io.clustercontroller.api.models.responses;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Map;

/**
 * Response model for all index operations.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@JsonInclude(JsonInclude.Include.NON_EMPTY)
@JsonNaming(PropertyNamingStrategies.SnakeCaseStrategy.class)
public class IndexResponse {
    private boolean acknowledged;
    private boolean shardsAcknowledged;
    private String index;
    private Map<String, Object> settings;
    private Map<String, Object> mappings;
    
    public static IndexResponse createSuccess(String indexName) {
        return IndexResponse.builder()
            .acknowledged(true)
            .shardsAcknowledged(true)
            .index(indexName)
            .build();
    }
    
    public static IndexResponse deleteSuccess(String indexName) {
        return IndexResponse.builder()
            .acknowledged(true)
            .index(indexName)
            .build();
    }
}